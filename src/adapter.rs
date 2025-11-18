use async_trait::async_trait;
use borsa_core::{AssetKind, BorsaError, Instrument, Interval, stream::StreamHandle};
use tokio::sync::{mpsc, oneshot};

// Re-export types from the fork for clarity
use binance::options::websockets::{OptionsWebSockets, OptionsWebsocketEvent};
use binance::websockets::{WebSockets, WebsocketEvent};
use binance::{
    api::Binance, // The trait for new()
    api::{API, Options as OptionsApi},
    config::Config,
    market::Market,
    model::{KlineEvent, KlineSummaries, KlineSummary, PriceStats, TradeEvent},
    options::general::OptionsGeneral,
    options::model::{Options24hrTickerEvent, OptionsExchangeInfo},
};
use std::sync::atomic::{AtomicBool, Ordering};

/// Maps the fork's error type to `BorsaError`.
fn map_binance_error(e: binance::errors::Error) -> BorsaError {
    match e {
        binance::errors::Error::BinanceError(content) => {
            if content.code == -1121 {
                // "Invalid symbol"
                BorsaError::not_found(content.msg)
            } else {
                BorsaError::connector("binance-rs", BorsaError::Other(content.msg))
            }
        }
        binance::errors::Error::Msg(s) => BorsaError::connector("binance-rs", BorsaError::Other(s)),
        other => BorsaError::connector("binance-rs", BorsaError::Other(other.to_string())),
    }
}

/// Abstract trait for Binance APIs needed by the connector.
#[async_trait]
pub trait BinanceApi: Send + Sync {
    /// Get a spot quote (24hr stats).
    async fn get_quote(&self, symbol: &str) -> Result<PriceStats, BorsaError>;

    /// Fetch options exchangeInfo (typed).
    async fn options_exchange_info_typed(&self) -> Result<OptionsExchangeInfo, BorsaError>;

    /// Stream option tickers.
    async fn stream_options(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<Options24hrTickerEvent>), BorsaError>;

    /// Stream spot trades for the given instruments (one receiver with combined events).
    async fn stream_spot_trades(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<TradeEvent>), BorsaError>;

    /// Stream spot kline events for the given instruments (combined receiver).
    async fn stream_spot_kline(
        &self,
        instruments: &[Instrument],
        interval: Interval,
    ) -> Result<(StreamHandle, mpsc::Receiver<KlineEvent>), BorsaError>;

    /// Fetch 1-second klines via the public Binance data API.
    async fn get_klines_1s(
        &self,
        symbol: &str,
        limit: Option<u16>,
        start_time_ms: Option<u64>,
        end_time_ms: Option<u64>,
    ) -> Result<Vec<KlineSummary>, BorsaError>;
}

/// The "real" adapter that holds clients for the `binance-rs`.
#[derive(Clone)]
pub struct RealAdapter {
    market: Market,
    config: Config,
}

impl RealAdapter {
    pub fn new(api_key: String, secret_key: String) -> Self {
        let config = Config::default();
        let market = Market::new_with_config(Some(api_key), Some(secret_key), &config);

        Self { market, config }
    }

    pub fn new_with_endpoints(
        api_key: String,
        secret_key: String,
        futures_rest_api_endpoint: Option<String>,
        futures_ws_endpoint: Option<String>,
        options_rest_api_endpoint: Option<String>,
        options_ws_endpoint: Option<String>,
        rest_api_endpoint: Option<String>,
        ws_endpoint: Option<String>,
    ) -> Self {
        let mut config = Config::default();

        if let Some(futures_rest_api_endpoint) = futures_rest_api_endpoint {
            config = config.set_futures_rest_api_endpoint(futures_rest_api_endpoint);
        }
        if let Some(futures_ws_endpoint) = futures_ws_endpoint {
            config = config.set_futures_ws_endpoint(futures_ws_endpoint);
        }
        if let Some(options_rest_api_endpoint) = options_rest_api_endpoint {
            config = config.set_options_rest_api_endpoint(options_rest_api_endpoint);
        }
        if let Some(options_ws_endpoint) = options_ws_endpoint {
            config = config.set_options_ws_endpoint(options_ws_endpoint);
        }
        if let Some(rest_api_endpoint) = rest_api_endpoint {
            config = config.set_rest_api_endpoint(rest_api_endpoint);
        }
        if let Some(ws_endpoint) = ws_endpoint {
            config = config.set_ws_endpoint(ws_endpoint);
        }

        let market = Market::new_with_config(Some(api_key), Some(secret_key), &config);

        Self { market, config }
    }
}

#[async_trait]
impl BinanceApi for RealAdapter {
    async fn get_quote(&self, symbol: &str) -> Result<PriceStats, BorsaError> {
        self.market
            .get_24h_price_stats(symbol.to_string())
            .await
            .map_err(map_binance_error)
    }

    async fn options_exchange_info_typed(&self) -> Result<OptionsExchangeInfo, BorsaError> {
        let rest: OptionsGeneral = Binance::new_with_config(None, None, &self.config);
        rest.client
            .get::<OptionsExchangeInfo>(API::Options(OptionsApi::ExchangeInfo), None)
            .await
            .map_err(map_binance_error)
    }

    async fn stream_options(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<Options24hrTickerEvent>), BorsaError> {
        // Build topics
        let mut topics: Vec<String> = Vec::new();
        for inst in instruments {
            if inst.kind() != &AssetKind::Option {
                return Err(BorsaError::InvalidArg(
                    "stream_options only supports Option assets".to_string(),
                ));
            }
            let symbol = match inst.id() {
                borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str().to_string(),
                _ => {
                    return Err(BorsaError::InvalidArg(
                        "instrument is not a security".into(),
                    ));
                }
            };
            topics.push(format!("{}@ticker", symbol));
        }
        if topics.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            let (stop_tx, _stop_rx) = oneshot::channel::<()>();
            let noop = tokio::spawn(async {});
            return Ok((StreamHandle::new(noop, stop_tx), rx));
        }

        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let config = self.config.clone();
        let topics_clone = topics.clone();

        let join = tokio::spawn(async move {
            let running = AtomicBool::new(true);
            let mut ws = OptionsWebSockets::new_with_config(
                {
                    let tx_clone = tx.clone();
                    move |evt: OptionsWebsocketEvent| {
                        if let OptionsWebsocketEvent::Ticker24hr(t) = evt {
                            let _ = tx_clone.try_send(t);
                        }
                        Ok(())
                    }
                },
                &config,
            );

            if let Err(e) = ws.connect_base().await {
                eprintln!("[binance-options] connect error: {}", e);
                return;
            }
            if let Err(e) = ws.subscribe_tickers(&topics_clone).await {
                eprintln!("[binance-options] subscribe error: {}", e);
                let _ = ws.disconnect().await;
                return;
            }

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        running.store(false, Ordering::Relaxed);
                        let _ = ws.unsubscribe_tickers(&topics_clone).await;
                        let _ = ws.disconnect().await;
                        break;
                    }
                    res = ws.event_loop(&running) => {
                        if let Err(e) = res {
                            eprintln!("[binance-options] event loop error: {}", e);
                        }
                        // Drop connection; outer loop will end unless stop signal sent; reconnect handled internally
                        if !running.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });

        Ok((StreamHandle::new(join, stop_tx), rx))
    }

    async fn stream_spot_trades(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<TradeEvent>), BorsaError> {
        // Validate and build combined endpoints
        let mut endpoints: Vec<String> = Vec::new();
        for inst in instruments {
            if inst.kind() != &AssetKind::Crypto {
                return Err(BorsaError::InvalidArg(
                    "stream_spot_trades only supports Crypto assets".to_string(),
                ));
            }
            let symbol = match inst.id() {
                borsa_core::IdentifierScheme::Security(sec) => {
                    sec.symbol.as_str().to_ascii_lowercase()
                }
                _ => {
                    return Err(BorsaError::InvalidArg(
                        "instrument is not a security".into(),
                    ));
                }
            };
            endpoints.push(format!("{}@trade", symbol));
        }
        if endpoints.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            let (stop_tx, _stop_rx) = oneshot::channel::<()>();
            let noop = tokio::spawn(async {});
            return Ok((StreamHandle::new(noop, stop_tx), rx));
        }

        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let config = self.config.clone();
        let endpoints_clone = endpoints.clone();
        let (init_tx, init_rx) = oneshot::channel::<Result<(), BorsaError>>();

        let join = tokio::spawn(async move {
            let running = AtomicBool::new(true);
            let mut ws = WebSockets::new({
                let tx_clone = tx.clone();
                move |event: WebsocketEvent| {
                    if let WebsocketEvent::Trade(t) = event {
                        let _ = tx_clone.try_send(t);
                    }
                    Ok(())
                }
            });

            match ws
                .connect_multiple_streams_with_config(&endpoints_clone, &config)
                .await
            {
                Ok(_) => {
                    let _ = init_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ =
                        init_tx.send(Err(BorsaError::Other(format!("ws connect error: {}", e))));
                    return;
                }
            }

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        running.store(false, Ordering::Relaxed);
                        let _ = ws.disconnect().await;
                        break;
                    }
                    res = ws.event_loop(&running) => {
                        if let Err(e) = res {
                            eprintln!("[binance-spot] event loop error: {}", e);
                        }
                        if !running.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });

        match init_rx.await {
            Ok(Ok(())) => Ok((StreamHandle::new(join, stop_tx), rx)),
            Ok(Err(e)) => {
                let _ = join.abort();
                Err(e)
            }
            Err(_) => {
                let _ = join.abort();
                Err(BorsaError::Other("Connect init task panicked".to_string()))
            }
        }
    }

    async fn stream_spot_kline(
        &self,
        instruments: &[Instrument],
        interval: Interval,
    ) -> Result<(StreamHandle, mpsc::Receiver<KlineEvent>), BorsaError> {
        let interval_suffix = match interval {
            Interval::I1s => "1s",
            Interval::I1h => "1h",
            _ => {
                return Err(BorsaError::InvalidArg(format!(
                    "stream_spot_kline unsupported interval: {:?}",
                    interval
                )));
            }
        };

        // Validate and build combined endpoints
        let mut endpoints: Vec<String> = Vec::new();
        for inst in instruments {
            if inst.kind() != &AssetKind::Crypto {
                return Err(BorsaError::InvalidArg(format!(
                    "stream_spot_kline({}) only supports Crypto assets",
                    interval_suffix
                )));
            }
            let symbol = match inst.id() {
                borsa_core::IdentifierScheme::Security(sec) => {
                    sec.symbol.as_str().to_ascii_lowercase()
                }
                _ => {
                    return Err(BorsaError::InvalidArg(
                        "instrument is not a security".into(),
                    ));
                }
            };
            endpoints.push(format!("{}@kline_{}", symbol, interval_suffix));
        }
        if endpoints.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            let (stop_tx, _stop_rx) = oneshot::channel::<()>();
            let noop = tokio::spawn(async {});
            return Ok((StreamHandle::new(noop, stop_tx), rx));
        }

        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let config = self.config.clone();
        let endpoints_clone = endpoints.clone();
        let (init_tx, init_rx) = oneshot::channel::<Result<(), BorsaError>>();

        let join = tokio::spawn(async move {
            let running = AtomicBool::new(true);
            let mut ws = WebSockets::new({
                let tx_clone = tx.clone();
                move |event: WebsocketEvent| {
                    if let WebsocketEvent::Kline(k) = event {
                        let _ = tx_clone.try_send(k);
                    }
                    Ok(())
                }
            });

            match ws
                .connect_multiple_streams_with_config(&endpoints_clone, &config)
                .await
            {
                Ok(_) => {
                    let _ = init_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ =
                        init_tx.send(Err(BorsaError::Other(format!("ws connect error: {}", e))));
                    return;
                }
            }

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        running.store(false, Ordering::Relaxed);
                        let _ = ws.disconnect().await;
                        break;
                    }
                    res = ws.event_loop(&running) => {
                        if let Err(e) = res {
                            eprintln!("[binance-spot] event loop error: {}", e);
                        }
                        if !running.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });

        match init_rx.await {
            Ok(Ok(())) => Ok((StreamHandle::new(join, stop_tx), rx)),
            Ok(Err(e)) => {
                let _ = join.abort();
                Err(e)
            }
            Err(_) => {
                let _ = join.abort();
                Err(BorsaError::Other("Connect init task panicked".to_string()))
            }
        }
    }

    async fn get_klines_1s(
        &self,
        symbol: &str,
        limit: Option<u16>,
        start_time_ms: Option<u64>,
        end_time_ms: Option<u64>,
    ) -> Result<Vec<KlineSummary>, BorsaError> {
        let summaries = self
            .market
            .get_klines_1s_data_api(symbol.to_string(), limit, start_time_ms, end_time_ms, Some(self.config.spot_data_api_endpoint.clone()))
            .await
            .map_err(map_binance_error)?;
        match summaries {
            KlineSummaries::AllKlineSummaries(v) => Ok(v),
        }
    }
}
