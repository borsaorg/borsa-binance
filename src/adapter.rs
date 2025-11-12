use async_trait::async_trait;
use borsa_core::{AssetKind, BorsaError, Instrument, Interval, stream::StreamHandle};
use tokio::sync::{mpsc, oneshot, watch};

// Re-export types from the fork for clarity
use binance::{
    api::Binance, // The trait for new()
    api::{API, Options as OptionsApi},
    config::Config,
    market::Market,
    model::{KlineEvent, PriceStats, TradeEvent},
    options::general::OptionsGeneral,
    options::model::{Options24hrTickerEvent, OptionsExchangeInfo},
};
// keep serde_json for WS parsing
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

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

    // raw JSON exchangeInfo removed; use typed API above
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
        let rest: OptionsGeneral = Binance::new(None, None);
        rest.client
            .get::<OptionsExchangeInfo>(API::Options(OptionsApi::ExchangeInfo), None)
            .await
            .map_err(map_binance_error)
    }

    async fn stream_options(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<Options24hrTickerEvent>), BorsaError> {
        // Build topic list
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
            // Nothing to subscribe to
            let (_tx, rx) = mpsc::channel(1);
            let (stop_tx, _stop_rx) = oneshot::channel::<()>();
            let noop = tokio::spawn(async {});
            return Ok((StreamHandle::new(noop, stop_tx), rx));
        }

        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let base = self.config.options_ws_endpoint.clone(); // e.g., wss://nbstream.binance.com/eoptions/ws
        let url = base; // subscribe via RPC after connect

        let topics_clone = topics.clone();
        let join = tokio::spawn(async move {
            let (mut stream, _resp) = match connect_async(&url).await {
                Ok(ans) => ans,
                Err(e) => {
                    eprintln!("[binance-options] connect error {}: {}", url, e);
                    return;
                }
            };

            // Send SUBSCRIBE
            let sub_req = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": topics_clone,
                "id": 1
            });
            if stream
                .send(Message::Text(sub_req.to_string().into()))
                .await
                .is_err()
            {
                eprintln!("[binance-options] failed to send SUBSCRIBE");
                let _ = stream.close(None).await;
                return;
            }

            // Ping task
            let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(15));

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        // Unsubscribe then close
                        let unsub_req = serde_json::json!({
                            "method": "UNSUBSCRIBE",
                            "params": topics,
                            "id": 2
                        });
                        let _ = stream.send(Message::Text(unsub_req.to_string().into())).await;
                        let _ = stream.close(None).await;
                        break;
                    }
                    _ = ping_interval.tick() => {
                        let _ = stream.send(Message::Ping(tokio_tungstenite::tungstenite::Bytes::new())).await;
                    }
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(Message::Text(txt))) => {
                                let mut v: serde_json::Value = match serde_json::from_str(&txt) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                if let Some(data) = v.get("data").cloned() {
                                    v = data;
                                }
                                // Skip RPC acks like {"result":null,"id":1}
                                if v.get("result").is_some() {
                                    continue;
                                }
                                if let Ok(t) = serde_json::from_value::<binance::options::model::Options24hrTickerEvent>(v) {
                                    if tx.send(t).await.is_err() {
                                        eprintln!("[binance-options] tx closed");
                                        break;
                                    }
                                } else {
                                    // Optional: uncomment for debugging schema issues
                                    // eprintln!("[binance-options] parse failed; raw={}", txt);
                                }
                            }
                            Some(Ok(Message::Ping(p))) => { let _ = stream.send(Message::Pong(p)).await; }
                            Some(Ok(Message::Close(_))) => { break; }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => { eprintln!("[binance-options] stream err: {}", e); break; }
                            None => { eprintln!("[binance-options] stream ended {}", url); break; }
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
        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (stop_broadcast_tx, stop_broadcast_rx) = watch::channel(false);

        let mut task_handles = Vec::new();
        let mut connect_futures = Vec::new();

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
            let endpoint = format!("{}/{}@trade", self.config.ws_endpoint.clone(), symbol);
            let mut stop_rx_clone = stop_broadcast_rx.clone();
            let tx_clone = tx.clone();
            let (init_tx, init_rx) = oneshot::channel::<Result<(), BorsaError>>();
            connect_futures.push(init_rx);

            task_handles.push(tokio::spawn(async move {
                let (mut stream, _resp) = match connect_async(&endpoint).await {
                    Ok(ans) => { let _ = init_tx.send(Ok(())); ans }
                    Err(e) => { eprintln!("[binance-spot] connect error {}: {}", endpoint, e); let _ = init_tx.send(Err(BorsaError::Other(format!("ws connect error: {}", e)))); return; }
                };
                loop {
                    tokio::select! {
                        _ = stop_rx_clone.changed() => { let _ = stream.close(None).await; break; }
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    let mut v: serde_json::Value = match serde_json::from_str(&txt) { Ok(v) => v, Err(_) => continue };
                                    if let Some(data) = v.get("data").cloned() { v = data; }
                                    if let Ok(t) = serde_json::from_value::<binance::model::TradeEvent>(v) {
                                        if tx_clone.send(t).await.is_err() { eprintln!("[binance-spot] tx closed"); break; }
                                    } else {
                                        // Aid debugging by showing the payload that failed to parse
                                        eprintln!("[binance-spot] parse failed; raw={}", txt);
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => { let _ = stream.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Close(_))) => { break; }
                                Some(Ok(_)) => {}
                                Some(Err(e)) => { eprintln!("[binance-spot] stream err: {}", e); break; }
                                None => { eprintln!("[binance-spot] stream ended {}", endpoint); break; }
                            }
                        }
                    }
                }
            }));
        }

        // Wait for all connection attempts
        let results = futures_util::future::join_all(connect_futures).await;
        let mut errors = Vec::new();
        for res in results {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(e),
                Err(_) => errors.push(BorsaError::Other("Connect init task panicked".to_string())),
            }
        }
        if !errors.is_empty() {
            let _ = stop_broadcast_tx.send(true);
            for h in task_handles {
                let _ = h.await;
            }
            return Err(BorsaError::AllProvidersFailed(errors));
        }

        // Supervisor to fan-out the stop to all threads
        let supervisor = tokio::spawn(async move {
            let _ = stop_rx.await;
            let _ = stop_broadcast_tx.send(true);
            for h in task_handles {
                let _ = h.await;
            }
        });

        Ok((StreamHandle::new(supervisor, stop_tx), rx))
    }

    async fn stream_spot_kline(
        &self,
        instruments: &[Instrument],
        interval: Interval,
    ) -> Result<(StreamHandle, mpsc::Receiver<KlineEvent>), BorsaError> {
        let (tx, rx) = mpsc::channel(1024);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (stop_broadcast_tx, stop_broadcast_rx) = watch::channel(false);

        let mut task_handles = Vec::new();
        let mut connect_futures = Vec::new();
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

        for inst in instruments {
            if inst.kind() != &AssetKind::Crypto {
                return Err(BorsaError::InvalidArg(
                    format!(
                        "stream_spot_kline({}) only supports Crypto assets",
                        interval_suffix
                    ),
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
            let endpoint = format!(
                "{}/{}@kline_{}",
                self.config.ws_endpoint.clone(),
                symbol,
                interval_suffix
            );
            let mut stop_rx_clone = stop_broadcast_rx.clone();
            let tx_clone = tx.clone();
            let (init_tx, init_rx) = oneshot::channel::<Result<(), BorsaError>>();
            connect_futures.push(init_rx);

            task_handles.push(tokio::spawn(async move {
                let (mut stream, _resp) = match connect_async(&endpoint).await {
                    Ok(ans) => { let _ = init_tx.send(Ok(())); ans }
                    Err(e) => { eprintln!("[binance-spot] connect error {}: {}", endpoint, e); let _ = init_tx.send(Err(BorsaError::Other(format!("ws connect error: {}", e)))); return; }
                };
                loop {
                    tokio::select! {
                        _ = stop_rx_clone.changed() => { let _ = stream.close(None).await; break; }
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    let mut v: serde_json::Value = match serde_json::from_str(&txt) { Ok(v) => v, Err(_) => continue };
                                    if let Some(data) = v.get("data").cloned() { v = data; }
                                    if let Ok(t) = serde_json::from_value::<binance::model::KlineEvent>(v) {
                                        if tx_clone.send(t).await.is_err() { eprintln!("[binance-spot] tx closed"); break; }
                                    } else {
                                        eprintln!("[binance-spot] parse failed; raw={}", txt);
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => { let _ = stream.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Close(_))) => { break; }
                                Some(Ok(_)) => {}
                                Some(Err(e)) => { eprintln!("[binance-spot] stream err: {}", e); break; }
                                None => { eprintln!("[binance-spot] stream ended {}", endpoint); break; }
                            }
                        }
                    }
                }
            }));
        }

        let results = futures_util::future::join_all(connect_futures).await;
        let mut errors = Vec::new();
        for res in results {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(e),
                Err(_) => errors.push(BorsaError::Other("Connect init task panicked".to_string())),
            }
        }
        if !errors.is_empty() {
            let _ = stop_broadcast_tx.send(true);
            for h in task_handles {
                let _ = h.await;
            }
            return Err(BorsaError::AllProvidersFailed(errors));
        }

        let supervisor = tokio::spawn(async move {
            let _ = stop_rx.await;
            let _ = stop_broadcast_tx.send(true);
            for h in task_handles {
                let _ = h.await;
            }
        });

        Ok((StreamHandle::new(supervisor, stop_tx), rx))
    }

    // raw JSON exchangeInfo removed
}
