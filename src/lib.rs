use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use async_trait::async_trait;
use borsa_core::connector::{
    BorsaConnector, CandleStreamProvider, ConnectorKey, OptionChainProvider, OptionStreamProvider,
    OptionsExpirationsProvider, QuoteProvider,
};
use borsa_core::{
    AssetKind, BorsaError, Candle, Currency, Instrument, Interval, IsoCurrency, Money, OptionChain,
    OptionContract, OptionUpdate, Quote, QuoteUpdate, stream::StreamHandle,
};
use chrono::TimeZone;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

pub mod adapter;
pub mod builder;
pub mod convert;

use crate::adapter::{BinanceApi, RealAdapter};

/// Borsa connector for the Binance (fork) API.
pub struct BinanceConnector {
    adapter: Arc<dyn BinanceApi>,
}

// Legacy kline helper removed; use CandleStreamProvider instead.

impl BinanceConnector {
    /// Static connector key used in orchestrator priority configuration.
    pub const KEY: ConnectorKey = ConnectorKey::new("borsa-binance");

    /// Creates a new connector with the default `RealAdapter` using API keys.
    pub fn new_with_keys(api_key: String, secret_key: String) -> Self {
        Self {
            adapter: Arc::new(RealAdapter::new(api_key, secret_key)),
        }
    }

    /// Creates a new connector from a specific adapter implementation (useful for testing).
    pub fn from_adapter(adapter: Arc<dyn BinanceApi>) -> Self {
        Self { adapter }
    }

    // Vendor helpers removed; use provider traits instead.

    // Legacy kline helper removed; use CandleStreamProvider::stream_candles instead.
}

#[async_trait]
impl BorsaConnector for BinanceConnector {
    fn name(&self) -> &'static str {
        "borsa-binance"
    }

    fn vendor(&self) -> &'static str {
        "Binance"
    }

    fn key(&self) -> ConnectorKey {
        Self::KEY
    }

    fn supports_kind(&self, kind: AssetKind) -> bool {
        // Supports Spot (Crypto) and Options
        matches!(kind, AssetKind::Crypto | AssetKind::Option)
    }

    fn as_quote_provider(&self) -> Option<&dyn QuoteProvider> {
        // Provides Spot API
        Some(self as &dyn QuoteProvider)
    }

    fn as_stream_provider(&self) -> Option<&dyn borsa_core::connector::StreamProvider> {
        // Provides Spot trade stream API
        Some(self as &dyn borsa_core::connector::StreamProvider)
    }

    fn as_option_stream_provider(&self) -> Option<&dyn OptionStreamProvider> {
        // Provides Option WebSocket
        Some(self as &dyn OptionStreamProvider)
    }

    fn as_options_expirations_provider(&self) -> Option<&dyn OptionsExpirationsProvider> {
        Some(self as &dyn OptionsExpirationsProvider)
    }

    fn as_option_chain_provider(&self) -> Option<&dyn OptionChainProvider> {
        Some(self as &dyn OptionChainProvider)
    }

    fn as_candle_stream_provider(&self) -> Option<&dyn CandleStreamProvider> {
        Some(self as &dyn CandleStreamProvider)
    }
}

#[async_trait]
impl QuoteProvider for BinanceConnector {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "borsa_binance::quote",
            skip(self, instrument),
            fields(id = ?instrument.id()),
        )
    )]
    async fn quote(&self, instrument: &Instrument) -> Result<Quote, BorsaError> {
        if instrument.kind() != &AssetKind::Crypto {
            return Err(BorsaError::unsupported(format!(
                "quote for {} (non-crypto assets)",
                self.name()
            )));
        }

        let symbol_str = match instrument.id() {
            borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
            _ => {
                return Err(BorsaError::InvalidArg(
                    "instrument is not a security".into(),
                ));
            }
        };
        let stats = self.adapter.get_quote(symbol_str).await?;

        convert::binance_price_stats_to_quote(stats, instrument.clone())
    }
}

#[async_trait]
impl OptionStreamProvider for BinanceConnector {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "borsa_binance::stream_options",
            skip(self, instruments),
            fields(num_symbols = instruments.len()),
        )
    )]
    async fn stream_options(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<OptionUpdate>), BorsaError> {
        // 1. Get the underlying stream from the adapter
        // This stream emits raw `binance` types
        let (binance_handle, mut binance_rx) = self.adapter.stream_options(instruments).await?;

        // 2. Create the output channel that will emit Borsa types
        let (borsa_tx, borsa_rx) = mpsc::channel(1024);

        // 3. Create the stop signal for the forwarder task
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

        // 4. Spawn a forwarder task
        // This task reads from binance_rx, converts the type, and sends to borsa_tx
        let forwarder_join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Biased select to prioritize stop signal
                    biased;
                    _ = &mut stop_rx => {
                        // We were told to stop by the StreamHandle
                        break;
                    }

                    // Listen for new messages from the binance stream
                    Some(binance_ticker) = binance_rx.recv() => {
                        match convert::binance_option_ticker_to_update(binance_ticker) {
                            Ok(update) => {
                                // Forward the converted update
                                if borsa_tx.send(update).await.is_err() {
                                    // Borsa side hung up, stop the stream
                                    break;
                                }
                            }
                            Err(_e) => {
                                // Log or ignore conversion error
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    target = "borsa::connector::binance",
                                    "Failed to convert binance option ticker: {}", _e
                                );
                            }
                        }
                    }

                    // The binance stream ended
                    else => {
                        break;
                    }
                }
            }
            // When the loop exits, ensure the underlying binance stream is stopped
            binance_handle.stop().await;
        });

        // 5. Return the handle (which controls the forwarder) and the borsa receiver
        Ok((StreamHandle::new(forwarder_join, stop_tx), borsa_rx))
    }
}

#[async_trait]
impl OptionsExpirationsProvider for BinanceConnector {
    async fn options_expirations(&self, instrument: &Instrument) -> Result<Vec<i64>, BorsaError> {
        // Derive underlying symbol string
        let underlying = match instrument.id() {
            borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
            _ => {
                return Err(BorsaError::InvalidArg(
                    "instrument is not a security".into(),
                ));
            }
        };
        let info = self.adapter.options_exchange_info_typed().await?;
        let mut expiries: Vec<i64> = info
            .option_symbols
            .iter()
            .filter(|s| s.underlying.eq_ignore_ascii_case(underlying))
            .map(|s| (s.expiry_date as i64) / 1000) // ms -> s
            .collect();
        expiries.sort_unstable();
        expiries.dedup();
        Ok(expiries)
    }
}

#[async_trait]
impl OptionChainProvider for BinanceConnector {
    async fn option_chain(
        &self,
        instrument: &Instrument,
        date: Option<i64>,
    ) -> Result<OptionChain, BorsaError> {
        let underlying = match instrument.id() {
            borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
            _ => {
                return Err(BorsaError::InvalidArg(
                    "instrument is not a security".into(),
                ));
            }
        };
        let info = self.adapter.options_exchange_info_typed().await?;
        // Determine target expiry (seconds)
        let all_for_underlying: Vec<&binance::options::model::OptionSymbol> = info
            .option_symbols
            .iter()
            .filter(|s| s.underlying.eq_ignore_ascii_case(underlying))
            .collect();
        if all_for_underlying.is_empty() {
            return Ok(OptionChain {
                calls: vec![],
                puts: vec![],
            });
        }
        // Choose expiry: provided date (by day) or nearest future
        let chosen_expiry_ms: i64 = if let Some(ts_secs) = date {
            // Normalize by date (UTC day)
            let provided_date = chrono::DateTime::<chrono::Utc>::from_timestamp(ts_secs, 0)
                .map(|dt| dt.date_naive())
                .ok_or_else(|| BorsaError::InvalidArg("invalid expiry timestamp".into()))?;
            // Find any symbol matching that date
            let mut candidates: Vec<i64> = all_for_underlying
                .iter()
                .map(|s| s.expiry_date as i64)
                .filter(|ms| {
                    let d = chrono::DateTime::<chrono::Utc>::from_timestamp(ms / 1000, 0)
                        .map(|dt| dt.date_naive());
                    d == Some(provided_date)
                })
                .collect();
            candidates.sort_unstable();
            candidates.first().copied().unwrap_or_else(|| {
                all_for_underlying
                    .iter()
                    .map(|s| s.expiry_date as i64)
                    .min()
                    .unwrap_or(0)
            })
        } else {
            // Nearest future expiry
            let now_ms = chrono::Utc::now().timestamp_millis();
            let mut fut: Vec<i64> = all_for_underlying
                .iter()
                .map(|s| s.expiry_date as i64)
                .filter(|&ms| ms >= now_ms)
                .collect();
            if fut.is_empty() {
                fut = all_for_underlying
                    .iter()
                    .map(|s| s.expiry_date as i64)
                    .collect();
            }
            fut.sort_unstable();
            fut[0]
        };
        // Partition by side for the chosen expiry
        let mut calls: Vec<OptionContract> = Vec::new();
        let mut puts: Vec<OptionContract> = Vec::new();
        for s in all_for_underlying
            .into_iter()
            .filter(|s| s.expiry_date as i64 == chosen_expiry_ms)
        {
            // Build contract
            let inst = Instrument::from_symbol(&s.symbol, AssetKind::Option).map_err(|e| {
                BorsaError::Data(format!("invalid option symbol {}: {}", s.symbol, e))
            })?;
            // Strike in USD
            let strike_money = {
                let d = Decimal::from_f64_retain(s.strike_price)
                    .ok_or_else(|| BorsaError::Data("invalid strike".into()))?;
                Money::new(d, Currency::Iso(IsoCurrency::USD)).map_err(BorsaError::from)?
            };
            let exp_dt = chrono::Utc.timestamp_millis_opt(chosen_expiry_ms).single();
            let exp_date = exp_dt
                .map(|dt| dt.date_naive())
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let oc = OptionContract {
                instrument: inst,
                strike: strike_money,
                price: None,
                bid: None,
                ask: None,
                volume: None,
                open_interest: None,
                implied_volatility: None,
                in_the_money: false,
                expiration_at: exp_dt,
                expiration_date: exp_date,
                greeks: None,
                last_trade_at: None,
            };
            if s.side.to_ascii_uppercase().starts_with('C') {
                calls.push(oc);
            } else {
                puts.push(oc);
            }
        }
        // Sort by strike
        calls.sort_by(|a, b| a.strike.amount().cmp(&b.strike.amount()));
        puts.sort_by(|a, b| a.strike.amount().cmp(&b.strike.amount()));
        Ok(OptionChain { calls, puts })
    }
}

#[async_trait]
impl borsa_core::connector::StreamProvider for BinanceConnector {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "borsa_binance::stream_spot",
            skip(self, instruments),
            fields(num_symbols = instruments.len()),
        )
    )]
    async fn stream_quotes(
        &self,
        instruments: &[Instrument],
    ) -> Result<(StreamHandle, mpsc::Receiver<QuoteUpdate>), BorsaError> {
        // 1. Start the adapter-level spot trade stream
        let (spot_handle, mut trades_rx) = self.adapter.stream_spot_trades(instruments).await?;

        // 2. Build symbol -> instrument map for quick lookup
        let mut map: HashMap<String, Instrument> = HashMap::new();
        for inst in instruments {
            let sym = match inst.id() {
                borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
                _ => continue,
            };
            map.insert(sym.to_ascii_uppercase(), inst.clone());
        }

        // 3. Output channel
        let (out_tx, out_rx) = mpsc::channel::<QuoteUpdate>(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

        // 4. Forwarder: TradeEvent -> QuoteUpdate
        let forwarder = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop_rx => { break; }
                    Some(trade) = trades_rx.recv() => {
                        let sym = trade.symbol.to_ascii_uppercase();
                        if let Some(inst) = map.get(&sym).cloned() {
                            // Parse price as Decimal -> Money in USD (baseline)
                            let price_money = Decimal::from_str(&trade.price)
                                .ok()
                                .and_then(|d| Money::new(d, Currency::Iso(IsoCurrency::USD)).ok());
                            let ts = chrono::Utc
                                .timestamp_millis_opt(trade.trade_order_time as i64)
                                .single()
                                .unwrap_or_else(chrono::Utc::now);
                            let update = QuoteUpdate {
                                instrument: inst,
                                price: price_money,
                                previous_close: None,
                                ts,
                                volume: None,
                            };
                            if out_tx.send(update).await.is_err() {
                                break;
                            }
                        }
                    }
                    else => { break; }
                }
            }
            // Stop underlying stream
            spot_handle.stop().await;
        });

        Ok((StreamHandle::new(forwarder, stop_tx), out_rx))
    }
}

#[async_trait]
impl CandleStreamProvider for BinanceConnector {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "borsa_binance::stream_candles",
            skip(self, instruments),
            fields(num_symbols = instruments.len()),
        )
    )]
    async fn stream_candles(
        &self,
        instruments: &[Instrument],
        interval: Interval,
    ) -> Result<(StreamHandle, mpsc::Receiver<borsa_core::CandleUpdate>), BorsaError> {
        // Support only H1 for now (maps to @kline_1h)
        if interval != Interval::I1h {
            return Err(BorsaError::unsupported(format!(
                "stream_candles: interval {:?}",
                interval
            )));
        }

        // Start adapter-level kline stream (H1)
        let (kline_handle, mut kline_rx) = self.adapter.stream_spot_kline_1h(instruments).await?;

        // Build symbol -> instrument map
        let mut map: HashMap<String, Instrument> = HashMap::new();
        for inst in instruments {
            let sym = match inst.id() {
                borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
                _ => continue,
            };
            map.insert(sym.to_ascii_uppercase(), inst.clone());
        }

        // Output channel
        let (out_tx, out_rx) = mpsc::channel::<borsa_core::CandleUpdate>(1024);
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

        let forwarder = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop_rx => { break; }
                    Some(ke) = kline_rx.recv() => {
                        let sym = ke.symbol.to_ascii_uppercase();
                        if let Some(inst) = map.get(&sym).cloned() {
                            let k = ke.kline;
                            // Currency baseline: USDT/USDC → USD
                            let cur = if sym.ends_with("USDT") || sym.ends_with("USDC") {
                                Currency::Iso(IsoCurrency::USD)
                            } else {
                                Currency::Iso(IsoCurrency::USD)
                            };
                            // Parse price strings into Money
                            let open = Decimal::from_str(&k.open).ok().and_then(|d| Money::new(d, cur.clone()).ok());
                            let high = Decimal::from_str(&k.high).ok().and_then(|d| Money::new(d, cur.clone()).ok());
                            let low  = Decimal::from_str(&k.low).ok().and_then(|d| Money::new(d, cur.clone()).ok());
                            let close = Decimal::from_str(&k.close).ok().and_then(|d| Money::new(d, cur.clone()).ok());
                            if let (Some(o), Some(h), Some(l), Some(c)) = (open, high, low, close) {
                                // Volume: best-effort parse to u64
                                let volume = k.volume.parse::<f64>().ok().map(|v| if v < 0.0 { 0 } else { v as u64 });
                                // Candle ts uses bar start (seconds)
                                let ts = chrono::Utc
                                    .timestamp_millis_opt(k.open_time)
                                    .single()
                                    .map(|dt| chrono::DateTime::<chrono::Utc>::from(dt))
                                    .unwrap_or_else(chrono::Utc::now);
                                let candle = Candle {
                                    ts,
                                    open: o,
                                    high: h,
                                    low: l,
                                    close: c,
                                    close_unadj: None,
                                    volume,
                                };
                                let update = borsa_core::CandleUpdate {
                                    instrument: inst,
                                    interval,
                                    candle,
                                    is_final: k.is_final_bar,
                                };
                                if out_tx.send(update).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    else => { break; }
                }
            }
            kline_handle.stop().await;
        });

        Ok((StreamHandle::new(forwarder, stop_tx), out_rx))
    }
}
