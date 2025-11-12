//! Fetches 1-second historical candles using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... cargo run --example history
//! ```
//! The connector will backfill the trailing 24 hours when no explicit period is supplied.

use std::env;

use borsa_binance::BinanceConnector;
use borsa_core::connector::HistoryProvider;
use borsa_core::{AssetKind, HistoryRequest, Instrument, Interval, Range};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret_key = env::var("BINANCE_API_SECRET").unwrap_or_default();
    let symbol = env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let instrument = Instrument::from_symbol(&symbol, AssetKind::Crypto)?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let request = HistoryRequest::try_from_range(Range::D1, Interval::I1s)?;
    let history = connector.history(&instrument, request).await?;

    println!(
        "Retrieved {} candles for {} (Range::D1, Interval::I1s)",
        history.candles.len(),
        symbol
    );

    if let Some(first) = history.candles.first() {
        println!(
            "First candle: {} open={} close={}",
            first.ts, first.open, first.close
        );
    }
    if let Some(last) = history.candles.last() {
        println!(
            "Last candle:  {} open={} close={}",
            last.ts, last.open, last.close
        );
    }

    Ok(())
}
