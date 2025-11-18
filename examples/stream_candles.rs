//! Streams spot kline candles using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_SYMBOLS="BTCUSDT" BINANCE_INTERVAL=1s cargo run --example stream_candles
//! ```
//! - BINANCE_INTERVAL: 1s or 1h (default: 1s)
//! - BINANCE_STREAM_LIMIT: number of updates to print (default: 5)

use borsa_binance::BinanceConnector;
use borsa_core::connector::CandleStreamProvider;
use borsa_core::{AssetKind, Instrument, Interval};
use std::env;
use std::time::Duration;
use tokio::time::timeout;

fn parse_symbols() -> Vec<String> {
    env::var("BINANCE_SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_interval() -> Interval {
    match env::var("BINANCE_INTERVAL")
        .unwrap_or_else(|_| "1s".to_string())
        .as_str()
    {
        "1h" | "1H" => Interval::I1h,
        _ => Interval::I1s,
    }
}

fn symbol_from_instrument(inst: &Instrument) -> Option<&str> {
    match inst.id() {
        borsa_core::IdentifierScheme::Security(sec) => Some(sec.symbol.as_str()),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret_key = env::var("BINANCE_API_SECRET").unwrap_or_default();
    let limit: usize = env::var("BINANCE_STREAM_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let symbols = parse_symbols();
    let interval = parse_interval();

    let instruments: Vec<Instrument> = symbols
        .iter()
        .map(|s| Instrument::from_symbol(s, AssetKind::Crypto))
        .collect::<Result<_, _>>()?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let (handle, mut rx) = connector.stream_candles(&instruments, interval).await?;
    println!(
        "Streaming candles for: {} (interval: {:?})",
        symbols.join(", "),
        interval
    );

    let mut printed = 0usize;
    while printed < limit {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(update)) => {
                let sym = symbol_from_instrument(&update.instrument).unwrap_or("UNKNOWN");
                let c = update.candle;
                println!(
                    "[{}] ts={} o={} h={} l={} c={} final={}",
                    sym, c.ts, c.open, c.high, c.low, c.close, update.is_final
                );
                printed += 1;
            }
            Ok(None) => {
                println!("Stream ended.");
                break;
            }
            Err(_) => {
                println!("No updates for 30s, stopping.");
                break;
            }
        }
    }
    handle.stop().await;
    println!("Stopped stream.");
    Ok(())
}
