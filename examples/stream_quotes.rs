//! Streams spot trade quotes using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_SYMBOLS="BTCUSDT,ETHUSDT" cargo run --example stream_quotes
//! ```
//! Set `BINANCE_STREAM_LIMIT` to limit number of printed updates (default: 5).
//! Use Ctrl-C to stop early.

use std::env;
use std::time::Duration;
use borsa_binance::BinanceConnector;
use borsa_core::connector::StreamProvider;
use borsa_core::{AssetKind, Instrument};
use tokio::time::timeout;

fn parse_symbols() -> Vec<String> {
    env::var("BINANCE_SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
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
    let limit: usize = env::var("BINANCE_STREAM_LIMIT").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let symbols = parse_symbols();

    let instruments: Vec<Instrument> = symbols
        .iter()
        .map(|s| Instrument::from_symbol(s, AssetKind::Crypto))
        .collect::<Result<_, _>>()?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let (handle, mut rx) = connector.stream_quotes(&instruments).await?;
    println!("Streaming spot trades for: {}", symbols.join(", "));

    let mut printed = 0usize;
    while printed < limit {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(update)) => {
                let sym = symbol_from_instrument(&update.instrument).unwrap_or("UNKNOWN");
                println!(
                    "[{}] price={:?} volume={:?}",
                    sym, update.price, update.volume
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


