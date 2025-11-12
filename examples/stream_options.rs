//! Streams option tickers using the Binance connector.
//!
//! # Running
//! Preferred (explicit symbols):
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... \
//! BINANCE_OPTION_SYMBOLS="BTC-YYYYMMDD-XXXXX-C,BTC-YYYYMMDD-XXXXX-P" \
//! cargo run --example stream_options
//! ```
//! Or, derive a couple of contracts from the nearest expiry automatically:
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_UNDERLYING=BTCUSDT cargo run --example stream_options
//! ```
//! Set `BINANCE_STREAM_LIMIT` to limit the number of printed updates (default: 5).

use std::env;
use std::time::Duration;
use borsa_binance::BinanceConnector;
use borsa_core::connector::{OptionChainProvider, OptionStreamProvider};
use borsa_core::{AssetKind, Instrument, OptionUpdate};
use tokio::time::timeout;

fn parse_option_symbols() -> Vec<String> {
    env::var("BINANCE_OPTION_SYMBOLS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn symbol_from_update(u: &OptionUpdate) -> Option<&str> {
    match u.instrument.id() {
        borsa_core::IdentifierScheme::Security(sec) => Some(sec.symbol.as_str()),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret_key = env::var("BINANCE_API_SECRET").unwrap_or_default();
    let limit: usize = env::var("BINANCE_STREAM_LIMIT").ok().and_then(|s| s.parse().ok()).unwrap_or(5);

    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    // Build instruments from explicit env symbols, or derive a couple from nearest expiry
    let option_instruments: Vec<Instrument> = {
        let env_syms = parse_option_symbols();
        if !env_syms.is_empty() {
            env_syms
                .iter()
                .map(|s| Instrument::from_symbol(s, AssetKind::Option))
                .collect::<Result<_, _>>()?
        } else {
            let underlying = env::var("BINANCE_UNDERLYING").unwrap_or_else(|_| "BTCUSDT".to_string());
            let underlying_inst = Instrument::from_symbol(&underlying, AssetKind::Crypto)?;
            let chain = connector.option_chain(&underlying_inst, None).await?;
            let mut picks: Vec<Instrument> = Vec::new();
            if let Some(first_call) = chain.calls.first() {
                picks.push(first_call.instrument.clone());
            }
            if let Some(first_put) = chain.puts.first() {
                picks.push(first_put.instrument.clone());
            }
            if picks.is_empty() {
                eprintln!("No options found for underlying {}; set BINANCE_OPTION_SYMBOLS instead.", underlying);
            }
            picks
        }
    };
    if option_instruments.is_empty() {
        return Ok(());
    }
    
    let symbols_pretty = option_instruments
        .iter()
        .filter_map(|i| match i.id() {
            borsa_core::IdentifierScheme::Security(sec) => Some(sec.symbol.clone()),
            _ => None,
        })
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let (handle, mut rx) = connector.stream_options(&option_instruments).await?;
    println!("Streaming option tickers for: {}", symbols_pretty);
    
    let mut printed = 0usize;
    while printed < limit {
        match timeout(Duration::from_secs(60), rx.recv()).await {
            Ok(Some(update)) => {
                let sym = symbol_from_update(&update).unwrap_or("UNKNOWN");
                println!(
                    "[{}] last_price={:?} iv={:?} ts={}",
                    sym, update.last_price, update.implied_volatility, update.ts
                );
                printed += 1;
            }
            Ok(None) => {
                println!("Stream ended.");
                break;
            }
            Err(_) => {
                println!("No updates for 60s, stopping.");
                break;
            }
        }
    }
    handle.stop().await;
    println!("Stopped stream.");
    Ok(())
}


