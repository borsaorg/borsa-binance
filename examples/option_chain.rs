//! Retrieves an option chain (calls/puts) for an underlying using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_UNDERLYING=BTCUSDT cargo run --example option_chain
//! ```
//! Optionally set `BINANCE_EXPIRY` to a UNIX timestamp (seconds) to target a specific day.

use std::env;
use borsa_binance::BinanceConnector;
use borsa_core::connector::OptionChainProvider;
use borsa_core::{AssetKind, Instrument};

fn parse_expiry() -> Option<i64> {
    env::var("BINANCE_EXPIRY").ok().and_then(|s| s.parse().ok())
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
    let underlying = env::var("BINANCE_UNDERLYING").unwrap_or_else(|_| "BTCUSDT".to_string());
    let target_expiry = parse_expiry();

    let underlying_inst = Instrument::from_symbol(&underlying, AssetKind::Crypto)?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let chain = connector.option_chain(&underlying_inst, target_expiry).await?;
    println!("Underlying: {}", underlying);
    println!("Calls: {} | Puts: {}", chain.calls.len(), chain.puts.len());

    println!("Sample calls (max 5):");
    for c in chain.calls.iter().take(5) {
        let sym = symbol_from_instrument(&c.instrument).unwrap_or("UNKNOWN");
        println!("- {} strike={} expiry_date={}", sym, c.strike, c.expiration_date);
    }
    println!("Sample puts (max 5):");
    for p in chain.puts.iter().take(5) {
        let sym = symbol_from_instrument(&p.instrument).unwrap_or("UNKNOWN");
        println!("- {} strike={} expiry_date={}", sym, p.strike, p.expiration_date);
    }
    Ok(())
}


