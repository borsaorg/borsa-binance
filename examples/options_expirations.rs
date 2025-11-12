//! Retrieves available option expirations for an underlying using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_UNDERLYING=BTCUSDT cargo run --example options_expirations
//! ```

use std::env;
use borsa_binance::BinanceConnector;
use borsa_core::connector::OptionsExpirationsProvider;
use borsa_core::{AssetKind, Instrument};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret_key = env::var("BINANCE_API_SECRET").unwrap_or_default();
    let underlying = env::var("BINANCE_UNDERLYING").unwrap_or_else(|_| "BTCUSDT".to_string());
    
    let underlying_inst = Instrument::from_symbol(&underlying, AssetKind::Crypto)?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let expirations = connector.options_expirations(&underlying_inst).await?;
    println!("Underlying: {}", underlying);
    println!("Found {} expirations (seconds since epoch):", expirations.len());
    for ts in expirations.iter().take(20) {
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(*ts, 0)
            .map(|d| d.date_naive())
            .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        println!("- {} ({}s)", dt, ts);
    }
    if expirations.len() > 20 {
        println!("... and {} more", expirations.len() - 20);
    }
    Ok(())
}


