//! Fetches a spot quote using the Binance connector.
//!
//! # Running
//! ```bash
//! BINANCE_API_KEY=... BINANCE_API_SECRET=... cargo run --example quote
//! ```
//! The API credentials are optional for public market data, but recommended.

use std::env;

use borsa_binance::BinanceConnector;
use borsa_core::connector::QuoteProvider;
use borsa_core::{AssetKind, Instrument};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret_key = env::var("BINANCE_API_SECRET").unwrap_or_default();
    let symbol = env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let instrument = Instrument::from_symbol(&symbol, AssetKind::Crypto)?;
    let connector = BinanceConnector::new_with_keys(api_key, secret_key);

    let quote = connector.quote(&instrument).await?;
    println!("Symbol: {}", symbol);
    println!("Last price: {:?}", quote.price);
    println!("Previous close: {:?}", quote.previous_close);
    println!("Day volume: {:?}", quote.day_volume);

    Ok(())
}
