use binance::{model::PriceStats, options::model::Options24hrTickerEvent};
use borsa_core::{
    AssetKind, BorsaError, Currency, Instrument, IsoCurrency, Money, OptionUpdate, Quote,
};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;

/// Infers the quote currency from a spot symbol (e.g., "BTCUSDT" -> "USDT").
/// This is a simplified helper.
fn infer_currency_from_spot_symbol(symbol: &str) -> Result<Currency, BorsaError> {
    // Map common quote tokens to ISO USD for now (building block; refine as needed).
    // If you require exact token tracking (USDT vs USDC), represent as non-ISO currency instead.
    if symbol.ends_with("USDT") || symbol.ends_with("USDC") {
        Ok(Currency::Iso(IsoCurrency::USD))
    } else {
        Err(BorsaError::Data(format!(
            "Could not infer quote currency from symbol {}",
            symbol
        )))
    }
}

/// Converts a float price to `Money` using a safe decimal conversion.
fn f64_to_money(price: f64, currency: Currency) -> Result<Money, BorsaError> {
    let dec = Decimal::from_f64_retain(price)
        .ok_or_else(|| BorsaError::Data("invalid float for price conversion".into()))?;
    Money::new(dec, currency).map_err(BorsaError::from)
}

/// Converts Binance `PriceStats` to a Borsa `Quote`.
pub fn binance_price_stats_to_quote(
    stats: PriceStats,
    instrument: Instrument,
) -> Result<Quote, BorsaError> {
    let symbol_str = match instrument.id() {
        borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
        _ => {
            return Err(BorsaError::InvalidArg(
                "instrument is not a security".into(),
            ));
        }
    };
    let currency = infer_currency_from_spot_symbol(symbol_str)?;

    let price = f64_to_money(stats.last_price, currency.clone())?;
    let prev_close = f64_to_money(stats.prev_close_price, currency)?;
    let volume = stats.volume.to_u64(); // `f64` to `u64` conversion (lossy by design for a building block)

    Ok(Quote {
        instrument,
        shortname: None,
        price: Some(price),
        previous_close: Some(prev_close),
        day_volume: volume,
        // Keep exchange/market state unset in this minimal mapping layer.
        exchange: None,
        market_state: None,
    })
}

/// Converts a Binance `Options24hrTickerEvent` to a Borsa `OptionUpdate`.
pub fn binance_option_ticker_to_update(
    ticker: Options24hrTickerEvent,
) -> Result<OptionUpdate, BorsaError> {
    let sym: &str = ticker.symbol.as_str();
    let instrument = borsa_core::Instrument::from_symbol(sym, AssetKind::Option)
        .map_err(|e| BorsaError::Data(format!("Invalid option symbol from Binance: {}", e)))?;

    let ts = Utc
        .timestamp_millis_opt(ticker.event_time as i64)
        .single()
        .ok_or_else(|| BorsaError::Data("Invalid event timestamp".to_string()))?;

    // Minimal mapping: last price and IV only (no extra logic).
    // Represent values in USD (ISO) for price to keep a consistent baseline.
    let last_price = ticker.mark_price.and_then(|p| {
        let dec = Decimal::from_f64_retain(p)?;
        Money::new(dec, Currency::Iso(IsoCurrency::USD)).ok()
    });
    let implied_volatility = ticker.implied_volatility.and_then(Decimal::from_f64_retain);

    Ok(OptionUpdate {
        instrument,
        ts,
        bid: None,
        ask: None,
        last_price,
        implied_volatility,
    })
}

// Helper trait to convert f64 to u64, capping at 0 if negative.
trait ToU64 {
    fn to_u64(&self) -> Option<u64>;
}

impl ToU64 for f64 {
    fn to_u64(&self) -> Option<u64> {
        if *self < 0.0 {
            Some(0)
        } else {
            Some(*self as u64)
        }
    }
}
