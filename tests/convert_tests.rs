use binance::{model::PriceStats, options::model::Options24hrTickerEvent};
use borsa_binance::convert::{binance_option_ticker_to_update, binance_price_stats_to_quote};
use borsa_core::{AssetKind, Instrument};
use std::collections::BTreeMap;

#[test]
fn price_stats_to_quote_maps_usdt_and_volume() {
    // Arrange
    let inst = Instrument::from_symbol("BTCUSDT", AssetKind::Crypto).unwrap();
    let stats = PriceStats {
        symbol: "BTCUSDT".to_string(),
        price_change: "0".to_string(),
        price_change_percent: "0".to_string(),
        weighted_avg_price: "0".to_string(),
        prev_close_price: 100_000.0,
        last_price: 101_000.5,
        bid_price: 0.0,
        ask_price: 0.0,
        open_price: 0.0,
        high_price: 0.0,
        low_price: 0.0,
        volume: 12345.67,
        open_time: 0,
        close_time: 0,
        first_id: 0,
        last_id: 0,
        count: 0,
    };

    // Act
    let q = binance_price_stats_to_quote(stats, inst).unwrap();

    // Assert: USDT pairs use Currency::USDT (6 decimal places)
    let price = q.price.unwrap();
    assert_eq!(price.currency().code(), "USDT");
    assert!(price.amount() > rust_decimal::Decimal::ZERO);
    assert_eq!(q.day_volume, Some(12345)); // f64 -> u64 lossy conversion
}

#[test]
fn options_ticker_to_update_maps_iv_and_price() {
    // Arrange
    let t = Options24hrTickerEvent {
        event_type: "24hrTicker".to_string(),
        event_time: 1_700_000_000_000,
        symbol: "BTC-251226-100000-C".to_string(),
        delta: None,
        gamma: None,
        theta: None,
        rho: None,
        vega: None,
        implied_volatility: Some(0.45),
        mark_price: Some(2500.25),
        rest: BTreeMap::new(),
    };

    // Act
    let upd = binance_option_ticker_to_update(t).unwrap();

    // Assert
    assert_eq!(
        match upd.instrument.id() {
            borsa_core::IdentifierScheme::Security(sec) => sec.symbol.as_str(),
            _ => "",
        },
        "BTC-251226-100000-C"
    );
    assert!(upd.implied_volatility.is_some());
    assert!(upd.last_price.is_some());
    // Ensure USD baseline for price
    assert_eq!(upd.last_price.as_ref().unwrap().currency().code(), "USD");
}
