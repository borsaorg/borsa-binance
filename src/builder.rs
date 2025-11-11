use std::sync::Arc;

use borsa_core::connector::BorsaConnector;
use borsa_middleware::ConnectorBuilder as GenericConnectorBuilder;

use crate::BinanceConnector;

/// Type alias for the generic `ConnectorBuilder` specialized for `BinanceConnector`.
pub type BinanceConnectorBuilder = GenericConnectorBuilder;

impl BinanceConnector {
    /// Creates a new `ConnectorBuilder` for a `BinanceConnector`.
    ///
    /// This is the recommended way to construct a connector, as it allows for
    /// easy configuration of middleware (caching, rate limiting, etc.).
    #[must_use]
    pub fn builder(api_key: String, secret_key: String) -> BinanceConnectorBuilder {
        let raw: Arc<dyn BorsaConnector> = Arc::new(Self::new_with_keys(api_key, secret_key));
        GenericConnectorBuilder::new(raw)
    }
}
