use object_store::path::Path;
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, Result, S3ObjectNamespace};

use crate::{S3Config, parse_endpoint};

impl S3Config {
    /// Resolves the declared Classic namespace for offline qualification, using only explicit configuration.
    pub fn declared_classic_key_scope(
        &self,
        root: &ObjectKey,
    ) -> Result<(S3ObjectNamespace, ObjectKey)> {
        let Some(region) = self.region.as_ref().filter(|region| !region.is_empty()) else {
            return Err(invalid(
                "An explicit S3 region is required for key qualification",
            ));
        };
        if self.bucket.is_empty() || self.terminal_scope.is_some() {
            return Err(invalid("A Classic S3 bucket must be explicitly configured"));
        }
        self.require_cell_root(root)?;
        let endpoint = self.endpoint.as_deref().map(parse_endpoint).transpose()?;
        let (endpoint, _) = self.address_endpoint(endpoint)?;
        let namespace = S3ObjectNamespace {
            bucket: self.bucket.clone(),
            endpoint: endpoint.map(|endpoint| endpoint.to_string()),
            region: Some(region.clone()),
        };
        Ok((namespace, self.object_prefix(root)?))
    }
}

/// Returns the literal JVM and legacy RTDB S3 keys for a fully rooted logical path.
pub fn classic_key_aliases(key: &ObjectKey) -> Result<(String, String)> {
    let literal = key.as_str();
    Path::parse(literal)
        .map_err(|_| invalid("The logical path is outside the shared Classic S3 key domain"))?;
    let legacy = Path::from(literal).to_string();
    if legacy.len() > 1024 || literal.len() > 1024 {
        return Err(invalid(
            "A Classic S3 key spelling exceeds the provider's key length limit",
        ));
    }
    Ok((literal.to_owned(), legacy))
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.s3/classic-key-scope", message)
}
