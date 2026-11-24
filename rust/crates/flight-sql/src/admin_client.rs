use std::{net::IpAddr, time::Duration};

use arrow_flight::{Action, flight_service_client::FlightServiceClient};
use prost::Message;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tonic::{
    Code, Request, Status,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use crate::rtdb::{admin::v1 as proto, v1 as data};

pub struct DeploymentAdminClient {
    flight: FlightServiceClient<Channel>,
    authorization: MetadataValue<tonic::metadata::Ascii>,
}

impl DeploymentAdminClient {
    pub async fn connect(endpoint: String, authorization: &str) -> Result<Self> {
        let mut authorization: MetadataValue<_> = authorization.parse().map_err(|_| {
            Anomaly::incorrect(
                "rtdb.admin/credential",
                "Invalid Flight authorization value",
            )
        })?;
        authorization.set_sensitive(true);
        let mut endpoint = Endpoint::from_shared(endpoint)
            .map_err(transport_error)?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30));
        match endpoint.uri().scheme_str() {
            Some("https") => {
                endpoint = endpoint
                    .tls_config(ClientTlsConfig::new().with_native_roots())
                    .map_err(transport_error)?;
            }
            Some("http") if endpoint.uri().host().is_some_and(is_loopback) => (),
            _ => {
                return Err(Anomaly::incorrect(
                    "rtdb.admin/endpoint",
                    "Use HTTPS or a loopback HTTP endpoint",
                ));
            }
        }
        let channel = endpoint.connect().await.map_err(transport_error)?;
        Ok(Self {
            flight: FlightServiceClient::new(channel),
            authorization,
        })
    }

    pub async fn create(
        &mut self,
        request: proto::CreateDeploymentRequest,
    ) -> Result<proto::Deployment> {
        let deployment = request.deployment.clone();
        let incarnation = request.incarnation.clone();
        let reply: proto::Deployment = self.invoke(crate::admin::CREATE, request).await?;
        if reply.deployment != deployment
            || incarnation.is_some_and(|incarnation| reply.incarnation != incarnation)
        {
            return Err(invalid_result());
        }
        Ok(reply)
    }

    pub async fn delete(
        &mut self,
        request: proto::DeleteDeploymentRequest,
    ) -> Result<proto::ErasureProof> {
        self.invoke(crate::admin::DELETE, request).await
    }

    pub async fn fork(
        &mut self,
        request: proto::ForkDeploymentRequest,
    ) -> Result<proto::Deployment> {
        self.invoke(crate::admin::FORK, request).await
    }

    pub async fn export(
        &mut self,
        request: proto::ExportCheckpointRequest,
    ) -> Result<proto::CheckpointExport> {
        self.invoke(crate::admin::EXPORT, request).await
    }

    pub async fn restore(
        &mut self,
        request: proto::RestoreDeploymentRequest,
    ) -> Result<proto::Deployment> {
        self.invoke(crate::admin::RESTORE, request).await
    }

    async fn invoke<M: Message, R: Message + Default>(
        &mut self,
        action: &str,
        body: M,
    ) -> Result<R> {
        let mut request = Request::new(Action {
            r#type: action.into(),
            body: body.encode_to_vec().into(),
        });
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        let mut results = self
            .flight
            .do_action(request)
            .await
            .map_err(status)?
            .into_inner();
        let frame = results
            .message()
            .await
            .map_err(status)?
            .ok_or_else(invalid_result)?;
        if results.message().await.map_err(status)?.is_some() {
            return Err(invalid_result());
        }
        R::decode(frame.body).map_err(|_| invalid_result())
    }
}

fn is_loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn transport_error(error: tonic::transport::Error) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.admin/transport",
        error.to_string(),
    )
}

fn invalid_result() -> Anomaly {
    Anomaly::fault(
        "rtdb.admin/result",
        "The administration reply did not contain one valid result",
    )
}

fn status(error: Status) -> Anomaly {
    let category = match error.code() {
        Code::InvalidArgument => AnomalyCategory::Incorrect,
        Code::NotFound => AnomalyCategory::NotFound,
        Code::PermissionDenied | Code::Unauthenticated => AnomalyCategory::Forbidden,
        Code::AlreadyExists | Code::FailedPrecondition => AnomalyCategory::Conflict,
        Code::ResourceExhausted => AnomalyCategory::Busy,
        Code::Unimplemented => AnomalyCategory::Unsupported,
        Code::DeadlineExceeded | Code::Unavailable => AnomalyCategory::Unavailable,
        Code::Cancelled | Code::Aborted => AnomalyCategory::Interrupted,
        _ => AnomalyCategory::Fault,
    };
    if let Some(detail) = error
        .metadata()
        .get_bin("rtdb-error-bin")
        .and_then(|encoded| encoded.to_bytes().ok())
        .and_then(|bytes| data::Error::decode(bytes).ok())
    {
        return Anomaly::new(category, detail.code, detail.message);
    }
    Anomaly::new(category, "rtdb.admin/request", error.message())
}
