use std::path::PathBuf;

use rtdb_tx::transact::native_dependent_disposition_key;

use super::{client::Connection, *};

#[derive(Clone, Debug)]
pub struct Options {
    mode: Mode,
    connection: PathBuf,
    request: PathBuf,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Settle,
    Status,
}

impl Options {
    pub(crate) fn parse(mut args: impl Iterator<Item = String>) -> Result<Self> {
        let mode = match args.next().as_deref() {
            Some("settle") => Mode::Settle,
            Some("status") => Mode::Status,
            _ => {
                return Err(crate::cli::invalid(
                    "Specify native-dependent-custody settle or status",
                ));
            }
        };
        let mut connection = None;
        let mut request = None;
        while let Some(flag) = args.next() {
            let field = match flag.as_str() {
                "--connection-file" if connection.is_none() => &mut connection,
                "--request-file" if request.is_none() => &mut request,
                _ => {
                    return Err(crate::cli::invalid(
                        "Unknown or repeated native-dependent-custody option",
                    ));
                }
            };
            *field = Some(PathBuf::from(
                args.next()
                    .filter(|path| !path.is_empty() && !path.starts_with('-'))
                    .ok_or_else(|| crate::cli::invalid(format!("{flag} requires a path")))?,
            ));
        }
        Ok(Self {
            mode,
            connection: connection
                .ok_or_else(|| crate::cli::invalid("Specify --connection-file"))?,
            request: request.ok_or_else(|| crate::cli::invalid("Specify --request-file"))?,
        })
    }

    pub async fn run(self) -> Result<impl Serialize> {
        let connection = Connection::from_profile(read_json(&self.connection)?)?;
        let request: SettlementRequest = read_json(&self.request)?;
        let Settlement {
            destination,
            scope,
            fence,
        } = &request.settlement;
        if request.version != VERSION
            || destination.is_empty()
            || !(16..=64).contains(&fence.revoke_key.len())
            || fence.revoked_at.incarnation() != scope.reservation.source.basis.incarnation()
            || scope.destination.incarnation() != scope.reservation.target.incarnation
            || fence.revoked_at <= scope.source_enrolled_at
        {
            return Err(refused());
        }
        let path = match self.mode {
            Mode::Settle => SETTLE,
            Mode::Status => STATUS,
        };
        let reply: SettlementResult = connection.post(path, &request).await?;
        let expected = native_dependent_disposition_key(*scope);
        match reply {
            SettlementResult::Pending { key } if key == expected => Ok(reply),
            SettlementResult::Settled { key, basis }
                if key == expected
                    && basis.incarnation() == fence.revoked_at.incarnation()
                    && basis > fence.revoked_at =>
            {
                Ok(reply)
            }
            _ => Err(refused()),
        }
    }
}
