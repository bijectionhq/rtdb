use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Point {
    pub table: TableRef,
    pub id: EntityId,
    pub valid_at: Timestamp,
}

impl Point {
    pub fn new(relation: &str, row_id: String, valid_at: Timestamp) -> Result<Self> {
        if !bounded(relation) || !relation.contains('.') || !bounded(&row_id)
            || valid_at == Timestamp::MAX
        {
            return Err(Anomaly::incorrect("rtdb/managed-workload-point",
                "Configure an exact schema-qualified relation, string row ID and finite valid time"));
        }
        Ok(Self { table: TableRef::parse(relation)?, id: EntityId::String(row_id), valid_at })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Holder {
    pub assignment_id: [u8; 16],
    pub holder: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub epoch: u64,
    pub assignment: Option<Holder>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Applied {
    pub namespace: [u8; 32],
    pub key: Vec<u8>,
    pub basis: [u8; Basis::WIDTH],
    pub system_time_micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fence {
    pub placement: Placement,
    pub applied: Applied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predecessor {
    pub fence: Fence,
    pub writer: Applied,
    pub close: Applied,
    pub closure: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Previous {
    Empty,
    Vacant { fence: Fence },
    Closed { predecessor: Predecessor },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Family { AdvancePlacement, ClaimWriter, Write, CloseWriter }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Authority {
    AdvancePlacement { installation_intent: [u8; 16], previous: Previous, next: Placement },
    ClaimWriter { fence: Fence },
    Write { fence: Fence, writer: Applied },
    CloseWriter { fence: Fence, writer: Applied, closure: [u8; 16] },
    ObservePlacement { fence: Fence },
    PredecessorEvidence { predecessor: Predecessor },
    ReceiptRecovery { original_namespace: [u8; 32], family: Family },
    AttemptSettlement { original_namespace: [u8; 32], family: Family, key: Vec<u8>, request_digest: [u8; 32] },
}

impl Authority {
    pub fn valid(&self, incarnation: [u8; 16]) -> bool {
        let applied = |value: &Applied| {
            valid_key(&value.key) && value.namespace != [0; 32]
                && Basis::from_bytes(&value.basis).is_ok_and(|basis| {
                    basis.incarnation() == incarnation && basis.position() > 0
                })
        };
        let fence = |value: &Fence| value.placement.valid() && applied(&value.applied);
        let held = |value: &Fence| value.placement.assignment.is_some() && fence(value);
        let predecessor = |value: &Predecessor| {
            held(&value.fence) && applied(&value.writer) && applied(&value.close) && value.closure != [0; 16]
        };
        match self {
            Self::AdvancePlacement { installation_intent, previous, next } => {
                *installation_intent != [0; 16] && next.valid() && match previous {
                    Previous::Empty => next.assignment.is_some(),
                    Previous::Vacant { fence: vacancy } => fence(vacancy)
                        && vacancy.placement.assignment.is_none() && next.assignment.is_some()
                        && next.epoch > vacancy.placement.epoch,
                    Previous::Closed { predecessor: prior } => predecessor(prior)
                        && next.epoch > prior.fence.placement.epoch,
                }
            }
            Self::ClaimWriter { fence } => held(fence),
            Self::Write { fence, writer } => held(fence) && applied(writer),
            Self::CloseWriter { fence, writer, closure } => held(fence) && applied(writer) && *closure != [0; 16],
            Self::ObservePlacement { fence: value } => fence(value),
            Self::PredecessorEvidence { predecessor: value } => predecessor(value),
            Self::ReceiptRecovery { original_namespace, .. } => *original_namespace != [0; 32],
            Self::AttemptSettlement { original_namespace, key, request_digest, .. } =>
                *original_namespace != [0; 32] && valid_key(key) && *request_digest != [0; 32],
        }
    }

    pub fn family(&self) -> Option<Family> {
        Some(match self {
            Self::AdvancePlacement { .. } => Family::AdvancePlacement,
            Self::ClaimWriter { .. } => Family::ClaimWriter,
            Self::Write { .. } => Family::Write,
            Self::CloseWriter { .. } => Family::CloseWriter,
            _ => return None,
        })
    }
}

impl Placement {
    fn valid(&self) -> bool {
        self.epoch > 0 && self.assignment.as_ref().is_none_or(|a| {
            a.assignment_id != [0; 16] && bounded(&a.holder)
        })
    }
}

pub fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_key(key: &[u8]) -> bool { (16..=64).contains(&key.len()) }

pub fn denied() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb/managed-workload-authority",
        "The command requires exact managed workload authority")
}
