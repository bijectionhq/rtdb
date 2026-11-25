use rtdb_db::{ClassicInstallation, classic_import_basis};
use rtdb_tx::transact::workload::{Applied, Fence};
use rtdb_types::{Timestamp, TransactionKey, TxId, rtdb_v1::Basis};

use super::{
    super::{AppliedAt, Attempt, Decision, Scope},
    *,
};
use crate::{
    cli::{self, Command},
    multi::classic_ingress::{HeldAssignment, HeldReady, ReadySelection},
};

fn witness() -> Witness {
    let catalog = [1; 16];
    let destination = [2; 16];
    let classic_cut = TransactionKey {
        tx_id: TxId(9),
        system_time: Timestamp(10),
    };
    Witness {
        decision: Decision {
            scope: Scope {
                domain: "migration-target".into(),
                authority: Uuid::from_bytes(destination),
            },
            attempt: Attempt {
                command_id: [3; 16],
                command_digest: [4; 32],
            },
            begin_sha256: [5; 32],
            applied_at: AppliedAt {
                history_id: [6; 16],
                sequence: 7,
            },
            decision_sha256: [8; 32],
        },
        held_ready: HeldReady {
            binding_sha256: [9; 32],
            ready: ReadySelection {
                endpoint: "https://managed.internal".into(),
                placement: HeldAssignment {
                    epoch: 8,
                    assignment: [10; 16],
                    holder: [11; 16],
                },
                classic_cut,
                native_basis: classic_import_basis(destination, classic_cut.tx_id).unwrap(),
                reserved_at: Basis::new(catalog, 10),
                installation: ClassicInstallation {
                    basis: Basis::new(destination, 10),
                    system_time: Timestamp(10),
                    checkpoint_lsn: 11,
                    checkpoint_writer_epoch: 1,
                    key_generation: 1,
                    checkpoint_sha256: [12; 32],
                    manifest: "manifest".into(),
                    manifest_ciphertext_sha256: [13; 32],
                },
            },
            recorded_at: Basis::new(catalog, 11),
        },
    }
}

#[test]
fn classic_opening_cli_requires_an_explicit_verb_and_sends_the_exact_typed_witness() {
    let witness = witness();
    assert!(witness.valid(Uuid::from_bytes([1; 16])));
    for (verb, operation) in [
        ("open", "open_classic_migration_destination"),
        ("exclude", "exclude_classic_migration_opening"),
        ("status", "classic_migration_opening_status"),
    ] {
        let args = [
            "classic-opening",
            verb,
            "--connection-file",
            "private.json",
            "--witness-file",
            "opening.json",
        ];
        let Command::ClassicOpening(options) =
            cli::parse(args.map(str::to_owned), &|_| None).unwrap()
        else {
            panic!("expected classic-opening CLI options");
        };
        assert_eq!(
            serde_json::to_value(options.action.request(&witness)).unwrap(),
            serde_json::json!({
                "operation": operation,
                "decision": witness.decision,
                "held_ready": witness.held_ready,
            }),
        );
    }
    for args in [
        vec!["classic-opening", "--connection-file", "connection.json"],
        vec!["classic-opening", "open", "--witness-file", "opening.json"],
        vec![
            "classic-opening",
            "open",
            "--connection-file",
            "one.json",
            "--connection-file",
            "two.json",
            "--witness-file",
            "opening.json",
        ],
    ] {
        assert!(cli::parse(args.into_iter().map(str::to_owned), &|_| None).is_err());
    }
}

#[test]
fn classic_opening_cli_accepts_only_exact_ordered_terminals() {
    let witness = witness();
    let ordered_at = Basis::new([1; 16], 12);
    let fence = Fence {
        placement: witness.placement(),
        applied: Applied {
            namespace: [14; 32],
            key: witness.decision.decision_sha256.to_vec(),
            basis: Basis::new([2; 16], 11).to_bytes(),
            system_time_micros: 10,
        },
    };
    let opened = |witness: &Witness, ordered_at, workload_fence| Reply::Opened {
        decision: witness.decision.clone(),
        held_ready: witness.held_ready.clone(),
        ordered_at,
        workload_fence,
    };
    for action in [Action::Open, Action::Status] {
        let reply = opened(&witness, ordered_at, fence.clone());
        let wire = serde_json::to_vec(&reply).unwrap();
        assert!(
            action
                .correlate(&witness, serde_json::from_slice(&wire).unwrap())
                .is_ok()
        );
    }
    assert!(
        Action::Exclude
            .correlate(&witness, opened(&witness, ordered_at, fence.clone()))
            .is_err()
    );
    for action in [Action::Open, Action::Exclude, Action::Status] {
        assert!(action.correlate(&witness, Reply::Unresolved).is_err());
        assert!(action.correlate(&witness, Reply::Refused).is_err());
        let reply = Reply::Excluded {
            decision: witness.decision.clone(),
            held_ready: witness.held_ready.clone(),
            ordered_at,
        };
        assert!(action.correlate(&witness, reply).is_ok());
    }

    let mut wrong_decision = witness.clone();
    wrong_decision.decision.attempt.command_digest = [15; 32];
    let mut wrong_ready = witness.clone();
    wrong_ready.held_ready.ready.installation.manifest = "other".into();
    let mut wrong_fence = fence.clone();
    wrong_fence.applied.key = [16; 32].to_vec();
    let mut wrong_placement = fence.clone();
    wrong_placement.placement.epoch += 1;
    let mut wrong_stream = fence.clone();
    wrong_stream.applied.basis = Basis::new([18; 16], 11).to_bytes();
    let mut wrong_time = fence.clone();
    wrong_time.applied.system_time_micros = 9;
    for reply in [
        opened(&wrong_decision, ordered_at, fence.clone()),
        opened(&wrong_ready, ordered_at, fence.clone()),
        opened(&witness, Basis::new([17; 16], 12), fence.clone()),
        opened(&witness, witness.held_ready.recorded_at, fence),
        opened(&witness, ordered_at, wrong_fence),
        opened(&witness, ordered_at, wrong_placement),
        opened(&witness, ordered_at, wrong_stream),
        opened(&witness, ordered_at, wrong_time),
    ] {
        assert!(Action::Status.correlate(&witness, reply).is_err());
    }
}
