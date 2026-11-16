import copy
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from source_gate import AREAS, DESIGN, MANIFEST, REFERENCE, TRACES, assess, require_phase_two, _working_source


SOURCE = "rust/crates/example/src/lib.rs"
CONTENT = b"pub fn ready() -> bool { true }\n"


def complete_review(revision=None):
    def entry():
        return {"status": "reviewed", "conclusion": "The production path reaches the required behavior.",
                "implementation_paths": [SOURCE], "evidence": {SOURCE: hashlib.sha256(CONTENT).hexdigest()}}
    return {"schema_version": 1, "reference_revision": REFERENCE, "design_revision": DESIGN,
            "implementation_revision": revision, "areas": {key: entry() for key in AREAS},
            "traces": {key: entry() for key in TRACES}, "findings": []}


def finding(kind):
    return {"id": "remaining-work", "kind": kind, "status": "open", "targets": [AREAS[0]],
            "requirement": "Complete the identified behavior and retain its evidence."}


class SourceReviewTests(unittest.TestCase):
    def setUp(self):
        self.review = complete_review()
        self.sources = {SOURCE: CONTENT}

    def assess(self):
        return assess(self.review, self.sources.__getitem__)

    def test_runtime_qualification_does_not_delay_the_first_compilation(self):
        self.review["findings"] = [finding("qualification")]
        result = self.assess()
        self.assertTrue(result["source_complete"])
        self.assertFalse(result["implementation_revision_recorded"])
        self.assertEqual(result["compatibility_claim"], "none")
        self.assertEqual(result["phase_two_qualifications"], self.review["findings"])

    def test_missing_area_or_trace_cannot_shrink_the_review(self):
        for section, identity in [("areas", AREAS[0]), ("traces", TRACES[-1])]:
            with self.subTest(section=section):
                self.review = complete_review()
                del self.review[section][identity]
                with self.assertRaisesRegex(ValueError, "exactly the required"):
                    self.assess()

    def test_unreviewed_source_remains_visible_beside_open_implementation_findings(self):
        self.review["areas"][AREAS[0]]["status"] = "pending"
        self.review["findings"] = [finding("implementation")]
        result = self.assess()
        self.assertFalse(result["source_complete"])
        self.assertEqual([item["id"] for item in result["pending_reviews"]], [AREAS[0]])
        self.assertEqual(result["implementation_blockers"], self.review["findings"])

    def test_review_findings_block_source_completion(self):
        self.review["findings"] = [finding("review")]
        self.assertFalse(self.assess()["source_complete"])

    def test_resolved_findings_require_the_actual_resolution(self):
        item = finding("implementation")
        item["status"] = "resolved"
        self.review["findings"] = [item]
        with self.assertRaisesRegex(ValueError, "resolution"):
            self.assess()
        item["resolution"] = "The reviewed production path now implements the missing behavior."
        self.assertTrue(self.assess()["source_complete"])

    def test_missing_production_source_is_an_error_even_before_review(self):
        self.review["areas"][AREAS[0]]["status"] = "pending"
        self.sources.clear()
        with self.assertRaisesRegex(ValueError, "implementation path is unavailable"):
            self.assess()

    def test_reviewed_sources_must_have_unchanged_evidence(self):
        self.sources[SOURCE] += b"pub fn changed() {}\n"
        with self.assertRaisesRegex(ValueError, "source review is stale"):
            self.assess()

    def test_reviewed_evidence_cannot_omit_a_production_path(self):
        self.review["areas"][AREAS[0]]["evidence"] = {}
        with self.assertRaisesRegex(ValueError, "lacks reviewed evidence"):
            self.assess()

    def test_unknown_targets_and_duplicate_findings_are_rejected(self):
        item = finding("implementation")
        self.review["findings"] = [item, copy.deepcopy(item)]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.assess()
        self.review["findings"] = [item]
        item["targets"] = ["not-a-design-surface"]
        with self.assertRaisesRegex(ValueError, "required source review entries"):
            self.assess()

    def test_repository_evidence_cannot_read_outside_its_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repository"
            root.mkdir()
            outside = Path(directory) / "outside.rs"
            outside.write_bytes(CONTENT)
            (root / "link.rs").symlink_to(outside)
            for path in ["../outside.rs", str(outside), "link.rs", "./code.rs", "a//b.rs"]:
                with self.subTest(path=path), self.assertRaises(ValueError):
                    _working_source(root, path)


class PhaseTwoGateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init")
        self.git("config", "user.name", "source review fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        source = self.root / SOURCE
        source.parent.mkdir(parents=True)
        source.write_bytes(CONTENT)
        self.git("add", ".")
        self.git("commit", "-m", "source implementation")
        self.revision = self.git("rev-parse", "HEAD")
        self.review = complete_review(self.revision)
        self.state = {"reference_revision": REFERENCE, "design_revision": DESIGN,
                      "implementation_revision": self.revision, "source_completeness_review": "complete",
                      "phase_2_authorized_by_completeness_gate": True}
        self.write_review()

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), "-c", "core.hooksPath=/dev/null", *args], stderr=subprocess.DEVNULL).decode().strip()

    def write_review(self):
        path = self.root / MANIFEST
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.review))
        (self.root / "rust/implementation-status.json").write_text(json.dumps(self.state))

    def test_a_boolean_flag_does_not_replace_the_source_review(self):
        self.review["traces"][TRACES[0]]["status"] = "pending"
        self.write_review()
        with self.assertRaisesRegex(ValueError, "source review is incomplete"):
            require_phase_two(self.root, True)

    def test_truthy_values_do_not_authorize_execution(self):
        for value in [1, "true", False, None]:
            with self.subTest(value=value):
                self.state["phase_2_authorized_by_completeness_gate"] = value
                self.write_review()
                with self.assertRaisesRegex(ValueError, "not completed"):
                    require_phase_two(self.root, True)
        with self.assertRaisesRegex(ValueError, "requires --phase-two"):
            require_phase_two(self.root, 1)

    def test_revision_mismatch_cannot_authorize_execution(self):
        self.review["implementation_revision"] = "0" * 40
        self.write_review()
        with self.assertRaisesRegex(ValueError, "different implementation revisions"):
            require_phase_two(self.root, True)

    def test_missing_or_stale_historical_source_cannot_authorize_execution(self):
        self.review["areas"][AREAS[0]]["evidence"][SOURCE] = "0" * 64
        self.write_review()
        with self.assertRaisesRegex(ValueError, "source review is stale"):
            require_phase_two(self.root, True)
        self.review = complete_review(self.revision)
        item = self.review["areas"][AREAS[0]]
        item["implementation_paths"] = ["rust/missing.rs"]
        item["evidence"] = {"rust/missing.rs": hashlib.sha256(CONTENT).hexdigest()}
        self.write_review()
        with self.assertRaisesRegex(ValueError, "implementation path is unavailable"):
            require_phase_two(self.root, True)

    def test_phase_two_repairs_keep_the_original_reviewed_boundary(self):
        (self.root / SOURCE).write_bytes(b"pub fn repaired() -> bool { true }\n")
        self.git("add", ".")
        self.git("commit", "-m", "phase two compiler repair")
        result = require_phase_two(self.root, True)
        self.assertTrue(result["source_complete"])
        self.assertEqual(result["implementation_revision"], self.revision)
        self.assertNotEqual(self.git("rev-parse", "HEAD"), self.revision)

    def test_unrelated_implementation_history_cannot_reuse_the_gate(self):
        self.git("checkout", "--orphan", "unrelated")
        self.git("add", ".")
        self.git("commit", "-m", "unrelated implementation")
        with self.assertRaisesRegex(ValueError, "Git provenance"):
            require_phase_two(self.root, True)


if __name__ == "__main__":
    unittest.main()
