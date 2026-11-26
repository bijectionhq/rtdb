import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import build as release
from build import HERE, capture_driver_inputs, extract_artifacts, immutable_image, jvm_recipe, selected_features, selected_jvm_variant, verify_driver_inputs


class ExtractionCommands:
    def __init__(self, directory, fail_copy=False, fail_volume_remove=False, fail_create_ack=False):
        self.directory = directory
        self.calls = []
        self.container = False
        self.volumes = set()
        self.fail_copy = fail_copy
        self.fail_volume_remove = fail_volume_remove
        self.fail_create_ack = fail_create_ack
        self.name = None
        self.exited = False
        self.executable_count = 0

    def run(self, name, argv, required=True, timeout=None):
        self.calls.append((name, argv))
        output = self.directory / f"command-{len(self.calls)}.json"
        content = ""
        if name == "artifact-container":
            self.name = argv[argv.index("--name") + 1]
            self.executable_count = argv[-1].count(" --build-info")
            self.container = True
            self.volumes.add("anonymous-volume")
            if self.fail_create_ack:
                raise RuntimeError("create acknowledgement lost")
        elif name == "inspect-artifact-container":
            if not self.container:
                raise RuntimeError("container absent")
            content = json.dumps([{"Image": "image", "Config": {"Labels": {"io.rtdb.release": self.name}},
                                  "State": {"Running": False, "Status": "exited" if self.exited else "created", "ExitCode": 0},
                                  "Mounts": [{"Type": "volume", "Name": value} for value in self.volumes]}])
        elif name == "artifact-build-info":
            self.exited = True
            content = (json.dumps({"schema_version": 1}) + "\n") * self.executable_count
        elif name.startswith("extract-"):
            if self.fail_copy:
                raise RuntimeError("copy failed")
            Path(argv[-1]).write_bytes(b"artifact")
        elif name == "remove-artifact-container":
            self.container = False
        elif name.startswith("remove-artifact-volume-"):
            if self.fail_volume_remove:
                raise RuntimeError("volume remove failed")
            self.volumes.remove(argv[-1])
        elif name == "check-artifact-container":
            content = self.name if self.container else ""
        elif name.startswith("check-artifact-volume-"):
            content = "\n".join(sorted(self.volumes))
        else:
            raise AssertionError(name)
        output.write_text(content)
        return {"completed": True, "exit_code": 0}, output


class ReleaseSelectionTests(unittest.TestCase):
    def test_multi_image_hosted_protocol_matches_compiled_protocol(self):
        image = {"Config": {"Labels": {"io.rtdb.hosted-protocol": "8"}}}
        release.verify_hosted_protocol(image, {"hosted_protocol": 8})
        for invalid in [{}, {"hosted_protocol": 7}, {"hosted_protocol": True}]:
            with self.assertRaises(ValueError):
                release.verify_hosted_protocol(image, invalid)
        with self.assertRaises(ValueError):
            release.verify_hosted_protocol({}, {"hosted_protocol": 8})

    def test_multi_image_attests_only_compiled_hosted_features(self):
        hosted = {"api": "rtdb.store.v1", "features": ["store.scoped-grants"]}
        compiled = {"hosted_protocol": 227, "capabilities": hosted}
        labels = {"io.rtdb.hosted-protocol": "227", "io.rtdb.capabilities": json.dumps(hosted)}
        image = {"Config": {"Labels": labels}}
        release.verify_hosted_protocol(image, compiled)
        for attestation in [None, "not json", json.dumps({"api": "rtdb.store.v1", "features": []})]:
            labels["io.rtdb.capabilities"] = attestation
            with self.assertRaises(ValueError):
                release.verify_hosted_protocol(image, compiled)

    def test_base_images_require_content_digests(self):
        pinned = "registry.example/runtime:bookworm@sha256:" + "a" * 64
        self.assertEqual(immutable_image(pinned), pinned)
        for value in ["rust:1.94.1-bookworm", "sha256:" + "a" * 64, "runtime@sha256:short", "runtime @sha256:" + "a" * 64]:
            with self.assertRaises(ValueError):
                immutable_image(value)

    def test_matrix_rejects_unadvertised_platform_or_feature_variant(self):
        matrix = json.loads((HERE / "matrix.json").read_bytes())
        self.assertEqual(selected_features(matrix, "local", "linux/amd64"), [])
        self.assertIn("rtdb-server/all-backends", selected_features(matrix, "all-backends", "linux/arm64"))
        self.assertEqual(selected_features(matrix, "multi", "linux/arm64"), ["rtdb-multi/dynamodb", "rtdb-multi/s3-key-vault"])
        for variant, platform in [("unknown", "linux/amd64"), ("local", "linux/mips")]:
            with self.assertRaises(ValueError):
                selected_features(matrix, variant, platform)

    def test_jvm_recipe_replaces_only_the_known_runtime_base(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "Dockerfile"
            original = "ARG BUILDER_IMAGE\nFROM ${BUILDER_IMAGE} AS jlink\nFROM alpine:3.23.7\nCOPY server.jar /server.jar\n"
            path.write_text(original)
            base = "alpine@sha256:" + "b" * 64
            self.assertEqual(jvm_recipe(path, base), original.replace("FROM alpine:3.23.7", "FROM " + base))
            self.assertEqual(path.read_text(), original)
            path.write_text(original.replace("alpine:3.23.7", "alpine:3.24.0"))
            with self.assertRaises(ValueError):
                jvm_recipe(path, base)

    def test_every_native_server_selection_has_the_matching_config_migrator_features(self):
        matrix = json.loads((HERE / "matrix.json").read_bytes())
        for variant in matrix["variants"]:
            features = selected_features(matrix, variant, "linux/amd64")
            server = {feature.split("/", 1)[1] for feature in features if feature.startswith("rtdb-server/")}
            migrator = {feature.split("/", 1)[1] for feature in features if feature.startswith("rtdb-config/")}
            self.assertEqual(server, migrator)
        matrix["variants"]["kafka"] = ["rtdb-server/kafka"]
        with self.assertRaisesRegex(ValueError, "must match"):
            selected_features(matrix, "kafka", "linux/amd64")

    def test_native_artifact_extraction_retains_the_config_migrator_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = extract_artifacts(ExtractionCommands(root), {"Id": "image"}, "rust", root)
            migrator = next(value for value in artifacts if value["path"] == "artifacts/rtdb-config")
            self.assertEqual(migrator["sha256"], release.digest(root / migrator["path"]))
            self.assertIn("artifacts/tzdb.dat", {value["path"] for value in artifacts})
            self.assertIn("artifacts/rtdb-build-info.json", {value["path"] for value in artifacts})
            self.assertIn("artifacts/rtdb-config-build-info.json", {value["path"] for value in artifacts})

    def test_multi_packaging_extracts_its_executable_and_compiled_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = extract_artifacts(ExtractionCommands(root), {"Id": "image"}, "rust", root, "multi")
            self.assertEqual({value["path"] for value in artifacts}, {
                "artifacts/build.json", "artifacts/rtdb-multi",
                "artifacts/rtdb-multi-build-info.json", "artifacts/tzdb.dat",
            })

    def test_tzdb_override_is_copied_and_bound_independently_of_later_source_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = root / "context"
            context.mkdir()
            source = root / "selected.dat"
            source.write_bytes(b"original database")
            selected = release.retain_tzdb(context, source)
            source.write_bytes(b"changed database")
            self.assertEqual((context / "release-tzdb.dat").read_bytes(), b"original database")
            self.assertEqual(selected, {"sha256": release.digest(context / "release-tzdb.dat"), "bytes": 17, "selection": "override"})
            source.write_bytes(b"x" * (1024 * 1024 + 1))
            with self.assertRaisesRegex(ValueError, "at most one MiB"):
                release.retain_tzdb(context, source)

    def test_compiled_executable_reports_must_match_selected_tzdb_and_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tzdb.dat").write_bytes(b"selected database")
            metadata = {"revision": "a" * 40, "version": "0.1.0", "tzdb": {"sha256": release.digest(root / "tzdb.dat"), "bytes": 17}}
            report = {"schema_version": 1, "engine": "rust", "version": "0.1.0", "revision": "a" * 40, "tzdb_version": "2026e", "tzdb_sha256": metadata["tzdb"]["sha256"]}
            for executable in ["rtdb", "rtdb-config"]:
                (root / (executable + "-build-info.json")).write_text(json.dumps(report))
            self.assertEqual(release.verify_native_build_info({"artifacts/" + path.name: path for path in root.iterdir()}, metadata), report)
            for field, value in [("revision", "b" * 40), ("tzdb_sha256", "b" * 64), ("tzdb_version", "")]:
                (root / "rtdb-config-build-info.json").write_text(json.dumps({**report, field: value}))
                with self.assertRaises(ValueError):
                    release.verify_native_build_info({"artifacts/" + path.name: path for path in root.iterdir()}, metadata)
            (root / "rtdb-config-build-info.json").write_text(json.dumps(report))
            (root / "tzdb.dat").write_bytes(b"altered database")
            with self.assertRaisesRegex(ValueError, "packaged TZDB bytes"):
                release.verify_native_build_info({"artifacts/" + path.name: path for path in root.iterdir()}, metadata)

    def test_failed_compiled_metadata_probe_still_drains_owned_extraction_resources(self):
        class FailedProbe(ExtractionCommands):
            def run(self, name, argv, required=True, timeout=None):
                if name == "artifact-build-info":
                    raise RuntimeError("probe failed")
                return super().run(name, argv, required, timeout)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = FailedProbe(root)
            with self.assertRaisesRegex(RuntimeError, "probe failed"):
                extract_artifacts(commands, {"Id": "image"}, "rust", root)
            self.assertFalse(commands.container)
            self.assertFalse(commands.volumes)

    def test_jvm_variants_match_pinned_shadow_jar_projects_and_actual_backends(self):
        matrix = json.loads((HERE / "matrix.json").read_bytes())
        expected = {"standalone": {"local", "pg-source"},
                    "aws": {"local", "kafka", "kafka-connect", "s3", "pg-source"},
                    "azure": {"local", "kafka", "kafka-connect", "azure", "pg-source"},
                    "google-cloud": {"local", "kafka", "kafka-connect", "gcs", "pg-source"}}
        for variant, capabilities in expected.items():
            selected = selected_jvm_variant(matrix, variant, "linux/amd64")
            self.assertEqual(selected["gradle_task"], f":docker:{variant}:shadowJar")
            self.assertEqual(selected["docker_variant"], variant)
            self.assertEqual(set(selected["capabilities"]), capabilities)
        with self.assertRaises(ValueError):
            selected_jvm_variant(matrix, "all-backends", "linux/amd64")

    def test_driver_input_changes_in_a_separate_checkout_invalidate_packaging(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "matrix.json"
            source.write_text("original")
            state = {"revision": "driver-revision", "dirty": ""}
            def git(repository, *args):
                self.assertEqual(repository, root)
                return state["dirty"] if args[0] == "status" else "tree" if args[-1] == "HEAD^{tree}" else state["revision"]
            with mock.patch.object(release, "git", side_effect=git):
                captured = capture_driver_inputs(root, [source])
                verify_driver_inputs(root, captured)
                source.write_text("changed")
                with self.assertRaisesRegex(ValueError, "input changed"):
                    verify_driver_inputs(root, captured)
                source.write_text("original")
                state["revision"] = "new-driver-revision"
                with self.assertRaisesRegex(ValueError, "checkout changed"):
                    verify_driver_inputs(root, captured)

    def test_extraction_removes_and_verifies_anonymous_volumes_after_the_container(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = ExtractionCommands(root)
            artifacts = extract_artifacts(commands, {"Id": "image"}, "jvm", root)
            self.assertEqual([value["path"] for value in artifacts], ["artifacts/xtdb.jar"])
            self.assertFalse(commands.container)
            self.assertFalse(commands.volumes)
            actions = [name for name, _ in commands.calls]
            self.assertLess(actions.index("remove-artifact-container"), actions.index("remove-artifact-volume-0"))
            lifecycle = json.loads((root / "extraction-lifecycle.json").read_bytes())
            self.assertTrue(lifecycle["drained"])
            self.assertEqual(lifecycle["volumes"], ["anonymous-volume"])

    def test_failed_extraction_preserves_primary_and_volume_cleanup_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = ExtractionCommands(root, fail_copy=True, fail_volume_remove=True)
            with self.assertRaises(BaseExceptionGroup) as caught:
                extract_artifacts(commands, {"Id": "image"}, "jvm", root)
            self.assertEqual(str(caught.exception.exceptions[0]), "copy failed")
            self.assertTrue(any(str(error) == "volume remove failed" for error in caught.exception.exceptions))
            self.assertFalse(commands.container)
            self.assertTrue(commands.volumes)
            lifecycle = json.loads((root / "extraction-lifecycle.json").read_bytes())
            self.assertFalse(lifecycle["drained"])
            self.assertEqual(lifecycle["primary_error"], "copy failed")
            self.assertTrue(any(name.startswith("check-artifact-volume-") for name, _ in commands.calls))

    def test_lost_create_acknowledgement_still_captures_and_removes_owned_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = ExtractionCommands(root, fail_create_ack=True)
            with self.assertRaisesRegex(RuntimeError, "create acknowledgement lost"):
                extract_artifacts(commands, {"Id": "image"}, "jvm", root)
            self.assertFalse(commands.container)
            self.assertFalse(commands.volumes)

    def test_extraction_does_not_remove_a_container_whose_ownership_cannot_be_verified(self):
        class OtherOwner(ExtractionCommands):
            def run(self, name, argv, required=True, timeout=None):
                status, output = super().run(name, argv, required, timeout)
                if name == "inspect-artifact-container":
                    record = json.loads(output.read_bytes())
                    record[0]["Config"]["Labels"]["io.rtdb.release"] = "different-owner"
                    output.write_text(json.dumps(record))
                return status, output
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = OtherOwner(root)
            with self.assertRaises(BaseExceptionGroup):
                extract_artifacts(commands, {"Id": "image"}, "jvm", root)
            self.assertTrue(commands.container)
            self.assertTrue(commands.volumes)
            self.assertFalse(any(name.startswith("remove-artifact") for name, _ in commands.calls))



class DefaultImageConfigurationTests(unittest.TestCase):
    def test_the_earlier_data_path_configuration_differs_from_the_default_only_in_its_paths(self):
        default = (HERE / "default.yaml").read_text()
        earlier = (HERE / "default-var-lib-rtdb.yaml").read_text()
        self.assertIn("/var/lib/xtdb/log", default)
        self.assertIn("/var/lib/xtdb/buffers", default)
        self.assertEqual(default.replace("/var/lib/xtdb/buffers", "/var/lib/rtdb/storage").replace("/var/lib/xtdb/", "/var/lib/rtdb/"), earlier)

    def test_the_image_declares_upstreams_volume_and_serves_through_the_path_choosing_entrypoint(self):
        dockerfile = (HERE / "Dockerfile").read_text()
        self.assertIn("VOLUME /var/lib/xtdb\n", dockerfile)
        self.assertNotIn("VOLUME /var/lib/rtdb", dockerfile)
        self.assertIn('ENTRYPOINT ["/usr/local/bin/rtdb-entrypoint"]', dockerfile)
        entrypoint = (HERE / "entrypoint.sh").read_text()
        self.assertIn("/etc/rtdb/config-var-lib-rtdb.yaml", entrypoint)
        self.assertTrue(entrypoint.rstrip().endswith('exec /usr/local/bin/rtdb "$@"'))


if __name__ == "__main__":
    unittest.main()
