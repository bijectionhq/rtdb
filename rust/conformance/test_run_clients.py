import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from run_clients import CLIENTS, LANES, compose_manifest, validate_package, validate_package_files


class PackagedClientTests(unittest.TestCase):
    def package(self):
        image = {"Id": "sha256:" + "1" * 64, "Config": {"Labels": {"org.opencontainers.image.revision": "2" * 40,
                 "org.opencontainers.image.source": "https://github.com/bijectionhq/rtdb", "io.rtdb.source-dirty": "false", "io.rtdb.test-hooks": "false"}}}
        manifest = {"schema_version": 1, "artifact_kind": "server_image", "revision": "2" * 40,
                    "source_dirty": False, "image_id": image["Id"], "engine": "rust", "test_hooks": False,
                    "build_command": ["docker", "build"], "source_manifest_sha256": "3" * 64}
        return image, manifest

    def package_files(self, root, manifest):
        sources = json.dumps({"revision": manifest["revision"]}).encode()
        (root / "source-manifest.json").write_bytes(sources)
        (root / "artifacts").mkdir()
        timezone = b"selected database"
        manifest.update(source_manifest_sha256=hashlib.sha256(sources).hexdigest(), version="0.1.0", features=[], platform="linux/amd64",
                        tzdb={"sha256": hashlib.sha256(timezone).hexdigest(), "bytes": len(timezone), "selection": "bundled"})
        report = {"schema_version": 1, "engine": "rust", "version": manifest["version"], "revision": manifest["revision"],
                  "tzdb_version": "2026e", "tzdb_sha256": manifest["tzdb"]["sha256"]}
        (root / "artifacts/build.json").write_text(json.dumps(manifest))
        (root / "artifacts/tzdb.dat").write_bytes(timezone)
        for name in ["rtdb", "rtdb-config"]:
            (root / "artifacts" / name).write_bytes(name.encode())
            (root / "artifacts" / (name + "-build-info.json")).write_text(json.dumps(report))
        manifest["compiled_build_info"] = report
        manifest["artifacts"] = [{"path": str(path.relative_to(root)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
                                 for path in sorted((root / "artifacts").iterdir())]

    def test_package_requires_matching_clean_source_and_actual_image(self):
        image, manifest = self.package()
        validate_package(image, manifest, "2" * 40, "rust")
        for key, value in [("source_dirty", True), ("image_id", "sha256:" + "4" * 64), ("revision", "5" * 40),
                           ("test_hooks", True), ("engine", "jvm"), ("build_command", "docker build"),
                           ("source_manifest_sha256", "not-a-digest")]:
            altered = copy.deepcopy(manifest)
            altered[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_package(image, altered, "2" * 40, "rust")

    def test_arbitrary_manifest_cannot_relabel_a_reference_image_as_native(self):
        image, manifest = self.package()
        image["Config"]["Labels"]["org.opencontainers.image.source"] = "https://github.com/xtdb/xtdb"
        with self.assertRaises(ValueError):
            validate_package(image, manifest, "2" * 40, "rust")

    def test_package_evidence_files_must_match_retained_digests(self):
        _, manifest = self.package()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.package_files(root, manifest)
            artifact = root / "artifacts/build.json"
            self.assertEqual(validate_package_files(root / "package.json", manifest), manifest["artifacts"][0])
            artifact.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                validate_package_files(root / "package.json", manifest)
            manifest["artifacts"][0]["path"] = "../outside"
            with self.assertRaises(ValueError):
                validate_package_files(root / "package.json", manifest)

    def test_native_package_requires_both_executable_reports_and_selected_timezone_bytes(self):
        _, manifest = self.package()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.package_files(root, manifest)
            for artifact in manifest["artifacts"]:
                missing = copy.deepcopy(manifest)
                missing["artifacts"] = [item for item in missing["artifacts"] if item["path"] != artifact["path"]]
                with self.subTest(missing=artifact["path"]), self.assertRaises(ValueError):
                    validate_package_files(root / "package.json", missing)
            changed = copy.deepcopy(manifest)
            changed["compiled_build_info"]["tzdb_version"] = "wrong"
            with self.assertRaisesRegex(ValueError, "compiled executable reports"):
                validate_package_files(root / "package.json", changed)
            changed = copy.deepcopy(manifest)
            changed["tzdb"]["sha256"] = "f" * 64
            with self.assertRaisesRegex(ValueError, "metadata differs at tzdb"):
                validate_package_files(root / "package.json", changed)

    def test_refreshed_artifact_digest_cannot_hide_disagreeing_executable_build_information(self):
        _, manifest = self.package()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.package_files(root, manifest)
            report = root / "artifacts/rtdb-config-build-info.json"
            report.write_text(json.dumps({**manifest["compiled_build_info"], "revision": "a" * 40}))
            record = next(item for item in manifest["artifacts"] if item["path"] == "artifacts/rtdb-config-build-info.json")
            record.update(sha256=hashlib.sha256(report.read_bytes()).hexdigest(), bytes=report.stat().st_size)
            with self.assertRaisesRegex(ValueError, "compiled metadata differs"):
                validate_package_files(root / "package.json", manifest)

    def test_runtime_services_pin_shared_client_images_without_host_ports_or_builds(self):
        images = {service: {"Id": "sha256:" + str(index) * 64} for index, service in enumerate(CLIENTS)}
        manifest = compose_manifest(Path("/reference"), "isolated", "sha256:" + "9" * 64, Path("/evidence"), images)
        for service, value in manifest["services"].items():
            self.assertNotIn("build", value)
            self.assertNotIn("ports", value)
            self.assertEqual(value["pull_policy"], "never")
            if service != "xtdb":
                self.assertEqual(value["image"], images[service]["Id"])
                self.assertEqual(value["environment"]["PG_HOST"], "xtdb")
        self.assertEqual([lane["lane"] for lane in LANES], ["js", "python", "elixir", "rust-pg", "rust-flight", "dotnet"])


if __name__ == "__main__":
    unittest.main()
