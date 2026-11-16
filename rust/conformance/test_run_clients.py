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
            sources = json.dumps({"revision": manifest["revision"]}).encode()
            (root / "source-manifest.json").write_bytes(sources)
            (root / "artifacts").mkdir()
            artifact = root / "artifacts/build.json"
            artifact.write_bytes(b"{}")
            manifest["source_manifest_sha256"] = hashlib.sha256(sources).hexdigest()
            manifest["artifacts"] = [{"path": "artifacts/build.json", "sha256": hashlib.sha256(b"{}").hexdigest(), "bytes": 2}]
            self.assertEqual(validate_package_files(root / "package.json", manifest), manifest["artifacts"][0])
            artifact.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                validate_package_files(root / "package.json", manifest)
            manifest["artifacts"][0]["path"] = "../outside"
            with self.assertRaises(ValueError):
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
