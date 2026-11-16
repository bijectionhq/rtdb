import json
import tempfile
import unittest
from pathlib import Path

from evidence import retained
from lane_artifacts import bind_inputs, expected_artifacts, package_matches


class SelectedArtifacts(unittest.TestCase):
    def test_same_revision_does_not_substitute_variant_platform_or_features(self):
        lane = {"platform": "linux/amd64", "selection": {"engine": "rust", "variant": "s3", "features": ["rtdb-server/s3"]}}
        package = {"engine": "rust", "variant": "s3", "platform": "linux/amd64", "features": ["rtdb-server/s3"], "revision": "same"}
        package_matches(package, lane)
        for field, value in (("variant", "local"), ("platform", "linux/arm64"), ("features", []), ("engine", "jvm")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                package_matches({**package, field: value}, lane)

    def test_client_packages_must_equal_the_selected_successful_dependencies(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            native = "linux-amd64/rust/local/package"
            jvm = "linux-amd64/jvm/standalone/package"
            lane = {"runner": "clients", "platform": "linux/amd64", "selection": {"variant": "local", "jvm_variant": "standalone"}, "dependencies": [native, jvm]}
            producers, paths = {}, {}
            for identity, engine, variant in ((native, "rust", "local"), (jvm, "jvm", "standalone")):
                directory = root / engine
                (directory / "run").mkdir(parents=True)
                selected = {"engine": engine, "variant": variant, **({"features": []} if engine == "rust" else {"capabilities": ["local"]})}
                package = {**selected, "platform": "linux/amd64", "image_id": engine}
                path = directory / "run/package.json"
                path.write_text(json.dumps(package))
                producers[identity] = ({"platform": "linux/amd64", "selection": selected}, directory)
                paths["native_package" if engine == "rust" else "jvm_package"] = path
            records = bind_inputs(lane, root, root, paths.__getitem__, producers.__getitem__)
            self.assertEqual({native, jvm}, {value["dependency"] for value in records})
            performance = {**lane, "runner": "performance", "selection": {**lane["selection"], "capabilities": ["local"]}}
            plan = root / "plan.json"
            value = {"resources": {"architecture": "linux/amd64"}, "package_capabilities": ["local"],
                     "engines": {"rust": {"package": retained(root, paths["native_package"])}, "jvm": {"package": retained(root, paths["jvm_package"])}}}
            plan.write_text(json.dumps(value))
            bind_inputs(performance, root, root, lambda _: plan, producers.__getitem__)
            for key, changed in (("package_capabilities", ["s3"]), ("resources", {"architecture": "linux/arm64"})):
                plan.write_text(json.dumps({**value, key: changed}))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    bind_inputs(performance, root, root, lambda _: plan, producers.__getitem__)
            other = root / "other.json"
            other.write_text(json.dumps({"revision": "same", "variant": "local", "image_id": "unrelated-build"}))
            paths["native_package"] = other
            with self.assertRaises(ValueError):
                bind_inputs(lane, root, root, paths.__getitem__, producers.__getitem__)

    def test_native_input_is_the_retained_production_conformance_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "rtdb-conformance"
            binary.write_bytes(b"selected production")
            identity = "linux-amd64/rust/s3/production"
            producer = {"id": identity, "platform": "linux/amd64", "selection": {"variant": "s3", "features": ["rtdb-server/s3"]}}
            build = {"configuration": identity, "selection": producer["selection"], "binaries": [{"retained_artifact": retained(root, binary),
                     "compiler_artifact": {"target": {"name": "rtdb-conformance"}, "profile": {"test": False}}}]}
            (root / "production-artifacts.json").write_text(json.dumps(build))
            lane = {"runner": "history", "platform": "linux/amd64", "selection": {"variant": "s3"}, "dependencies": [identity]}
            self.assertEqual(binary, expected_artifacts(lane, lambda _: (producer, root))["native"][1])
            other = root / "other"
            other.write_bytes(b"same revision other features")
            with self.assertRaises(ValueError):
                bind_inputs(lane, root, root, lambda _: other, lambda _: (producer, root))

if __name__ == "__main__":
    unittest.main()
