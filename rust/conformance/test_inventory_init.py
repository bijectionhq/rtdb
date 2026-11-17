import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from runtime_inventory import read_plan


ROOT = Path(__file__).resolve().parents[2]
ADAPTER = ROOT / "rust/conformance"


class InventoryInitIntegrationTests(unittest.TestCase):
    def test_nested_builds_and_modules_without_launcher_keep_one_complete_inventory(self):
        with tempfile.TemporaryDirectory(prefix="rtdb-inventory-") as temporary:
            project = Path(temporary)
            (project / "settings.gradle").write_text('rootProject.name = "inventory-fixture"\ninclude("empty", "spec")\n')
            (project / "buildSrc").mkdir()
            (project / "buildSrc/build.gradle").write_text('plugins { id "java" }\n')
            (project / "empty").mkdir()
            (project / "spec/src/test/java/example").mkdir(parents=True)
            (project / "build.gradle").write_text('''
allprojects {
    apply plugin: "java"
    repositories { mavenCentral() }
    tasks.withType(Test).configureEach { useJUnitPlatform(); systemProperty "rtdb.fixture.property", "retained" }
}
dependencies {
    testImplementation "org.junit.jupiter:junit-jupiter-api:6.1.3"
    testRuntimeOnly "org.junit.jupiter:junit-jupiter-engine:6.1.3"
    testRuntimeOnly "org.junit.platform:junit-platform-launcher:6.1.3"
    testRuntimeOnly "com.fasterxml.jackson.core:jackson-databind:2.21.2"
}
project(":spec") {
    dependencies {
        testImplementation "org.junit.jupiter:junit-jupiter-api:6.1.3"
        testImplementation "org.junit.jupiter:junit-jupiter-params:6.1.3"
        testRuntimeOnly "org.junit.jupiter:junit-jupiter-engine:6.1.3"
    }
}
''')
            (project / "spec/src/test/java/example/InventoryTest.java").write_text('''
package example;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
@org.junit.jupiter.api.DisplayNameGeneration(InventoryTest.RequiresProperty.class)
class InventoryTest {
    public static class RequiresProperty extends org.junit.jupiter.api.DisplayNameGenerator.Standard {
        public RequiresProperty() {
            if (!"retained".equals(System.getProperty("rtdb.fixture.property"))) throw new IllegalStateException("discovery lost the baseline system properties");
        }
    }
    @Test void passes() {}
    @Disabled("retained fixture exclusion") @Test void skipped() {}
    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void parameterized(int value) { org.junit.jupiter.api.Assertions.assertTrue(value > 0); }
}
''')
            evidence = project / "evidence"
            command = [str(ROOT / "gradlew"), "--project-dir", str(project), "--console=plain",
                       "--init-script", str(ADAPTER / "inventory.init.gradle"),
                       f"-Prtdb.adapterRoot={ADAPTER}", f"-Prtdb.inventoryOutput={evidence}",
                       "rtdbDiscoverTests", "test"]
            result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=180)
            self.assertEqual(result.returncode, 0, result.stdout)
            lanes = json.loads((evidence / "requested-lanes.json").read_text())["lanes"]
            self.assertEqual({lane["task"] for lane in lanes}, {":test", ":empty:test", ":spec:test"})
            empty = json.loads((evidence / "_empty-discovery.json").read_text())
            self.assertFalse(any(value["is_test"] for value in empty["descriptors"]))
            plans = [read_plan(path) for path in evidence.glob("**/*.jsonl")]
            self.assertTrue(plans)
            self.assertFalse([error for plan in plans for error in plan["errors"]])
            cases = [case for plan in plans for case in plan["cases"]]
            self.assertEqual(len(cases), 5)
            self.assertEqual(sum(case["outcome"].get("status") == "SUCCESSFUL" for case in cases), 4)
            skipped = [case for case in cases if case["outcome"]["event"] == "execution_skipped"]
            self.assertEqual(len(skipped), 1)
            self.assertEqual(skipped[0]["outcome"]["reason"], "retained fixture exclusion")
            self.assertEqual(len({case["descriptor"]["unique_id"] for case in cases}), 5)


if __name__ == "__main__":
    unittest.main()
