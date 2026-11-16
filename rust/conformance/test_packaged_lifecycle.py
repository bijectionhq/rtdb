import unittest

from packaged_lifecycle import validate_stopped


class PackagedLifecycleTest(unittest.TestCase):
    def test_jvm_sigterm_requires_completed_shutdown_hook_and_unforced_state(self):
        state = {"Running": False, "OOMKilled": False, "Restarting": False, "Dead": False, "Error": "", "ExitCode": 143}
        validate_stopped("jvm", state, "Stopping node...\nNode stopped.\n")
        for output in ("Stopping node...", "forcing exit\nNode stopped."):
            with self.assertRaises(ValueError):
                validate_stopped("jvm", state, output)
        for changed in ({"Running": True}, {"OOMKilled": True}, {"ExitCode": 137}, {"Error": "failed"}):
            with self.assertRaises(ValueError):
                validate_stopped("jvm", {**state, **changed}, "Node stopped.")
        with self.assertRaises(ValueError):
            validate_stopped("jvm", state, "Node stopped.", "SIGKILL")

    def test_native_zero_and_jvm_sigterm_are_distinct(self):
        state = {"Running": False, "OOMKilled": False, "ExitCode": 0}
        validate_stopped("rust", state, "")
        with self.assertRaises(ValueError):
            validate_stopped("rust", {**state, "ExitCode": 143}, "Node stopped.")
        with self.assertRaises(ValueError):
            validate_stopped("jvm", state, "")
