import hashlib
import itertools
import json
from pathlib import Path

from sql_logic_records import digest


PASSING = {"statement_ok", "statement_expected_error", "query_ok"}
EXCLUDED = {"engine_condition", "query_limit", "script_parse_error", "optimizer_mode_unavailable", "harness_gap"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def json_lines(path, maximum_bytes=64 * 1024 * 1024):
    with Path(path).open("rb") as stream:
        while line := stream.readline(maximum_bytes + 1):
            if len(line) > maximum_bytes:
                raise ValueError("SQL logic observation line exceeds retained byte limit")
            if not line.endswith(b"\n"):
                raise ValueError("incomplete SQL logic observation line")
            yield json.loads(line)


def verify_execution(plan, plan_sha, execution, root):
    if execution.get("schema_version") != 1 or execution.get("execution_complete") is not True or execution.get("plan_sha256") != plan_sha:
        raise ValueError("SQL logic execution is incomplete or uses another plan")
    scripts = execution.get("scripts")
    if not isinstance(scripts, list) or len(scripts) != len(plan["scripts"]):
        raise ValueError("SQL logic execution omitted or added scripts")
    files = []
    for expected, actual in zip(plan["scripts"], scripts):
        if actual.get("source") != expected["source"] or actual.get("selection") != expected["selection"] or actual.get("execution_complete") is not True:
            raise ValueError("SQL logic script identity or completion differs")
        artifact = actual["observations"]
        relative = Path(artifact["path"])
        if relative.is_absolute() or len(relative.parts) != 1:
            raise ValueError("SQL logic observation path escapes its run")
        path = root / relative
        if path.is_symlink() or not path.is_file() or digest(path) != artifact["sha256"]:
            raise ValueError("SQL logic observations differ from retained digest")
        if path in files:
            raise ValueError("SQL logic scripts alias one observation artifact")
        files.append(path)
    if set(root.glob("script-*.jsonl")) != set(files):
        raise ValueError("SQL logic run has unclaimed observation artifacts")
    return files


def check_observation(original, observation):
    if not isinstance(observation, dict) or canonical(observation.get("identity")) != canonical(original["identity"]):
        return "missing, reordered, or altered original record identity"
    disposition = observation.get("disposition")
    if not isinstance(disposition, str):
        return "missing record disposition"
    record = original.get("record")
    if disposition in PASSING or disposition == "query_mismatch":
        if observation.get("executed") is not True or observation.get("engine_executed") is not True:
            return "passing outcome without engine execution"
        if not record:
            return "execution reported for malformed source record"
        if record["type"] == "query":
            result = observation.get("result")
            if not isinstance(result, dict) or not isinstance(result.get("text"), str):
                return "query omitted actual formatted result"
            actual_md5 = hashlib.md5(result["text"].encode("utf-8")).hexdigest()
            if result.get("md5") != actual_md5:
                return "query digest differs from actual result bytes"
            expected = record["expected"]
            matches = expected["md5"] == actual_md5 if "md5" in expected else expected["text"] == result["text"]
            if disposition != ("query_ok" if matches else "query_mismatch"):
                return "query disposition contradicts immutable source expectation"
        elif record["type"] == "statement":
            expected = "statement_ok" if record["mode"] == "ok" else "statement_expected_error"
            if disposition != expected:
                return "statement disposition contradicts immutable source expectation"
            if disposition == "statement_expected_error" and not isinstance(observation.get("error"), dict):
                return "expected exception omitted its actual failure evidence"
        else:
            return "directive was classified as a passing assertion"
    return None


def check_selection(original, observation, script, queries):
    if not isinstance(observation, dict):
        return "missing selection observation"
    record = original.get("record")
    disposition = observation.get("disposition")
    if script.get("parse_errors", 0):
        return None if disposition == "script_parse_error" and observation.get("engine_executed") is False else "malformed script executed despite pinned eager parser"
    limit = script.get("options", {}).get("query_limit")
    if limit is not None and limit == queries:
        return None if disposition == "query_limit" and observation.get("engine_executed") is False else "record executed after pinned query limit"
    if not record:
        return "malformed source record has no script parse failure"
    after = queries + (record["type"] == "query")
    if any(type(observation.get(field)) is not int or observation[field] != expected
           for field, expected in (("query_count_before", queries), ("query_count_after", after))):
        return "record query counter differs from original source control flow"
    skip = "xtdb" in record.get("skipif", []) or record.get("onlyif", "xtdb") != "xtdb"
    if skip and record["type"] in ("query", "statement") and disposition in PASSING:
        return "engine-conditioned record was classified as passing execution"
    return None


def compare(plan, plan_directory, reference_files, native_files, output):
    if len(reference_files) != len(plan["scripts"]) or len(native_files) != len(plan["scripts"]):
        raise ValueError("SQL logic comparison omitted or added script artifacts")
    counts = {"required_assertions": 0, "passing_both": 0, "matching_reference_failure": 0,
              "missing_evidence": 0, "mismatch": 0, "directives": 0, "records": 0}
    dispositions = {"reference": {}, "native": {}}
    with Path(output).open("x", encoding="utf-8") as report:
        for script, reference_path, native_path in zip(plan["scripts"], reference_files, native_files):
            records = json_lines(plan_directory / script["records_file"])
            maximum_bytes = 64 * 1024 * 1024 + 6 * script.get("options", {}).get("max_result_bytes", 128 * 1024 * 1024)
            count = 0
            queries = 0
            assertions = 0
            for original, reference, native in itertools.zip_longest(records, json_lines(reference_path, maximum_bytes), json_lines(native_path, maximum_bytes)):
                if original is None:
                    raise ValueError("engine emitted an observation not in the original source")
                count += 1
                counts["records"] += 1
                errors = [reason for observation in (reference, native) if (reason := check_observation(original, observation))]
                errors.extend(reason for observation in (reference, native) if (reason := check_selection(original, observation, script, queries)))
                outcome = "missing_evidence" if errors else None
                assertion = original["assertion_ordinal"] is not None
                if assertion:
                    counts["required_assertions"] += 1
                    assertions += 1
                else:
                    counts["directives"] += 1
                for side, observation in (("reference", reference), ("native", native)):
                    name = observation.get("disposition", "absent") if isinstance(observation, dict) else "absent"
                    dispositions[side][name] = dispositions[side].get(name, 0) + 1
                if outcome is None:
                    left, right = reference["disposition"], native["disposition"]
                    if left in EXCLUDED or right in EXCLUDED or left.startswith("suppressed_") or right.startswith("suppressed_"):
                        outcome = "missing_evidence"
                    elif left != right:
                        outcome = "mismatch"
                    elif left.startswith("query_") and (reference.get("result") or {}).get("text") != (native.get("result") or {}).get("text"):
                        outcome = "mismatch"
                    elif left in PASSING:
                        outcome = "passing_both"
                    elif left in ("query_mismatch", "statement_expected_error_missing"):
                        outcome = "matching_reference_failure"
                    elif left == "error":
                        outcome = "missing_evidence"
                    elif not assertion and left in ("directive", "halt_error"):
                        outcome = "directive_observed"
                    else:
                        outcome = "missing_evidence"
                    if reference.get("query_count_after") != native.get("query_count_after"):
                        outcome = "mismatch"
                        errors.append("pinned query-limit counter differs")
                if assertion or outcome in ("mismatch", "missing_evidence"):
                    counts[outcome] += 1
                report.write(json.dumps({"identity": original["identity"], "assertion": assertion, "outcome": outcome,
                                         "errors": errors, "reference_disposition": reference.get("disposition") if isinstance(reference, dict) else None,
                                         "native_disposition": native.get("disposition") if isinstance(native, dict) else None}, ensure_ascii=False) + "\n")
                limit = script.get("options", {}).get("query_limit")
                if not script.get("parse_errors", 0) and (limit is None or queries != limit) and (original.get("record") or {}).get("type") == "query":
                    queries += 1
            if count != script["records"]:
                raise ValueError("record stream differs from selected original record count")
            if "assertions" in script and assertions != script["assertions"]:
                raise ValueError("assertion denominator differs from original source records")
    required = counts["required_assertions"]
    return {"schema_version": 1, **counts, "dispositions": dispositions,
            "strict_agreement": counts["passing_both"] / required if required else None,
            "selection_passed": required > 0 and counts["passing_both"] == required and counts["mismatch"] == 0 and counts["missing_evidence"] == 0,
            "complete_parity": False, "assertion_ledger_reconciled": False,
            "record_comparison_sha256": digest(output)}
