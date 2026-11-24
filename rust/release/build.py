#!/usr/bin/env python3
"""Package a clean revision and retain source, build, image, and extracted artifact digests."""

import argparse
import json
import re
import shutil
import sys
import tarfile
import tomllib
import uuid
from pathlib import Path


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "conformance"))
from source_gate import require_phase_two
from run_controls import REFERENCE, REPOSITORY, digest, git, owned_cancellation, write_json
from run_clients import Commands, image_record, verify_native_build_info


RUST_SOURCE = ["rust", "core/src/main/proto", "api/src/main/proto", "modules/aws/src/main/proto",
               "modules/azure/src/main/proto", "modules/google-cloud/src/main/proto", "modules/kafka/src/main/proto",
               "modules/postgres-source/src/main/proto", "LICENSE"]


def immutable_image(value):
    if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", value):
        raise ValueError("base images must include an immutable repository@sha256 digest")
    return value


def selected_features(matrix, variant, platform):
    if matrix.get("schema_version") != 1 or platform not in matrix["platforms"]:
        raise ValueError("release selection is absent from the source matrix")
    if variant == "multi":
        return ["rtdb-multi/dynamodb", "rtdb-multi/s3-key-vault"]
    if variant not in matrix["variants"]:
        raise ValueError("release selection is absent from the source matrix")
    features = matrix["variants"][variant]
    if any(not re.fullmatch(r"rtdb-(server|config|ffi)/[a-z0-9-]+", feature) for feature in features):
        raise ValueError("release feature selection is invalid")
    server = {feature.split("/", 1)[1] for feature in features if feature.startswith("rtdb-server/")}
    migrator = {feature.split("/", 1)[1] for feature in features if feature.startswith("rtdb-config/")}
    if server != migrator:
        raise ValueError("server and configuration migrator feature selections must match")
    return features


def selected_jvm_variant(matrix, variant, platform):
    if matrix.get("schema_version") != 1 or platform not in matrix["platforms"] or variant not in matrix.get("jvm_variants", {}):
        raise ValueError("JVM release selection is absent from the source matrix")
    selection = matrix["jvm_variants"][variant]
    if variant not in {"standalone", "aws", "azure", "google-cloud"} or selection.get("docker_variant") != variant or selection.get("gradle_task") != f":docker:{variant}:shadowJar":
        raise ValueError("JVM release selection does not name a pinned packaging task")
    if not selection.get("capabilities") or len(set(selection["capabilities"])) != len(selection["capabilities"]):
        raise ValueError("JVM release capabilities are missing or duplicated")
    return selection


def capture_driver_inputs(repository, paths):
    return {"revision": git(repository, "rev-parse", "HEAD"),
            "git_tree": git(repository, "rev-parse", "HEAD^{tree}"),
            "files": [{"path": str(path.relative_to(repository)), "sha256": digest(path)} for path in paths]}


def verify_driver_inputs(repository, captured):
    if git(repository, "rev-parse", "HEAD") != captured["revision"] or git(repository, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("release driver checkout changed during packaging")
    for item in captured["files"]:
        if digest(repository / item["path"]) != item["sha256"]:
            raise ValueError(f"release driver input changed during packaging: {item['path']}")


def source_snapshot(commands, repository, revision, paths, directory):
    _, archive = commands.run("source-archive", ["git", "-C", str(repository), "archive", "--format=tar", revision, "--", *paths])
    directory.mkdir()
    with tarfile.open(archive, "r:") as source:
        source.extractall(directory, filter="data")
    files = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            files.append({"path": str(path.relative_to(directory)), "symlink": str(path.readlink())})
        elif path.is_file():
            files.append({"path": str(path.relative_to(directory)), "sha256": digest(path), "mode": path.stat().st_mode & 0o777})
    return {"revision": revision, "git_tree": git(repository, "rev-parse", revision + "^{tree}"),
            "archive_selection": paths, "archive_sha256": digest(archive), "files": files}


def jvm_recipe(original, runtime_image):
    source = original.read_text()
    old = "FROM alpine:3.23.7"
    if source.splitlines().count(old) != 1:
        raise ValueError("pinned JVM Dockerfile no longer has the expected runtime base")
    return source.replace(old, "FROM " + immutable_image(runtime_image))


def retain_tzdb(context, override=None):
    source = override if override is not None else context / "rust/crates/types/data/tzdb/2026e.dat"
    if not source.is_file():
        raise ValueError("selected TZDB input must be a regular file")
    with source.open("rb") as stream:
        data = stream.read(1024 * 1024 + 1)
    if not data or len(data) > 1024 * 1024:
        raise ValueError("selected TZDB input must contain at most one MiB")
    selected = context / "release-tzdb.dat"
    selected.write_bytes(data)
    return {"sha256": digest(selected), "bytes": len(data), "selection": "override" if override is not None else "bundled"}


def native_executables(variant):
    return ("rtdb-multi",) if variant == "multi" else ("rtdb", "rtdb-config")


def verify_hosted_protocol(image, compiled):
    protocol = compiled.get("hosted_protocol")
    labels = image.get("Config", {}).get("Labels", {})
    label = labels.get("io.rtdb.hosted-protocol")
    if type(protocol) is not int or protocol < 1 or label != str(protocol):
        raise ValueError("packaged multi image hosted protocol differs from its executable")
    arrow = compiled.get("pgwire_arrow_copy_out")
    arrow_label = labels.get("io.rtdb.pgwire-arrow-copy-out")
    if (arrow is not None or arrow_label is not None) and (arrow is not True or arrow_label != "true"):
        raise ValueError("packaged multi image Arrow COPY OUT differs from its executable")
    capabilities = compiled.get("capabilities")
    attestation = labels.get("io.rtdb.capabilities")
    if capabilities is not None or attestation is not None:
        try:
            if capabilities is not None and json.loads(attestation) == capabilities:
                return
        except (TypeError, ValueError):
            pass
        raise ValueError("packaged multi image capabilities differ from its executable")


def extract_artifacts(commands, image, engine, output, variant=None):
    name = "rtdb-release-" + uuid.uuid4().hex
    destination = output / "artifacts"
    destination.mkdir()
    if engine == "rust":
        executables = native_executables(variant)
        paths = {executable: f"/usr/local/bin/{executable}" for executable in executables}
        paths.update({"build.json": "/usr/share/rtdb/build.json", "tzdb.dat": "/usr/share/rtdb/tzdb.dat"})
        if variant != "multi":
            paths.update({"librtdb_ffi.so": "/usr/local/lib/librtdb_ffi.so",
                          "librtdb_ffi.a": "/usr/local/lib/librtdb_ffi.a",
                          "rtdb.h": "/usr/local/include/rtdb.h"})
        entrypoint = ["--entrypoint", "/bin/sh", image["Id"], "-ec",
                      "\n".join(f"{paths[executable]} --build-info" for executable in executables)]
    else:
        paths = {"xtdb.jar": "/usr/local/lib/xtdb/xtdb.jar"}
        entrypoint = ["--entrypoint", "/bin/true", image["Id"]]
    volumes, inspected = set(), False
    primary, cleanup_errors = None, []
    def capture_container():
        _, inspection = commands.run("inspect-artifact-container", ["docker", "inspect", name])
        containers = json.loads(inspection.read_bytes())
        if len(containers) != 1 or containers[0].get("Image") != image["Id"] or containers[0].get("Config", {}).get("Labels", {}).get("io.rtdb.release") != name:
            raise ValueError("artifact container does not belong to this release extraction")
        for mount in containers[0].get("Mounts", []):
            if mount.get("Type") == "volume":
                volume = mount.get("Name")
                if not isinstance(volume, str) or not volume:
                    raise ValueError("artifact container has an unnamed volume")
                volumes.add(volume)
        return containers[0]
    with owned_cancellation() as owner:
        try:
            commands.run("artifact-container", ["docker", "create", "--name", name, "--label", "io.rtdb.release=" + name, *entrypoint])
            capture_container()
            inspected = True
            for filename, source in paths.items():
                commands.run("extract-" + filename.replace(".", "-"), ["docker", "cp", name + ":" + source, str(destination / filename)])
            if engine == "rust":
                _, report = commands.run("artifact-build-info", ["docker", "start", "--attach", name])
                state = capture_container().get("State", {})
                if state.get("Running") is not False or state.get("Status") != "exited" or state.get("ExitCode") != 0:
                    raise ValueError("packaged build-information commands did not complete successfully")
                lines = report.read_text().splitlines()
                if len(lines) != len(executables):
                    raise ValueError("packaged executables must each emit one build-information record")
                for executable, line in zip(executables, lines):
                    write_json(destination / (executable + "-build-info.json"), json.loads(line))
        except BaseException as error:
            primary = error
        finally:
            owner.begin_cleanup()
            if not inspected:
                try:
                    capture_container()
                    inspected = True
                except BaseException as error:
                    cleanup_errors.append(error)
            removals = []
            if inspected:
                removals.append(("remove-artifact-container", ["docker", "rm", "--force", name]))
                removals.extend((f"remove-artifact-volume-{index}", ["docker", "volume", "rm", "--force", volume]) for index, volume in enumerate(sorted(volumes)))
            for action, command in removals:
                try:
                    commands.run(action, command, timeout=120)
                except BaseException as error:
                    cleanup_errors.append(error)
            checks = [("check-artifact-container", ["docker", "ps", "--all", "--quiet", "--filter", "label=io.rtdb.release=" + name], None)]
            checks.extend((f"check-artifact-volume-{index}", ["docker", "volume", "ls", "--quiet", "--filter", "name=" + volume], volume) for index, volume in enumerate(sorted(volumes)))
            for action, command, volume in checks:
                try:
                    _, remaining = commands.run(action, command, timeout=30)
                    names = remaining.read_text().splitlines()
                    if (volume is None and names) or volume in names:
                        raise RuntimeError(f"release artifact resource did not drain: {volume or name}")
                except BaseException as error:
                    cleanup_errors.append(error)
            try:
                write_json(output / "extraction-lifecycle.json", {"container": name, "volumes": sorted(volumes),
                    "primary_error": str(primary) if primary else None, "cleanup_errors": [str(error) for error in cleanup_errors],
                    "drained": inspected and not cleanup_errors})
            except BaseException as error:
                cleanup_errors.append(error)
    if primary is not None and cleanup_errors:
        raise BaseExceptionGroup("release extraction and resource cleanup failed", [primary, *cleanup_errors])
    if primary is not None:
        raise primary
    if cleanup_errors:
        raise BaseExceptionGroup("release extraction resource cleanup failed", cleanup_errors)
    return [{"path": str(path.relative_to(output)), "sha256": digest(path), "bytes": path.stat().st_size} for path in sorted(destination.iterdir())]


def build(args):
    require_phase_two(REPOSITORY, args.phase_two)
    immutable_image(args.builder_image)
    immutable_image(args.runtime_image)
    if args.timeout <= 0 or not re.fullmatch(r"\d{8}T\d{6}Z", args.debian_snapshot):
        raise ValueError("release timeout or Debian snapshot timestamp is invalid")
    repository = (args.reference if args.engine == "jvm" else REPOSITORY).resolve()
    revision = git(repository, "rev-parse", "HEAD")
    if args.engine == "jvm" and revision != REFERENCE:
        raise ValueError("JVM package must use the pinned reference revision")
    if git(repository, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("release packaging requires a clean source checkout")
    if git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("release driver requires a clean implementation checkout")
    driver_inputs = capture_driver_inputs(REPOSITORY, [Path(__file__).resolve(), HERE / "matrix.json", HERE / "Dockerfile", HERE / "default.yaml", HERE / "cluster-default.yaml",
        HERE.parent / "conformance/run_controls.py", HERE.parent / "conformance/run_clients.py", HERE.parent / "conformance/client_results.py"])
    input_digests = {item["path"]: item["sha256"] for item in driver_inputs["files"]}
    matrix = json.loads((HERE / "matrix.json").read_bytes())
    jvm = selected_jvm_variant(matrix, args.variant, args.platform) if args.engine == "jvm" else None
    features = selected_features(matrix, args.variant, args.platform) if args.engine == "rust" else []
    if args.engine == "rust" and not (repository / "rust/Cargo.lock").is_file():
        raise ValueError("release packaging requires the committed Phase 2 Cargo.lock")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        with owned_cancellation():
            commands = Commands(output / "commands", args.timeout)
            write_json(output / "driver-inputs.json", driver_inputs)
            commands.run("docker-version", ["docker", "version"])
            commands.run("buildx-version", ["docker", "buildx", "version"])
            source_directory = output / "context"
            paths = RUST_SOURCE if args.engine == "rust" else ["docker", "LICENSE"]
            sources = source_snapshot(commands, repository, revision, paths, source_directory)
            write_json(output / "source-manifest.json", sources)
            source_hash = digest(output / "source-manifest.json")
            epoch = git(repository, "show", "-s", "--format=%ct", revision)
            version = tomllib.loads((REPOSITORY / "rust/Cargo.toml").read_text())["workspace"]["package"]["version"] if args.engine == "rust" else revision[:12]
            metadata = {"schema_version": 1, "engine": args.engine, "revision": revision, "source_dirty": False,
                        "source_manifest_sha256": source_hash, "version": version, "variant": args.variant,
                        "features": features if args.engine == "rust" else [], "platform": args.platform,
                        "builder_image": args.builder_image, "runtime_image": args.runtime_image,
                        "debian_snapshot": args.debian_snapshot if args.engine == "rust" else None,
                        "source_date_epoch": int(epoch), "test_hooks": False}
            if args.engine == "rust":
                metadata["tzdb"] = retain_tzdb(source_directory, getattr(args, "tzdb", None))
            if jvm is not None:
                metadata.update(capabilities=jvm["capabilities"], gradle_task=jvm["gradle_task"], docker_variant=jvm["docker_variant"])
            write_json(source_directory / "release-input.json", metadata)
            image_file = output / "image.id"
            # The build's own tag names the loaded image on either image store; with the containerd store the
            # inspectable image ID is the manifest digest, not the config digest --iidfile records.
            tag = "rtdb-release-build:" + uuid.uuid4().hex
            command = ["docker", "buildx", "build", "--load", "--provenance=false", "--platform", args.platform,
                       "--iidfile", str(image_file), "--tag", tag, "--build-arg", "SOURCE_DATE_EPOCH=" + epoch]
            if args.engine == "rust":
                command += ["--file", str(source_directory / "rust/release/Dockerfile"),
                            "--build-arg", "RUST_IMAGE=" + args.builder_image, "--build-arg", "RUNTIME_IMAGE=" + args.runtime_image,
                            "--build-arg", "DEBIAN_SNAPSHOT=" + args.debian_snapshot,
                            "--build-arg", "RTDB_FEATURES=" + ",".join(features),
                            "--build-arg", "RTDB_REVISION=" + revision, "--build-arg", "RTDB_VERSION=" + version,
                            "--build-arg", "RTDB_VARIANT=" + args.variant]
                if args.variant == "multi":
                    command += ["--target", "multi"]
            else:
                variant = jvm["docker_variant"]
                commands.run("reference-shadow-jar", [repository / "gradlew", "-p", repository, "--no-daemon", "--console=plain", "-PfullJdk", jvm["gradle_task"]])
                jar_path = f"docker/{variant}/build/libs/xtdb-{variant}.jar"
                jar = repository / jar_path
                destination = source_directory / jar_path
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(jar, destination)
                recipe = source_directory / "Dockerfile.reference"
                recipe.write_text(jvm_recipe(source_directory / "docker/Dockerfile", args.runtime_image))
                write_json(output / "reference-jar.json", {"path": str(jar), "sha256": digest(jar), "dockerfile_sha256": digest(recipe)})
                command += ["--file", str(recipe), "--build-arg", "BUILDER_IMAGE=" + args.builder_image,
                            "--build-arg", "VARIANT=" + variant, "--build-arg", "GIT_SHA=" + revision,
                            "--build-arg", "XTDB_VERSION=" + version]
            command.append(str(source_directory))
            commands.run("server-image-build", command)
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_file.read_text().strip()):
                raise ValueError("image build did not emit one immutable image ID")
            image = image_record(commands, tag, "server")
            identifier = image["Id"]
            expected_arch = args.platform.split("/")[1]
            if image["Os"] != "linux" or image["Architecture"] != expected_arch:
                raise ValueError("packaged image architecture differs from requested platform")
            if image.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != revision:
                raise ValueError("packaged image revision label differs from build source")
            artifacts = extract_artifacts(commands, image, args.engine, output, args.variant)
            compiled = None
            if args.engine == "rust":
                compiled = verify_native_build_info(
                    {item["path"]: output / item["path"] for item in artifacts},
                    metadata,
                    native_executables(args.variant),
                )
                if args.variant == "multi":
                    verify_hosted_protocol(image, compiled)
            archive = output / f"rtdb-{args.engine}-{args.variant}-{expected_arch}.docker.tar"
            commands.run("save-server-image", ["docker", "image", "save", "--output", str(archive), image["Id"]])
            if git(repository, "rev-parse", "HEAD") != revision or git(repository, "status", "--porcelain", "--untracked-files=all"):
                raise ValueError("source checkout changed during release packaging")
            verify_driver_inputs(REPOSITORY, driver_inputs)
            manifest = {**metadata, "artifact_kind": "server_image", "image_id": image["Id"], "build_command": command,
                        "image": image, "artifacts": artifacts, "image_archive": {"path": archive.name, "sha256": digest(archive), "bytes": archive.stat().st_size},
                        "build_driver_sha256": input_digests["rust/release/build.py"], "qualification": "built_unqualified",
                        "driver_revision": driver_inputs["revision"], "driver_inputs_sha256": digest(output / "driver-inputs.json"),
                        "compatibility_claim": "none", "release_matrix_sha256": input_digests["rust/release/matrix.json"]}
            if compiled is not None:
                manifest["compiled_build_info"] = compiled
            write_json(output / "package.json", manifest)
            return manifest
    except BaseException as error:
        write_json(output / "failure.json", {"schema_version": 1, "qualification": "failed", "error": str(error)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--engine", choices=["rust", "jvm"], required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--variant", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--builder-image", required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--debian-snapshot", default="20261115T000000Z")
    parser.add_argument("--tzdb", type=Path, help="Native OpenJDK TZDB data to retain and compile instead of the bundled data")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=3600)
    args = parser.parse_args()
    if args.engine == "jvm" and args.reference is None:
        parser.error("--reference is required for a JVM package")
    if args.engine == "jvm" and args.tzdb is not None:
        parser.error("--tzdb selects native compiled data only")
    build(args)


if __name__ == "__main__":
    main()
