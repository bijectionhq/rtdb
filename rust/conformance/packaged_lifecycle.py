def validate_stopped(engine, state, output, signal="SIGTERM"):
    """Validate an inspected container after its owner has joined a requested stop.

    - Inputs: the selected engine, Docker State object, retained combined output, and requested signal.
    - Return: None when native success or JVM shutdown-hook completion is evidenced.
    - Errors: ValueError for an active, forced, failed, or unproved shutdown.
    - Preconditions: the caller has awaited Docker stop and captured this container's final state and logs.
    """
    if signal != "SIGTERM" or state.get("Running") is not False or state.get("OOMKilled") is not False or state.get("Restarting") is True or state.get("Dead") is True or state.get("Error"):
        raise ValueError("packaged server has not completed an unforced SIGTERM shutdown")
    code = state.get("ExitCode")
    if engine in {"rust", "provider_observer"} and code == 0:
        return
    if engine == "jvm" and code in {0, 143} and "Node stopped." in output and "forcing exit" not in output:
        return
    raise ValueError("packaged server has no successful owner-drain evidence")
