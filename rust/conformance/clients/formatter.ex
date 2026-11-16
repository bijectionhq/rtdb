defmodule RTDB.ConformanceFormatter do
  use GenServer

  @impl true
  def init(_opts) do
    file = File.open!(System.fetch_env!("RTDB_CLIENT_REPORT"), [:write, :exclusive])
    {:ok, %{file: file, sequence: 0, test_count: 0}}
  end

  @impl true
  def handle_cast({:suite_started, opts}, state) do
    {:noreply, emit(state, %{kind: "suite_started", seed: opts[:seed]})}
  end

  def handle_cast({:test_started, test}, state) do
    {:noreply, emit(state, Map.put(identity(test), :kind, "test_started"))}
  end

  def handle_cast({:test_finished, test}, state) do
    outcome = case test.state do
      nil -> "passed"
      {:failed, _} -> "failed"
      {:skipped, _} -> "skipped"
      {:excluded, _} -> "excluded"
      {:invalid, _} -> "invalid"
    end
    event = Map.merge(identity(test), %{kind: "test_finished", outcome: outcome,
      source: %{file: Path.relative_to_cwd(test.tags.file), line: test.tags.line},
      details: inspect(test.state, limit: :infinity, printable_limit: :infinity)})
    {:noreply, emit(%{state | test_count: state.test_count + 1}, event)}
  end

  def handle_cast({:module_finished, %{state: {:failed, failures}} = test_module}, state) do
    {:noreply, emit(state, %{kind: "module_failed", module: inspect(test_module.name),
      details: inspect(failures, limit: :infinity, printable_limit: :infinity)})}
  end

  def handle_cast({:suite_finished, times}, state) do
    state = emit(state, %{kind: "suite_finished", test_count: state.test_count, times: times})
    :ok = :file.sync(state.file)
    {:noreply, state}
  end

  def handle_cast(:max_failures_reached, state) do
    {:noreply, emit(state, %{kind: "max_failures_reached"})}
  end

  def handle_cast({:sigquit, _}, state) do
    {:noreply, emit(state, %{kind: "sigquit"})}
  end

  def handle_cast(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: File.close(state.file)

  defp identity(test), do: %{suite: inspect(test.module), name: Atom.to_string(test.name)}

  defp emit(state, event) do
    :ok = IO.binwrite(state.file, Jason.encode!(Map.put(event, :sequence, state.sequence)) <> "\n")
    %{state | sequence: state.sequence + 1}
  end
end
