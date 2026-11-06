defmodule Player.BFS.Search do
  @moduledoc """
  Level-synchronous BFS with monitored, replaceable fetch workers.

  Each level gets up to 64 workers by default; remaining articles are queued.
  Set `:player, :bfs_max_concurrency` to tune the limit. Newly discovered
  URLs are held behind a depth barrier; a parent map both deduplicates discovery
  and reconstructs the shortest path. Status lives in ETS so a large merge cannot
  block the harness's progress polls.
  """
  use GenServer, restart: :temporary
  require Logger
  alias Player.BFS.{Page, Progress, Worker}

  def start_link({start, target, options}) do
    GenServer.start_link(__MODULE__, {start, target, options})
  end

  @impl true
  def init({start, target, options}) do
    max_concurrency =
      Keyword.get(
        options,
        :max_concurrency,
        Application.get_env(:player, :bfs_max_concurrency, 64)
      )

    if is_integer(max_concurrency) and max_concurrency > 0 do
      init_search(start, target, options, max_concurrency)
    else
      {:stop, :invalid_max_concurrency}
    end
  end

  defp init_search(start, target, options, max_concurrency) do
    Process.flag(:trap_exit, true)

    state = %{
      target: Page.key(target),
      parents: %{Page.key(start) => nil},
      urls: %{Page.key(start) => start},
      queue: :queue.from_list([{start, 0}]),
      queued_count: 1,
      next: [],
      next_count: 0,
      workers: %{},
      active: 0,
      max_concurrency: max_concurrency,
      fetch: Keyword.get(options, :fetch, &Page.fetch/1),
      timeout: Keyword.get(options, :worker_timeout, 5_000),
      retries: Keyword.get(options, :retries, 2),
      snapshot: WikiRace.Status.new(frontier_size: 1)
    }

    Progress.register(self(), state.snapshot)
    send(self(), :start)
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.snapshot, state}

  @impl true
  def handle_info(:start, state) do
    # The referee rejects a one-article path and repeated articles, so equal
    # endpoints have no admissible walk under this competition's rules.
    if Map.has_key?(state.parents, state.target) do
      {:noreply, finish(state, :exhausted)}
    else
      {:noreply, state |> add_worker() |> schedule()}
    end
  end

  def handle_info({:result, pid, token, result}, state) do
    case state.workers[pid] do
      %{job: {url, attempt, ^token, timer}} = worker ->
        Process.cancel_timer(timer)
        state = put_in(state.workers[pid], %{worker | job: nil})
        state = %{state | active: state.active - 1}

        state =
          case result do
            {:ok, links} when is_list(links) -> accept(state, url, links)
            other -> retry(state, url, attempt, other)
          end

        {:noreply, schedule(state)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:timeout, pid, token}, state) do
    case state.workers[pid] do
      %{job: {_, _, ^token, _}} -> Process.exit(pid, :kill)
      _ -> :ok
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.workers, pid) do
      {%{ref: ^ref, job: job}, workers} ->
        state = %{state | workers: workers}

        state =
          case job do
            {url, attempt, _, timer} ->
              Process.cancel_timer(timer)
              state = %{state | active: state.active - 1}
              retry(state, url, attempt, {:worker_exit, reason})

            nil ->
              state
          end

        {:noreply, state |> add_worker() |> schedule()}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: stop_workers(state)

  defp add_worker(state) do
    {pid, ref} = Worker.start(self(), state.fetch)
    put_in(state.workers[pid], %{ref: ref, job: nil})
  end

  defp accept(state, url, links) do
    state = put_in(state.snapshot.visited_count, state.snapshot.visited_count + 1)
    parent = Page.key(url)

    state =
      Enum.reduce(links, state, fn {key, link}, acc ->
        if Map.has_key?(acc.parents, key) do
          acc
        else
          %{
            acc
            | parents: Map.put(acc.parents, key, parent),
              urls: Map.put(acc.urls, key, link),
              next: [link | acc.next],
              next_count: acc.next_count + 1
          }
        end
      end)

    cond do
      Map.has_key?(state.parents, state.target) ->
        path = path(state.target, state, [])
        WikiRace.Harness.report(path)
        state |> put_in([:snapshot, Access.key(:path)], path) |> finish(:found)

      true ->
        state
    end
  end

  defp retry(state, url, attempt, reason) do
    if attempt < state.retries do
      %{
        state
        | queue: :queue.in_r({url, attempt + 1}, state.queue),
          queued_count: state.queued_count + 1
      }
    else
      Logger.warning("BFS skipped #{url} after #{attempt + 1} attempts: #{inspect(reason)}")
      state
    end
  end

  defp schedule(%{snapshot: %{state: status}} = state) when status != :running, do: state

  defp schedule(state) do
    state =
      Enum.reduce_while(state.workers, state, fn {pid, worker}, acc ->
        case {worker.job, :queue.out(acc.queue)} do
          {_, {:empty, _}} ->
            {:halt, acc}

          {nil, {{:value, {url, attempt}}, queue}} ->
            token = make_ref()
            timer = Process.send_after(self(), {:timeout, pid, token}, acc.timeout)
            send(pid, {:fetch, token, url})

            acc = %{
              acc
              | queue: queue,
                queued_count: acc.queued_count - 1,
                active: acc.active + 1
            }

            acc = put_in(acc.workers[pid].job, {url, attempt, token, timer})
            {:cont, put_in(acc.snapshot.current_url, url)}

          _ ->
            {:cont, acc}
        end
      end)

    busy = state.active

    cond do
      busy == 0 and :queue.is_empty(state.queue) and state.next == [] ->
        finish(state, :exhausted)

      busy == 0 and :queue.is_empty(state.queue) ->
        queue = state.next |> Enum.reverse() |> Enum.map(&{&1, 0}) |> :queue.from_list()
        state = resize_workers(state, min(state.next_count, state.max_concurrency))
        schedule(%{state | queue: queue, queued_count: state.next_count, next: [], next_count: 0})

      true ->
        state =
          put_in(state.snapshot.frontier_size, state.queued_count + state.next_count + busy)

        Progress.put(state.snapshot)
        state
    end
  end

  defp path(nil, _state, acc), do: acc
  defp path(key, state, acc), do: path(state.parents[key], state, [state.urls[key] | acc])

  # Called only at a depth barrier, when every existing worker is idle.
  defp resize_workers(state, count) do
    {keep, remove} = state.workers |> Enum.to_list() |> Enum.split(count)
    stop_workers(%{state | workers: Map.new(remove)})
    state = %{state | workers: Map.new(keep)}

    Enum.reduce(List.duplicate(nil, max(count - map_size(state.workers), 0)), state, fn _, acc ->
      add_worker(acc)
    end)
  end

  defp finish(state, status) do
    stop_workers(state)
    if status == :exhausted, do: WikiRace.Harness.report_exhausted()

    state = %{
      state
      | workers: %{},
        active: 0,
        queue: :queue.new(),
        queued_count: 0,
        next: [],
        next_count: 0,
        snapshot: %{state.snapshot | state: status, frontier_size: 0}
    }

    Progress.put(state.snapshot)
    state
  end

  defp stop_workers(state) do
    Enum.each(state.workers, fn {pid, worker} ->
      if worker.job != nil, do: Process.cancel_timer(elem(worker.job, 3))
      Process.demonitor(worker.ref, [:flush])
      Process.exit(pid, :kill)
    end)
  end
end
