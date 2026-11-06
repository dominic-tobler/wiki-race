defmodule Player.RandomCrawler do
  @moduledoc """
  An OTP-based random search crawler for WikiRace.
  Spawns multiple parallel random walkers that explore paths independently
  while sharing progress and reporting successes immediately.
  """

  def start(start_url, target_url) do
    DynamicSupervisor.start_child(
      WikiRace.CrawlerSupervisor,
      {WikiRace.RandomCrawler.Coordinator, {start_url, target_url}}
    )
  end

  def status(pid), do: GenServer.call(pid, :status)
end

defmodule WikiRace.RandomCrawler.Coordinator do
  use GenServer

  @worker_count 100

  defstruct [
    :start_url,
    :target_url,
    :snapshot,
    :fetched_urls,
    workers: []
  ]

  def start_link({start_url, target_url}) do
    GenServer.start_link(__MODULE__, {start_url, target_url})
  end

  @impl true
  def init({start_url, target_url}) do
    state = %__MODULE__{
      start_url: start_url,
      target_url: target_url,
      snapshot: WikiRace.Status.new(),
      fetched_urls: MapSet.new()
    }

    send(self(), :start_workers)
    {:ok, state}
  end


  @impl true
  def handle_info(:start_workers, %{start_url: url, target_url: url} = state) do
    handle_cast({:path_found, [url]}, state)
  end

  def handle_info(:start_workers, state) do
    coordinator_pid = self()

    workers =
      Enum.map(1..@worker_count, fn _ ->
        Task.async(fn ->
          run_random_walk(state.start_url, state.target_url, coordinator_pid)
        end)
      end)

    {:noreply, %{state | workers: workers}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state.snapshot, state}
  end

  @impl true
  def handle_cast({:fetch_started, url}, state) do
    snapshot = %{state.snapshot | current_url: url}
    {:noreply, %{state | snapshot: snapshot}}
  end

  def handle_cast({:article_fetched, url}, state) do
    fetched_urls = MapSet.put(state.fetched_urls, url)
    snapshot = %{state.snapshot | visited_count: MapSet.size(fetched_urls)}
    {:noreply, %{state | snapshot: snapshot, fetched_urls: fetched_urls}}
  end

  def handle_cast({:path_found, path}, state) do
    best_path = state.snapshot.path

    if best_path == [] or length(path) < length(best_path) do
      WikiRace.Harness.report(path)
      snapshot = %{state.snapshot | state: :found, path: path}
      {:noreply, %{state | snapshot: snapshot}}
    else
      {:noreply, state}
    end
  end

  # --- Worker Logic ---

  defp run_random_walk(start_url, target_url, coordinator_pid) do
    walk(
      start_url,
      start_url,
      target_url,
      [start_url],
      MapSet.new([start_url]),
      coordinator_pid
    )
  end

defp walk(current_url, start_url, target_url, path, visited, coordinator_pid) do
  GenServer.cast(coordinator_pid, {:fetch_started, current_url})
  case fetch_and_parse_links(current_url) do
    {:ok, links} ->
      GenServer.cast(coordinator_pid, {:article_fetched, current_url})
      if target_url in links do
        GenServer.cast(coordinator_pid, {:path_found, path ++ [target_url]})
        run_random_walk(start_url, target_url, coordinator_pid)
      else
        case Enum.reject(links, &MapSet.member?(visited, &1)) do
          [] ->
            IO.puts("Dead end reached at: #{current_url} (No unvisited links remaining)")
            run_random_walk(start_url, target_url, coordinator_pid)

          unvisited ->
            next_url = Enum.random(unvisited)

            walk(
              next_url,
              start_url,
              target_url,
              path ++ [next_url],
              MapSet.put(visited, next_url),
              coordinator_pid
            )
        end
      end

    {:error, reason} ->
      IO.puts("Fetch failed for: #{current_url} with reason: #{inspect(reason)}")
      run_random_walk(start_url, target_url, coordinator_pid)
  end
end

  defp fetch_and_parse_links(url) do
    case Req.get(url, receive_timeout: 3000) do
      {:ok, %{status: 200, body: body}} ->
        links = extract_wiki_links(body, url)
        {:ok, links}
      _ ->
        {:error, :fetch_failed}
    end
  end

  defp extract_wiki_links(html, base_url) do
    base = URI.parse(base_url)
    regex = ~r/<a\b[^>]*?\s+href\s*=\s*["']([^"']*)["'][^>]*>/i

    Regex.scan(regex, html, capture: :all_but_first)
    |> Enum.map(fn [href] -> URI.merge(base, href) end)
    |> Enum.filter(fn uri ->
      uri.scheme in ["http", "https"] and
        String.ends_with?(uri.host || "", "wikipedia.org") and
        is_nil(uri.fragment) and
        article_path?(uri.path)
    end)
    |> Enum.map(&URI.to_string/1)
    |> Enum.uniq()
  end

  defp article_path?("/wiki/" <> article) do
    article != "" and not String.contains?(URI.decode(article), ":")
  end

  defp article_path?(_path), do: false
end
