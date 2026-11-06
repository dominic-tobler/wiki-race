defmodule Player.JaccardSimilarityCrawler do
  @moduledoc """
  An OTP-based A* Search crawler for WikiRace using Token Jaccard Similarity heuristic.
  """

  def start(start_url, target_url) do
    DynamicSupervisor.start_child(
      WikiRace.CrawlerSupervisor,
      {WikiRace.AStarCrawler.Coordinator, {start_url, target_url}}
    )
  end

  def status(pid), do: GenServer.call(pid, :status)
end

defmodule WikiRace.AStarCrawler.Coordinator do
  use GenServer

  defstruct [
    :start_url,
    :target_url,
    :target_tokens,
    :snapshot,
    :fetched_urls,
    :open_set,
    :closed_set
  ]

  def start_link({start_url, target_url}) do
    GenServer.start_link(__MODULE__, {start_url, target_url})
  end

  @impl true
  def init({start_url, target_url}) do
    target_tokens = tokenize(url_to_title(target_url))

    start_title = url_to_title(start_url)
    h_score = calculate_heuristic(start_title, target_tokens)
    f_score = 0.0 + h_score

    open_set = :gb_trees.empty()
    open_set = :gb_trees.insert({f_score, start_url}, {start_url, 0, [start_url]}, open_set)

    state = %__MODULE__{
      start_url: start_url,
      target_url: target_url,
      target_tokens: target_tokens,
      snapshot: WikiRace.Status.new(frontier_size: :gb_trees.size(open_set)),
      fetched_urls: MapSet.new(),
      open_set: open_set,
      closed_set: MapSet.new()
    }

    send(self(), :step)
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state.snapshot, state}
  end

  @impl true
  def handle_info(:step, %{snapshot: %{state: :found}} = state) do
    {:noreply, state}
  end

  def handle_info(:step, state) do
    if :gb_trees.is_empty(state.open_set) do
      snapshot = %{state.snapshot | state: :exhausted}
      {:noreply, %{state | snapshot: snapshot}}
    else
      {_smallest_key, {current_url, g_score, path}, remaining_open} = :gb_trees.take_smallest(state.open_set)

      updated_state = %{state | open_set: remaining_open, snapshot: %{state.snapshot | frontier_size: :gb_trees.size(remaining_open)}}

      if current_url == state.target_url do
        WikiRace.Harness.report(path)
        snapshot = %{updated_state.snapshot | state: :found, path: path}
        {:noreply, %{updated_state | snapshot: snapshot}}
      else
        if MapSet.member?(updated_state.closed_set, current_url) do
          send(self(), :step)
          {:noreply, updated_state}
        else
          closed_set = MapSet.put(updated_state.closed_set, current_url)

          # Process node exploration (A* specific exploration strategy)
          coordinator_pid = self()

          # Run fetch and expand synchronously inside the GenServer step or asynchronously;
          # here handled directly in step loop with async casts updating state via messages.
          GenServer.cast(coordinator_pid, {:fetch_started, current_url})

          new_state = case fetch_and_parse_links(current_url) do
            {:ok, links} ->
              GenServer.cast(coordinator_pid, {:article_fetched, current_url})

              new_open = Enum.reduce(links, updated_state.open_set, fn link, acc_open ->
                if MapSet.member?(closed_set, link) do
                  acc_open
                else
                  new_g = g_score + 1
                  title = url_to_title(link)
                  h = calculate_heuristic(title, updated_state.target_tokens)
                  f = new_g + h
                  new_path = path ++ [link]

                  :gb_trees.enter({f, link}, {link, new_g, new_path}, acc_open)
                end
              end)

              %{updated_state | open_set: new_open, closed_set: closed_set, snapshot: %{updated_state.snapshot | frontier_size: :gb_trees.size(new_open)}}

            {:error, reason} ->
              IO.puts("Fetch failed for: #{current_url} with reason: #{inspect(reason)}")
              %{updated_state | closed_set: closed_set}
          end

          send(self(), :step)
          {:noreply, new_state}
        end
      end
    end
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

  # --- Heuristic & Helper Functions ---

  defp calculate_heuristic(title, target_tokens) do
    current_tokens = tokenize(title)
    intersection = MapSet.intersection(current_tokens, target_tokens)
    union = MapSet.union(current_tokens, target_tokens)

    jaccard = if MapSet.size(union) == 0, do: 0.0, else: MapSet.size(intersection) / MapSet.size(union)
    (1.0 - jaccard) * 2.0
  end

  defp tokenize(text) do
    text
    |> String.downcase()
    |> String.replace(~r/[^\w\s]/u, "")
    |> String.split()
    |> MapSet.new()
  end

  defp url_to_title(url) do
    url
    |> URI.parse()
    |> Map.get(:path, "")
    |> String.split("/")
    |> List.last()
    |> Kernel.||("")
    |> URI.decode()
    |> String.replace("_", " ")
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
