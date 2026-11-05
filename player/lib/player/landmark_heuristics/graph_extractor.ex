defmodule WikiRace.GraphExtractor do
  @moduledoc """
  Extracts and parses Wikipedia graph structures from HTML pages,
  mapping URLs to integer IDs and storing a compact adjacency list in ETS.
  """

  @doc """
  Takes an enumerable of `{url, html_body}` tuples and builds:
  1. Bidirectional ETS mapping tables (`url_to_id` and `id_to_url`).
  2. An adjacency list ETS table mapping integer node IDs to lists of neighbor IDs.
  """
  def build_graph(articles) do
    # Create public ETS tables for fast, concurrent access
    url_to_id = :ets.new(:url_to_id, [:set, :public, :protected])
    id_to_url = :ets.new(:id_to_url, [:set, :public, :protected])
    adjacency = :ets.new(:adjacency, [:set, :public, :protected])

    # Pass 1: Assign sequential integer IDs (0 to N-1) to all unique article URLs
    final_next_id = Enum.reduce(articles, 0, fn {url, _html}, next_id ->
      normalized_url = normalize_url(url)

      case :ets.lookup(url_to_id, normalized_url) do
        [] ->
          :ets.insert(url_to_id, {normalized_url, next_id})
          :ets.insert(id_to_url, {next_id, normalized_url})
          next_id + 1
        _ ->
          next_id
      end
    end)

    # Pass 2: Parse links, map target URLs to integer IDs, and build adjacency representation
    Enum.each(articles, fn {url, html} ->
      source_normalized = normalize_url(url)

      case :ets.lookup(url_to_id, source_normalized) do
        [{^source_normalized, source_id}] ->
          neighbor_ids =
            html
            |> extract_wiki_links(url)
            |> Enum.map(&normalize_url/1)
            |> Enum.flat_map(fn neighbor_url ->
              case :ets.lookup(url_to_id, neighbor_url) do
                [{^neighbor_url, neighbor_id}] -> [neighbor_id]
                [] -> [] # Ignore external links or unindexed stubs
              end
            end)
            |> Enum.uniq()

          :ets.insert(adjacency, {source_id, neighbor_ids})
        [] ->
          :ok
      end
    end)

    %{
      url_to_id: url_to_id,
      id_to_url: id_to_url,
      adjacency: adjacency,
      total_nodes: final_next_id
    }
  end

  # --- Helper Functions ---

  defp normalize_url(url) do
    url
    |> URI.parse()
    |> Map.get(:path, "")
    |> URI.decode()
  end

  defp extract_wiki_links(html, base_url) do
    # Regex matching anchor href attributes
    regex = ~r/<a\s+(?:[^>]*?\s+)?href="([^"]*)"[^>]*>/i

    Regex.scan(regex, html)
    |> Enum.map(fn [_, href] -> URI.merge(base_url, href) |> URI.to_string() end)
    |> Enum.filter(fn u ->
      String.contains?(u, "/wiki/") &&
      !String.contains?(u, "#") &&
      !String.contains?(u, ":")
    end)
  end
end
