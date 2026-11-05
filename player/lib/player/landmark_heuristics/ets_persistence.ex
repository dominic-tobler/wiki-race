defmodule Player.LandmarkHeuristics.EtsPersistence do
  @moduledoc """
  Provides disk persistence routines for graph ETS tables and metadata
  using Erlang's built-in `:ets.tab2file` and binary serialization.
  """

  @doc """
  Saves the graph structure (ETS table references and total_nodes metadata) to disk.
  """
  def save_graph_to_disk(graph_data, dir_path \\ "priv/cache") do
    File.mkdir_p!(dir_path)

    # Persist individual ETS tables using :ets.tab2file
    save_table(graph_data.url_to_id, Path.join(dir_path, "url_to_id.etab"))
    save_table(graph_data.id_to_url, Path.join(dir_path, "id_to_url.etab"))
    save_table(graph_data.adjacency, Path.join(dir_path, "adjacency.etab"))

    # Persist scalar metadata (e.g., total_nodes count) as binary data
    metadata = %{total_nodes: graph_data.total_nodes}
    metadata_path = Path.join(dir_path, "metadata.bin")
    File.write!(metadata_path, :erlang.term_to_binary(metadata))

    {:ok, dir_path}
  end

  @doc """
  Loads the graph structure and ETS tables back from disk into memory.
  """
  def load_graph_from_disk(dir_path \\ "priv/cache") do
    url_to_id = load_table(Path.join(dir_path, "url_to_id.etab"))
    id_to_url = load_table(Path.join(dir_path, "id_to_url.etab"))
    adjacency = load_table(Path.join(dir_path, "adjacency.etab"))

    metadata_path = Path.join(dir_path, "metadata.bin")
    metadata = metadata_path |> File.read!() |> :erlang.binary_to_term()

    %{
      url_to_id: url_to_id,
      id_to_url: id_to_url,
      adjacency: adjacency,
      total_nodes: metadata.total_nodes
    }
  end

  # --- Helper Functions ---

  defp save_table(table_ref, file_path) do
    # :ets.tab2file expects a charlist path for compatibility
    charlist_path = String.to_charlist(file_path)

    case :ets.tab2file(table_ref, charlist_path) do
      :ok ->
        :ok
      {:error, reason} ->
        raise "Failed to write ETS table to #{file_path}: #{inspect(reason)}"
    end
  end

  defp load_table(file_path) do
    charlist_path = String.to_charlist(file_path)

    case :ets.file2tab(charlist_path) do
      {:ok, table_ref} ->
        table_ref
      {:error, reason} ->
        raise "Failed to load ETS table from #{file_path}: #{inspect(reason)}"
    end
  end
end
