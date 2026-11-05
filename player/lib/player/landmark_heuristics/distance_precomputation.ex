defmodule Player.LandmarkHeuristics.DistancePrecomputation do
  @moduledoc """
  Computes forward and backward shortest-path distance ETS tables for selected landmarks
  and serializes them to disk for runtime ALT heuristic evaluation.
  """

  @doc """
  Computes forward and backward distance tables for each landmark and saves them to disk.
  """
  def compute_and_serialize_distances(adjacency, landmarks, total_nodes, dir_path \\ "priv/cache/landmarks") do
    File.mkdir_p!(dir_path)

    # Step 1: Build reverse adjacency table for backward BFS
    reverse_adjacency = build_reverse_adjacency(adjacency, total_nodes)

    # Step 2: Compute and serialize distance tables for each landmark
    landmark_metadata =
      landmarks
      |> Enum.with_index()
      |> Enum.map(fn {landmark_id, index} ->
        # Forward distances: From landmark L to all other nodes (using outgoing edges)
        fwd_table = compute_bfs(adjacency, landmark_id)

        # Backward distances: From all other nodes to landmark L (using incoming edges / reverse graph)
        bwd_table = compute_bfs(reverse_adjacency, landmark_id)

        # File paths for serialization
        fwd_filename = "landmark_#{index}_fwd.etab"
        bwd_filename = "landmark_#{index}_bwd.etab"
        fwd_path = Path.join(dir_path, fwd_filename)
        bwd_path = Path.join(dir_path, bwd_filename)

        # Save ETS tables to disk using :ets.tab2file
        save_etab(fwd_table, fwd_path)
        save_etab(bwd_table, bwd_path)

        # Free in-memory ETS tables
        :ets.delete(fwd_table)
        :ets.delete(bwd_table)

        %{
          index: index,
          landmark_id: landmark_id,
          fwd_file: fwd_filename,
          bwd_file: bwd_filename
        }
      end)

    # Clean up reverse adjacency table
    :ets.delete(reverse_adjacency)

    # Step 3: Serialize landmark metadata index
    metadata_path = Path.join(dir_path, "landmarks_metadata.bin")
    File.write!(metadata_path, :erlang.term_to_binary(landmark_metadata))

    {:ok, dir_path}
  end

  # --- Reverse Adjacency Builder ---

  defp build_reverse_adjacency(adjacency, total_nodes) do
    rev_adj = :ets.new(:reverse_adjacency, [:set, :public])

    # Initialize empty lists for all nodes
    Enum.each(0..(total_nodes - 1), fn id ->
      :ets.insert(rev_adj, {id, []})
    end)

    # Populate reverse edges
    :ets.foldl(fn {u, neighbors}, _acc ->
      Enum.each(neighbors, fn v ->
        case :ets.lookup(rev_adj, v) do
          [{^v, existing}] ->
            :ets.insert(rev_adj, {v, [u | existing]})
          [] ->
            :ets.insert(rev_adj, {v, [u]})
        end
      end)
    end, nil, adjacency)

    rev_adj
  end

  # --- Breadth-First Search (BFS) Traversal ---

  defp compute_bfs(adj_table, source_id) do
    dist_table = :ets.new(:bfs_distances, [:set, :private])
    :ets.insert(dist_table, {source_id, 0})
    queue = :queue.in({source_id, 0}, :queue.new())

    bfs_loop(adj_table, queue, dist_table)
    dist_table
  end

  defp bfs_loop(adj_table, queue, dist_table) do
    case :queue.out(queue) do
      {{:value, {node, dist}}, q2} ->
        neighbors =
          case :ets.lookup(adj_table, node) do
            [{^node, list}] -> list
            [] -> []
          end

        new_q =
          Enum.reduce(neighbors, q2, fn neighbor, acc_q ->
            case :ets.lookup(dist_table, neighbor) do
              [] ->
                :ets.insert(dist_table, {neighbor, dist + 1})
                :queue.in({neighbor, dist + 1}, acc_q)
              _ ->
                acc_q
            end
          end)

        bfs_loop(adj_table, new_q, dist_table)

      {:empty, _} ->
        dist_table
    end
  end

  # --- Helper Utilities ---

  defp save_etab(table_ref, file_path) do
    charlist_path = String.to_charlist(file_path)
    case :ets.tab2file(table_ref, charlist_path) do
      :ok -> :ok
      {:error, reason} -> raise "Failed to serialize ETS table to #{file_path}: #{inspect(reason)}"
    end
  end
end
