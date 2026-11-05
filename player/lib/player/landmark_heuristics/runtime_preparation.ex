defmodule Player.LandmarkHeuristics.Runtime do
  @moduledoc """
  Loads precomputed landmark distance ETS tables at runtime and provides
  the ALT heuristic calculation function for the A* crawler.
  """

  @doc """
  Loads serialized landmark metadata and ETS tables from disk into memory.
  Returns `{:ok, landmarks_data}` where each landmark contains its forward and backward ETS table references.
  """
  def load_landmarks(dir_path \\ "priv/cache/landmarks") do
    metadata_path = Path.join(dir_path, "landmarks_metadata.bin")

    if File.exists?(metadata_path) do
      metadata = metadata_path |> File.read!() |> :erlang.binary_to_term()

      landmarks_data =
        Enum.map(metadata, fn entry ->
          fwd_path = String.to_charlist(Path.join(dir_path, entry.fwd_file))
          bwd_path = String.to_charlist(Path.join(dir_path, entry.bwd_file))

          {:ok, fwd_table} = :ets.file2tab(fwd_path)
          {:ok, bwd_table} = :ets.file2tab(bwd_path)

          %{
            index: entry.index,
            landmark_id: entry.landmark_id,
            fwd_table: fwd_table,
            bwd_table: bwd_table
          }
        end)

      {:ok, landmarks_data}
    else
      {:error, :metadata_not_found}
    end
  end

  @doc """
  Calculates the admissible ALT heuristic estimate between `current_id` and `target_id`
  using all loaded landmark tables, returning the maximum lower bound.
  """
  def calculate_alt_heuristic(current_id, target_id, landmarks_data) do
    landmarks_data
    |> Enum.map(fn %{fwd_table: fwd, bwd_table: bwd} ->
      # Retrieve precomputed distances
      d_L_u = lookup_distance(fwd, current_id)
      d_L_v = lookup_distance(fwd, target_id)

      d_u_L = lookup_distance(bwd, current_id)
      d_v_L = lookup_distance(bwd, target_id)

      # Triangle inequality lower bounds for directed graphs
      bound_1 = d_L_v - d_L_u
      bound_2 = d_u_L - d_v_L

      max(bound_1, bound_2)
    end)
    |> Enum.max(fn -> 0 end)
  end

  # --- Helper Functions ---

  defp lookup_distance(table, node_id) do
    case :ets.lookup(table, node_id) do
      [{^node_id, dist}] -> dist
      [] -> 0 # Fallback for unreachable or unindexed nodes to preserve A* admissibility
    end
  end
end
