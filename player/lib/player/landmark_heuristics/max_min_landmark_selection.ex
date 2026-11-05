defmodule Player.LandmarkHeuristics.MaxMinLandmarkSelection do
  @moduledoc """
  Implements the MaxMin (Farthest-Node) algorithm to select optimal landmark nodes
  from a precomputed Wikipedia adjacency ETS table.
  """

  @doc """
  Selects `k` optimal landmarks from the adjacency ETS table.
  Returns a list of integer node IDs representing the landmarks.
  """
  def select_landmarks(adjacency, total_nodes, k \\ 4) do
    # Step 1: Find a valid starting node with outgoing links
    start_node = find_valid_starting_node(adjacency, total_nodes)

    # Step 2: Run initial BFS to find the farthest node -> First Landmark (L1)
    initial_dist_table = bfs(adjacency, start_node)
    l1 = find_farthest_node(initial_dist_table)

    # Initialize tracking ETS table for min distances across all selected landmarks
    min_dist_table = :ets.new(:min_distances, [:set, :public])
    populate_min_distances(min_dist_table, initial_dist_table)

    # Step 3: Iteratively select the remaining k - 1 landmarks
    select_subsequent_landmarks(adjacency, k - 1, [l1], min_dist_table)
  end

  # --- Landmark Selection Loop ---

  defp select_subsequent_landmarks(_adjacency, 0, landmarks, min_dist_table) do
    :ets.delete(min_dist_table)
    Enum.reverse(landmarks)
  end

  defp select_subsequent_landmarks(adjacency, remaining, landmarks, min_dist_table) do
    # Find the node that has the MAXIMUM value among its minimum distances to current landmarks
    {next_landmark, _max_min_dist} =
      :ets.foldl(fn {node, dist}, {max_n, max_d} ->
        if dist > max_d, do: {node, dist}, else: {max_n, max_d}
      end, {-1, -1}, min_dist_table)

    # Run BFS from the newly chosen landmark
    new_dist_table = bfs(adjacency, next_landmark)

    # Update min_dist_table: keep the minimum distance to any landmark seen so far
    :ets.foldl(fn {node, new_dist}, _acc ->
      case :ets.lookup(min_dist_table, node) do
        [{^node, old_dist}] ->
          if new_dist < old_dist, do: :ets.insert(min_dist_table, {node, new_dist})
        [] ->
          :ets.insert(min_dist_table, {node, new_dist})
      end
    end, nil, new_dist_table)

    :ets.delete(new_dist_table)

    select_subsequent_landmarks(adjacency, remaining - 1, [next_landmark | landmarks], min_dist_table)
  end

  # --- Breadth-First Search (BFS) Traversal ---

  defp bfs(adjacency, source) do
    dist_table = :ets.new(:bfs_dist, [:set, :private])
    :ets.insert(dist_table, {source, 0})
    queue = :queue.in({source, 0}, :queue.new())

    bfs_loop(adjacency, queue, dist_table)
    dist_table
  end

  defp bfs_loop(adjacency, queue, dist_table) do
    case :queue.out(queue) do
      {{:value, {node, dist}}, q2} ->
        neighbors =
          case :ets.lookup(adjacency, node) do
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

        bfs_loop(adjacency, new_q, dist_table)

      {:empty, _} ->
        dist_table
    end
  end

  # --- Helper Utilities ---

  defp find_farthest_node(dist_table) do
    {farthest_node, _max_dist} =
      :ets.foldl(fn {node, dist}, {max_n, max_d} ->
        if dist > max_d, do: {node, dist}, else: {max_n, max_d}
      end, {-1, -1}, dist_table)

    farthest_node
  end

  defp populate_min_distances(min_dist_table, dist_table) do
    :ets.foldl(fn {node, dist}, _acc ->
      :ets.insert(min_dist_table, {node, dist})
    end, nil, dist_table)
    :ets.delete(dist_table)
  end

  defp find_valid_starting_node(adjacency, total_nodes) do
    Enum.find_value(0..(total_nodes - 1), 0, fn id ->
      case :ets.lookup(adjacency, id) do
        [{^id, [_ | _]}] -> id
        _ -> nil
      end
    end)
  end
end
