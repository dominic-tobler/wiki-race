defmodule Player.LandmarkHeuristics.StepByStepComputation do
  @moduledoc """
  Orchestrates the offline precomputation pipeline for ALT landmarks.

  to run the pipeline, run the following commands (when located in the outer player directory)
    mix compile
    iex -S mix
    Player.LandmarkHeuristics.StepByStepComputation.run_pipeline()
  """

  def run_pipeline() do
    graph_dir = "priv/cache/wiki_graph"
    landmarks_dir = "priv/cache/landmarks"

    kiwix_base_url = "http://localhost:8080"

    # --- Step 1: Build graph from Docker Kiwix container and save to disk ---
    IO.puts("Starting to build graph ")
    max_article_number = 10000000
    articles = Player.LandmarkHeuristics.KiwixCrawler.crawl_kiwix_container(kiwix_base_url, max_article_number)

    graph_data = WikiRace.GraphExtractor.build_graph(articles)
    Player.LandmarkHeuristics.EtsPersistence.save_graph_to_disk(graph_data, graph_dir)
    IO.puts("Graph successfully built and saved. Total nodes: #{graph_data.total_nodes}")

    # --- Step 2: Compute Landmarks using MaxMin algorithm ---
    IO.puts("Selecting optimal landmarks using MaxMin algorithm...")
    reloaded_graph = Player.LandmarkHeuristics.EtsPersistence.load_graph_from_disk(graph_dir)

    number_of_landmarks = 4
    selected_landmarks =
      Player.LandmarkHeuristics.MaxMinLandmarkSelection.select_landmarks(
        reloaded_graph.adjacency,
        reloaded_graph.total_nodes,
        number_of_landmarks
      )

    save_landmarks_to_disk(selected_landmarks, landmarks_dir)
    IO.puts("Selected landmarks saved: #{inspect(selected_landmarks)}")

    # --- Step 3: Compute forward and backward shortest-path distances ---
    IO.puts("Computing forward/backward distance matrices for landmarks...")
    reloaded_adjacency = reloaded_graph.adjacency
    reloaded_landmarks = load_landmarks_from_disk(landmarks_dir)

    # total_nodes is derived dynamically inside or read directly from reloaded graph data
    Player.LandmarkHeuristics.DistancePrecomputation.compute_and_serialize_distances(
      reloaded_adjacency,
      reloaded_landmarks,
      reloaded_graph.total_nodes, # Derived automatically from graph persistence
      landmarks_dir
    )

    IO.puts("ALT Heuristics precomputation pipeline completed successfully!")
  end

  # --- Helper Persistence Functions for Landmarks ---

  defp save_landmarks_to_disk(landmarks, dir_path) do
    File.mkdir_p!(dir_path)
    file_path = Path.join(dir_path, "selected_landmarks.bin")
    File.write!(file_path, :erlang.term_to_binary(landmarks))
  end

  defp load_landmarks_from_disk(dir_path) do
    file_path = Path.join(dir_path, "selected_landmarks.bin")
    file_path |> File.read!() |> :erlang.binary_to_term()
  end

end
