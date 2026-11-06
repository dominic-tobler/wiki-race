defmodule Player.Crawler do
  @moduledoc """
  The active, supervised concurrent breadth-first crawler.
  """

  @behaviour WikiRace.Crawler

  @impl WikiRace.Crawler
  def start(start_url, target_url) do
    if Player.BFS.Page.valid_pair?(start_url, target_url) do
      DynamicSupervisor.start_child(
        WikiRace.CrawlerSupervisor,
        {Player.BFS.Search, {start_url, target_url, []}}
      )
    else
      {:error, :invalid_kiwix_urls}
    end
  end

  @impl WikiRace.Crawler
  def status(pid), do: Player.BFS.Progress.get(pid)
end
