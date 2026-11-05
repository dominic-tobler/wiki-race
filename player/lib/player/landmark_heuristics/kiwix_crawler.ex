defmodule Player.LandmarkHeuristics.KiwixCrawler do
  @moduledoc """
  Crawls pages from a local Docker Kiwix container to build a dataset
  of {url, html_body} tuples for graph extraction.
  """

  @zim_name "wikipedia_en_top_nopic"

  def crawl_kiwix_container(base_url, max_articles, concurrency \\ 16)
    when is_integer(max_articles) and max_articles >= 0 and
           is_integer(concurrency) and concurrency > 0 do
    initial_url =
      "#{String.trim_trailing(base_url, "/")}/content/#{@zim_name}/" <>
        "List_of_states_and_territories_of_the_United_States"

    queue = :queue.in(initial_url, :queue.new())
    seen = MapSet.new([initial_url])
    request = Req.new(receive_timeout: 5_000)

    collect_pages(queue, seen, [], max_articles, base_url, request, concurrency)
  end

  defp collect_pages(_queue, _seen, acc, 0, _base_url, _request, _concurrency) do
    Enum.reverse(acc)
  end

  defp collect_pages(queue, seen, acc, remaining, base_url, request, concurrency) do
    case take_batch(queue, min(concurrency, remaining), []) do
      {[], _queue} ->
        Enum.reverse(acc)

      {urls, rest_queue} ->
        {next_queue, next_seen, next_acc, next_remaining} =
          urls
          |> Task.async_stream(
            fn url ->
              {fetch_us, response} = :timer.tc(fn -> Req.get(request, url: url) end)

              case response do
                {:ok, %{status: 200, body: body}} when is_binary(body) ->
                  {parse_us, links} = :timer.tc(fn -> extract_wiki_links(body, url, base_url) end)
                  {:page, url, body, links, fetch_us, parse_us}

                _ ->
                  :skip
              end
            end,
            max_concurrency: concurrency,
            ordered: true,
            timeout: :infinity
          )
          |> Enum.reduce({rest_queue, seen, acc, remaining}, fn
  {:ok, {:page, url, body, links, fetch_us, parse_us}},
  {queue, seen, acc, remaining} ->
    {queue, seen} =
      Enum.reduce(links, {queue, seen}, fn link, {queue, seen} ->
        if MapSet.member?(seen, link) do
          {queue, seen}
        else
          {:queue.in(link, queue), MapSet.put(seen, link)}
        end
      end)

    remaining = remaining - 1

    if rem(remaining, 100) == 0 or remaining == 0 do
      IO.puts(
        "Remaining articles #{remaining}" <>
          "(links: #{length(links)}, queued: #{:queue.len(queue)}, " <>
          "fetch: #{Float.round(fetch_us / 1_000, 1)} ms, " <>
          "parse: #{Float.round(parse_us / 1_000, 1)} ms)"
      )
    end

    {queue, seen, [{url, body} | acc], remaining}

  {:ok, :skip}, state ->
    state
end)

        collect_pages(
          next_queue,
          next_seen,
          next_acc,
          next_remaining,
          base_url,
          request,
          concurrency
        )
    end
  end

  defp take_batch(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp take_batch(queue, remaining, acc) do
    case :queue.out(queue) do
      {{:value, url}, rest_queue} ->
        take_batch(rest_queue, remaining - 1, [url | acc])

      {:empty, _} ->
        {Enum.reverse(acc), queue}
    end
  end

  defp extract_wiki_links(html, current_url, base_url) do
    article_root =
      URI.parse("#{String.trim_trailing(base_url, "/")}/content/#{@zim_name}/")

    # Match opening anchor tags, allowing `>` inside quoted attributes.
    anchor_tag = ~r/<a(?=[\s>])(?:[^>"']|"[^"]*"|'[^']*')*>/i

    # Consume complete attributes so `data-href` and text inside other
    # attribute values aren't mistaken for the actual `href`.
    attribute = ~r/\s+([^\s=\/>]+)(?:\s*=\s*("[^"]*"|'[^']*'|[^\s>]+))?/

    links =
    for [tag] <- Regex.scan(anchor_tag, html),
        [_, name, value] <- Regex.scan(attribute, tag),
        String.downcase(name) == "href",
        href = value |> String.trim("\"") |> String.trim("'") |> String.trim(),
        href != "" and not String.starts_with?(href, "#"),
        uri = URI.merge(current_url, href),
        uri.scheme == article_root.scheme,
        uri.host == article_root.host,
        uri.port == article_root.port,
        is_nil(uri.query),
        String.starts_with?(uri.path || "", article_root.path),
        entry = uri.path |> String.replace_prefix(article_root.path, "") |> URI.decode(),
        entry != "" and not String.starts_with?(entry, ["Special:", "Talk:", "File:"]),
        do: URI.to_string(%{uri | fragment: nil})

     Enum.uniq(links)
  end

end
