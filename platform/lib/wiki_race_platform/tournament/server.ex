defmodule WikiRacePlatform.Tournament.Server do
  @moduledoc """
  Walks the tournament's fixture list and reports standings.

  Everything here is recomputed from `WikiRacePlatform.Tournament.Store`'s
  race history plus the configured roster. There is nothing else to
  persist and nothing that can drift out of sync with it. A round's
  fixtures are generated once, deterministically, from standings as of the
  previous round's end, never mid-round, so a round's pairings don't shift
  as its own results trickle in. Recovering after a restart just means
  regenerating the current round and skipping however many of its
  fixtures are already in the store.

  Dispatch is deliberately sequential, one fixture in flight at a time,
  which is all today's two-player-VM topology can ever use anyway. The
  harness underneath can run more than one race at once; this module just
  never asks it to.
  """

  use GenServer

  alias WikiRacePlatform.Articles
  alias WikiRacePlatform.Race
  alias WikiRacePlatform.Race.Config
  alias WikiRacePlatform.Race.Dispatcher
  alias WikiRacePlatform.Race.Result
  alias WikiRacePlatform.Tournament.Fixtures
  alias WikiRacePlatform.Tournament.Standings
  alias WikiRacePlatform.Tournament.Store

  @time_limit_ms 120_000
  @poll_interval_ms 250

  defstruct [
    :roster,
    :generator,
    :total_rounds,
    :store,
    :dispatcher,
    :pair,
    :round_pairs,
    current_round: 0,
    pair_overridden: false,
    queue: []
  ]

  # Client

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Current standings, one entry per roster name."
  @spec standings(GenServer.server()) :: %{String.t() => Standings.standing()}
  def standings(server \\ __MODULE__), do: GenServer.call(server, :standings)

  @doc "The next fixture to dispatch, or `nil` if the tournament is complete."
  @spec next_fixture(GenServer.server()) :: Fixtures.fixture() | nil
  def next_fixture(server \\ __MODULE__), do: GenServer.call(server, :next_fixture)

  @doc "Up to `count` fixtures still queued this round, in dispatch order. The spectator bracket."
  @spec upcoming(pos_integer(), GenServer.server()) :: [Fixtures.fixture()]
  def upcoming(count \\ 3, server \\ __MODULE__),
    do: GenServer.call(server, {:upcoming, count})

  @doc """
  Every fixture still queued this round, in dispatch order.

  Not the whole tournament. Swiss round 2+ pairings aren't decided until
  the round before them finishes, so there's nothing further to show yet.
  Round-robin's fixture list is generated all at once, so under that
  generator this genuinely is every fixture from here to the end.
  """
  @spec upcoming_round(GenServer.server()) :: [Fixtures.fixture()]
  def upcoming_round(server \\ __MODULE__), do: GenServer.call(server, :upcoming_round)

  @doc "Every pairing that has already raced, normalized so {a, b} and {b, a} collapse."
  @spec played_pairs(GenServer.server()) :: MapSet.t({String.t(), String.t()})
  def played_pairs(server \\ __MODULE__), do: GenServer.call(server, :played_pairs)

  @doc "Which round the tournament is on, and whether it's finished."
  @spec progress(GenServer.server()) :: %{
          round: pos_integer(),
          total_rounds: pos_integer(),
          complete: boolean()
        }
  def progress(server \\ __MODULE__), do: GenServer.call(server, :progress)

  @doc """
  Starts the next queued fixture, using the currently configured article
  pair (see `current_pair/1`).

  `node_mapping` maps each roster name involved in that fixture to the
  connected node currently running their release. This module stays
  decoupled from `WikiRacePlatform.Deploy.Manifest` on purpose. The caller
  (`ScheduleLive`, resolving each name against `priv/groups.exs`) supplies
  the mapping explicitly instead of this module reading the manifest
  itself.
  """
  @spec start_next!(%{String.t() => node()}, GenServer.server()) ::
          {:ok, map()} | {:error, String.t()}
  def start_next!(node_mapping, server \\ __MODULE__) do
    GenServer.call(server, {:start_next, node_mapping}, 20_000)
  end

  @doc "Forces a recompute from the store, e.g. after a race finished elsewhere."
  @spec refresh(GenServer.server()) :: :ok
  def refresh(server \\ __MODULE__), do: GenServer.cast(server, :refresh)

  @doc """
  The curated article pair (see `WikiRacePlatform.Articles.pairs/0`) every
  subsequent `start_next!/2` call races, until changed with `set_pair/2` or
  the round advances.

  Fixed for the round rather than drawn fresh per race: a randomly-drawn
  pair can be many hops apart, making a race's wait time unpredictable, so
  a known pair for the whole round keeps every match in it comparable.
  Defaults to the `:round_pairs` progression configured under
  `config :wiki_race_platform, :tournament` (a `{start_title, target_title}`
  per round, so later rounds can be harder). `set_pair/2` overrides just
  the current round; the next round reapplies the configured progression.
  """
  @spec current_pair(GenServer.server()) :: Articles.pair() | nil
  def current_pair(server \\ __MODULE__), do: GenServer.call(server, :current_pair)

  @doc "Whether the current round's pair was manually chosen rather than from the configured progression."
  @spec pair_overridden?(GenServer.server()) :: boolean()
  def pair_overridden?(server \\ __MODULE__),
    do: GenServer.call(server, :pair_overridden?)

  @doc "Overrides the article pair for the rest of the current round only. The next round reapplies the configured progression."
  @spec set_pair(Articles.pair(), GenServer.server()) :: :ok
  def set_pair(pair, server \\ __MODULE__), do: GenServer.call(server, {:set_pair, pair})

  @doc "Every finished race counted toward this tournament's progress, in store order."
  @spec history(GenServer.server()) :: [Result.t()]
  def history(server \\ __MODULE__), do: GenServer.call(server, :history)

  # Server

  @impl true
  def init(opts) do
    config = Application.get_env(:wiki_race_platform, :tournament, [])

    round_pairs =
      (Keyword.get(opts, :round_pairs) || Keyword.get(config, :round_pairs, []))
      |> resolve_round_pairs()

    state = %__MODULE__{
      roster: Keyword.get(opts, :roster) || Keyword.fetch!(config, :roster),
      generator: Keyword.get(opts, :generator) || Keyword.get(config, :generator, :swiss),
      total_rounds:
        Keyword.get(opts, :total_rounds) || Keyword.get(config, :total_rounds, 3),
      store: Keyword.get(opts, :store, Store),
      dispatcher: Keyword.get(opts, :dispatcher, Dispatcher),
      round_pairs: round_pairs
    }

    Race.subscribe()

    {:ok, recompute(state)}
  end

  # `round_pairs` config is `{start_title, target_title}` tuples, not full
  # pair maps, since plain strings are what a professor actually edits.
  # Resolving against the curated pool here means a typo falls back to a
  # working (if unvetted) pair instead of silently never matching the
  # dropdown's `==` comparison against `Articles.pairs()`.
  defp resolve_round_pairs(configured) do
    pool = Articles.pairs()

    Enum.map(configured, fn {start_title, target_title} ->
      Enum.find(
        pool,
        %{start: start_title, target: target_title, note: ""},
        &(&1.start == start_title and &1.target == target_title)
      )
    end)
  end

  @impl true
  def handle_call(:standings, _from, state) do
    {standings, _played} = Standings.compute(tournament_history(state))
    {:reply, fill_roster(standings, state.roster), state}
  end

  def handle_call(:next_fixture, _from, state) do
    {:reply, List.first(state.queue), state}
  end

  def handle_call({:upcoming, count}, _from, state) do
    {:reply, Enum.take(state.queue, count), state}
  end

  def handle_call(:upcoming_round, _from, state) do
    {:reply, state.queue, state}
  end

  def handle_call(:played_pairs, _from, state) do
    {_standings, played} = Standings.compute(tournament_history(state))
    {:reply, played, state}
  end

  def handle_call(:progress, _from, state) do
    completed = completed_rounds(state)

    progress = %{
      round: min(completed + 1, state.total_rounds),
      total_rounds: state.total_rounds,
      complete: completed >= state.total_rounds
    }

    {:reply, progress, state}
  end

  def handle_call({:start_next, node_mapping}, _from, state) do
    case List.first(state.queue) do
      nil ->
        {:reply, {:error, "no fixture is queued"}, state}

      fixture ->
        reply =
          with {:ok, config} <- build_config(fixture, node_mapping, state) do
            Dispatcher.start_race(config, state.dispatcher)
          end

        {:reply, reply, state}
    end
  end

  def handle_call(:current_pair, _from, state), do: {:reply, state.pair, state}

  def handle_call(:pair_overridden?, _from, state),
    do: {:reply, state.pair_overridden, state}

  def handle_call({:set_pair, pair}, _from, state),
    do: {:reply, :ok, %{state | pair: pair, pair_overridden: true}}

  def handle_call(:history, _from, state), do: {:reply, tournament_history(state), state}

  @impl true
  def handle_info({:race_update, %{status: :finished}}, state) do
    {:noreply, recompute(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_cast(:refresh, state), do: {:noreply, recompute(state)}

  # Queue computation

  defp recompute(state) do
    state
    |> apply_round_pair()
    |> then(&%{&1 | queue: compute_queue(&1)})
  end

  # The configured pair progression only ever applies when the round
  # actually changes. A race finishing mid-round (or `refresh/1` being
  # called for an unrelated reason) must never clobber a pair the admin
  # just manually set for the round still in progress.
  defp apply_round_pair(state) do
    round = min(completed_rounds(state) + 1, max(state.total_rounds, 1))

    if round == state.current_round do
      state
    else
      %{
        state
        | current_round: round,
          pair: round_pair(state, round),
          pair_overridden: false
      }
    end
  end

  defp round_pair(state, round) do
    case Enum.at(state.round_pairs, round - 1) do
      nil -> List.last(state.round_pairs) || state.pair || List.first(Articles.pairs())
      pair -> pair
    end
  end

  defp compute_queue(state) do
    history = tournament_history(state)
    per_round = entries_per_round(state.roster)
    completed_rounds = completed_rounds(state, history, per_round)

    if completed_rounds >= state.total_rounds do
      []
    else
      into_round = rem(length(history), per_round)

      state.roster
      |> generate_round(state.generator, completed_rounds, history, per_round)
      |> Enum.drop(into_round)
    end
  end

  defp completed_rounds(state) do
    history = tournament_history(state)
    per_round = entries_per_round(state.roster)
    completed_rounds(state, history, per_round)
  end

  defp completed_rounds(_state, history, per_round), do: div(length(history), per_round)

  # `Store.list/1` is every race the platform has ever run, not just this
  # tournament's. Practice races, mock-crawler demos, and old test data all
  # land in the same log with whatever labels their callers used. Only
  # results where every participant is actually in the roster count toward
  # round/standings tracking, so unrelated races can't shift the queue.
  defp tournament_history(state) do
    state.store
    |> Store.list()
    |> Enum.map(& &1.result)
    |> Enum.filter(&tournament_result?(&1, state.roster))
  end

  defp tournament_result?(result, roster) do
    Enum.all?(result.ranking, &(&1.label in roster))
  end

  # A roster name with no finished race (or bye) yet has nothing for
  # `Standings.compute/1` to fold, so it's simply absent from its result.
  # That's not a bug there; that module only knows about results, never the
  # roster. This is where "one entry per roster name" (the documented
  # contract of `standings/1`) actually gets kept: every configured group
  # shows up from round one, zeroed out, instead of being invisible until
  # its first race.
  defp fill_roster(standings, roster) do
    zero = %{wins: 0, score_sum: 0.0, byes: 0, games: 0}
    Enum.reduce(roster, standings, &Map.put_new(&2, &1, zero))
  end

  defp entries_per_round(roster), do: roster |> length() |> Kernel.+(1) |> div(2)

  defp generate_round(roster, :swiss, completed_rounds, history, per_round) do
    prior = Enum.take(history, completed_rounds * per_round)
    {standings, played} = Standings.compute(prior)
    Fixtures.swiss(roster, standings, played)
  end

  defp generate_round(roster, :round_robin, completed_rounds, _history, _per_round) do
    roster
    |> Fixtures.round_robin()
    |> Enum.at(completed_rounds, [])
  end

  # Dispatch

  defp build_config({:bye, name}, node_mapping, state) do
    with {:ok, node} <- fetch_node(node_mapping, name) do
      new_config(state, [%{node: node, label: name, module: nil}])
    end
  end

  defp build_config({a, b}, node_mapping, state) do
    with {:ok, node_a} <- fetch_node(node_mapping, a),
         {:ok, node_b} <- fetch_node(node_mapping, b) do
      new_config(state, [
        %{node: node_a, label: a, module: nil},
        %{node: node_b, label: b, module: nil}
      ])
    end
  end

  defp fetch_node(node_mapping, name) do
    case Map.fetch(node_mapping, name) do
      {:ok, node} -> {:ok, node}
      :error -> {:error, "no node given for #{name}"}
    end
  end

  defp new_config(state, players) do
    case state.pair do
      nil ->
        {:error, "no article pair configured"}

      pair ->
        {start_url, target_url} = Articles.urls(pair)

        Config.new(
          start_url: start_url,
          target_url: target_url,
          time_limit_ms: @time_limit_ms,
          poll_interval_ms: @poll_interval_ms,
          players: players
        )
    end
  end
end
