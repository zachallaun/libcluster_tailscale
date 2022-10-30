defmodule ClusterTailscale.Strategy do
  @moduledoc """
  Clustering strategy for libcluster using Tailscale to securely identify and connect to peers.

  ## Usage

      config :libcluster,
        topologies: [
          tailnet_example: [
            strategy: ClusterTailscale.Strategy,
            config: [
              hostname: "my-app",
              cli: "tailscale",
              polling_interval: 5_000
            ]
          ]
        ]
  """

  use GenServer

  alias Cluster.Strategy.State
  alias Cluster.Logger

  @default_polling_interval 5_000
  @not_seen ~U[0001-01-01 00:00:00Z]

  def start_link([state]), do: GenServer.start_link(__MODULE__, state)

  @impl true
  def init(%State{meta: nil} = state) do
    state
    |> Map.put(:meta, %{})
    |> init()
  end

  def init(%State{} = state) do
    {:ok, poll(state)}
  end

  @impl true
  def handle_info(:timeout, state), do: {:noreply, poll(state)}
  def handle_info(:poll, state), do: {:noreply, poll(state)}
  def handle_info(_, state), do: {:noreply, state}

  defp poll(%{meta: known_peers} = state) do
    connected = list_nodes(state)

    {known_peers, to_connect, to_disconnect} =
      state
      |> get_online_peers()
      |> resolve_peers(known_peers, connected)

    for node <- to_connect do
      connect(state, node)
    end

    for node <- to_disconnect do
      disconnect(state, node)
    end

    schedule_next_poll(state)

    %{state | meta: known_peers}
  end

  defp resolve_peers(known_peers, prev_known_peers, connected) do
    dbg(known_peers)
    dbg(prev_known_peers)
    dbg(connected)
    all_nodes = known_peers |> Map.values() |> MapSet.new()

    to_connect =
      all_nodes
      |> MapSet.difference(connected)
      |> Enum.filter(fn n ->
        n not in connected &&
          DateTime.compare(
            get_in(known_peers, [n, :last_seen]),
            get_in(prev_known_peers, [n, :last_seen]) || @not_seen
          ) == :gt
      end)

    to_disconnect = MapSet.difference(connected, all_nodes)

    {known_peers, to_connect, to_disconnect}
  end

  defp get_online_peers(state) do
    get_online_peers(state, fetch_hostname(state), fetch_basename(state), fetch_cli(state))
  end

  defp get_online_peers(state, {:ok, hostname}, {:ok, basename}, {:ok, cli}) do
    with {result, 0} <- System.shell("#{cli} status --json"),
         {:ok, %{"Peer" => peers}} <- Jason.decode(result) do
      for {_, %{"Online" => true, "HostName" => ^hostname} = peer} <- peers, into: %{} do
        parsed = parse_peer(peer, basename)
        {parsed.node, parsed}
      end
    else
      _ ->
        error(state.topology, "unable to fetch peers with Tailscale CLI")
        %{}
    end
  end

  defp get_online_peers(_, _, _, _), do: MapSet.new()

  defp parse_peer(peer, basename) do
    ip = peer_ip(peer)

    %{
      ip: ip,
      node: :"#{basename}@#{ip}",
      last_seen: peer_last_seen(peer)
    }
  end

  defp peer_ip(%{"TailscaleIPs" => [ipv4, ipv6]}) do
    case :init.get_argument(:proto_dist) do
      {:ok, [['inet6_tcp']]} -> ipv6
      _ -> ipv4
    end
  end

  defp peer_last_seen(%{"LastSeen" => iso8601}) when is_binary(iso8601) and iso8601 != "" do
    case DateTime.from_iso8601(iso8601) do
      {:ok, last_seen, 0} -> last_seen
      _ -> @not_seen
    end
  end

  defp peer_last_seen(_), do: @not_seen

  defp schedule_next_poll(state) do
    Process.send_after(self(), :poll, polling_interval(state))
  end

  defp list_nodes(%{list_nodes: {m, f, a}}) do
    apply(m, f, a) |> MapSet.new() |> MapSet.put(Node.self())
  end

  defp connect(%{connect: {m, f, a}} = state, node) do
    case apply(m, f, a ++ [node]) do
      true -> info(state, "connected to #{inspect(node)}")
      false -> warn(state, "unable to connect to #{inspect(node)}")
      :ignored -> warn(state, "unable to connect to #{inspect(node)}; not part of network")
    end
  end

  defp disconnect(%{disconnect: {m, f, a}} = state, node) do
    case apply(m, f, a ++ [node]) do
      true -> info(state, "disconnected from #{inspect(node)}")
      false -> warn(state, "unable to disconnect from #{inspect(node)}")
      :ignored -> warn(state, "unable to disconnect from #{inspect(node)}; not part of network")
      reason -> warn(state, "disconnect from #{inspect(node)} failed with: #{inspect(reason)}")
    end
  end

  ## Config

  defp fetch_hostname(%{config: config, topology: t}) do
    case Keyword.fetch(config, :hostname) do
      {:ok, hostname} when is_binary(hostname) and hostname != "" ->
        {:ok, hostname}

      {:ok, invalid} ->
        invalid_config(t, :hostname, invalid)
        :error

      :error ->
        error(t, "no :hostname specified for ClusterTailscale.Strategy")
        :error
    end
  end

  defp fetch_basename(%{config: config, topology: t} = state) do
    case Keyword.fetch(config, :node_basename) do
      {:ok, basename} when is_binary(basename) and basename != "" ->
        {:ok, basename}

      {:ok, invalid} ->
        invalid_config(t, :node_basename, invalid)
        :error

      :error ->
        fetch_default_basename(state)
    end
  end

  defp fetch_cli(%{config: config, topology: t}) do
    case Keyword.fetch(config, :cli) do
      {:ok, cli} when is_binary(cli) and cli != "" ->
        {:ok, cli}

      {:ok, invalid} ->
        invalid_config(t, :cli, invalid)
        :error

      :error ->
        {:ok, "tailscale"}
    end
  end

  defp fetch_default_basename(%{topology: t}) do
    if Node.alive?() do
      [basename, _host] = Node.self() |> to_string() |> String.split("@")
      {:ok, basename}
    else
      error(t, "current node must be alive to use default :node_basename")
    end
  end

  defp polling_interval(%{config: config}) do
    Keyword.get(config, :polling_interval, @default_polling_interval)
  end

  defp invalid_config(topology, key, invalid) do
    error(topology, "invalid #{inspect(key)} for ClusterTailscale.Strategy: #{inspect(invalid)}")
  end

  ## Logging

  defp info(%{topology: t}, msg), do: Logger.info(t, msg)
  defp warn(%{topology: t}, msg), do: Logger.warn(t, msg)
  defp error(%{topology: t}, msg), do: Logger.error(t, msg)
end
