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
  import Cluster.Logger

  alias Cluster.Strategy.State
  alias Cluster.Strategy

  @default_polling_interval 5_000

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init([%State{meta: nil} = state]) do
    init([%{state | meta: MapSet.new()}])
  end

  def init([%State{} = state]) do
    {:ok, poll(state)}
  end

  @impl true
  def handle_info(:timeout, state), do: {:noreply, poll(state)}
  def handle_info(:poll, state), do: {:noreply, poll(state)}
  def handle_info(_, state), do: {:noreply, state}

  defp poll(%State{} = state) do
    nodeset =
      get_nodes(state)
      |> disconnect_nodes(state)
      |> connect_nodes(state)

    schedule_next_poll(state)

    %{state | meta: nodeset}
  end

  defp disconnect_nodes(nodeset, %{meta: old_nodeset} = state) do
    %{disconnect: disconnect, topology: t, list_nodes: list_nodes} = state
    removed = MapSet.difference(old_nodeset, nodeset)

    case Strategy.disconnect_nodes(t, disconnect, list_nodes, MapSet.to_list(removed)) do
      :ok -> nodeset
      {:error, not_disconnected} -> MapSet.union(nodeset, MapSet.new(not_disconnected))
    end
  end

  defp connect_nodes(nodeset, state) do
    %{connect: connect, topology: t, list_nodes: list_nodes} = state

    case Strategy.connect_nodes(t, connect, list_nodes, MapSet.to_list(nodeset)) do
      :ok -> nodeset
      {:error, not_connected} -> MapSet.difference(nodeset, MapSet.new(not_connected))
    end
  end

  defp schedule_next_poll(state) do
    Process.send_after(self(), :poll, polling_interval(state))
  end

  defp get_nodes(state) do
    get_nodes(state, fetch_hostname(state), fetch_basename(state), fetch_cli(state))
  end

  defp get_nodes(state, {:ok, hostname}, {:ok, basename}, {:ok, cli}) do
    with {result, 0} <- System.shell("#{cli} status --json"),
         {:ok, %{"Peer" => peers}} <- Jason.decode(result) do
      for {_, %{"Online" => true, "HostName" => ^hostname} = peer} <- peers,
          into: MapSet.new() do
        :"#{basename}@#{peer_ip(peer)}"
      end
    else
      _ ->
        error(state.topology, "unable to fetch peers with Tailscale CLI")
        MapSet.new()
    end
  end

  defp get_nodes(_, _, _, _), do: MapSet.new()

  defp peer_ip(%{"TailscaleIPs" => [ipv4, ipv6]}) do
    case :init.get_argument(:proto_dist) do
      {:ok, [['inet6_tcp']]} -> ipv6
      _ -> ipv4
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
end
