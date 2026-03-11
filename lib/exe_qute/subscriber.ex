defmodule ExeQute.Subscriber do
  @moduledoc """
  A long-lived KDB+ tickerplant connection that fans push messages out to
  multiple local subscribers over a single TCP connection.

  In most cases you do not need to interact with this module directly —
  `ExeQute.subscribe/2` and `ExeQute.unsubscribe/2` create and manage
  subscriber processes for you.

  Use this module directly when you want explicit lifecycle control, such as
  starting the subscriber in a supervision tree:

      defmodule MyApp.Application do
        use Application

        def start(_type, _args) do
          children = [
            {ExeQute.Subscriber, host: "tp-host", port: 5010, name: :tp},
            MyApp.TradeHandler
          ]

          Supervisor.start_link(children, strategy: :one_for_one)
        end
      end

  Once started, attach subscriptions via `ExeQute.subscribe/2`:

      ExeQute.subscribe(:tp, "trade")
      ExeQute.subscribe(:tp, "quote")

  Or use this module's own `add/3` and `add/4` for callback-based subscriptions:

      {:ok, ref} = ExeQute.Subscriber.add(:tp, "trade", fn {table, data} ->
        IO.inspect({table, data})
      end)

      ExeQute.Subscriber.remove(:tp, ref)
  """

  use GenServer
  require Logger
  alias ExeQute.Protocol

  @default_timeout 5000

  @doc """
  Starts a subscriber process connected to the KDB+ tickerplant at `host`/`port`.

  Accepts the same connection options as `ExeQute.connect/1`. Pass `name:` to
  register the process under an atom for use throughout your application.

  ## Options

  | Option | Default | Description |
  |---|---|---|
  | `:host` | `"localhost"` | Tickerplant hostname or IP |
  | `:port` | `5001` | Tickerplant port |
  | `:username` | `nil` | Username |
  | `:password` | `nil` | Password |
  | `:name` | `nil` | Register under this atom |

  ## Examples

      {:ok, sub} = ExeQute.Subscriber.start_link(host: "tp-host", port: 5010)

      ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, init_opts} =
      case Keyword.pop(opts, :name) do
        {nil, rest} -> {[], rest}
        {name, rest} -> {[name: name], rest}
      end

    GenServer.start_link(__MODULE__, init_opts, name_opt)
  end

  @doc """
  Finds an existing subscriber for `host`/`port` or starts a new one.

  Multiple calls with the same host and port return the same pid without
  opening a second TCP connection. This is the recommended way to obtain a
  subscriber in Livebook notebooks because re-evaluating a cell reuses the
  existing connection rather than tearing it down.

  ## Examples

      {:ok, tp} = ExeQute.Subscriber.start_or_find(host: "tp-host", port: 9600)

  """
  @spec start_or_find(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_or_find(opts) do
    host = Keyword.get(opts, :host, "localhost")
    port = Keyword.get(opts, :port, 5001)

    case Registry.lookup(ExeQute.SubscriberRegistry, {host, port}) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        via = {:via, Registry, {ExeQute.SubscriberRegistry, {host, port}}}
        full_opts = Keyword.put(opts, :name, via)

        case DynamicSupervisor.start_child(ExeQute.SubscriberSupervisor, {__MODULE__, full_opts}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          error -> error
        end
    end
  end

  @doc """
  Adds a callback subscription to `table` on the given subscriber.

  The callback receives `{table, data}` on each update from the tickerplant.

  Returns `{:ok, ref}` — pass `ref` to `remove/2` to unsubscribe.

  ## Examples

      {:ok, ref} = ExeQute.Subscriber.add(:tp, "trade", fn {table, data} ->
        IO.inspect({table, data})
      end)
  """
  @spec add(pid() | atom(), String.t(), (tuple() -> any())) ::
          {:ok, reference()} | {:error, term()}
  def add(sub, table, callback) when is_binary(table) and is_function(callback, 1) do
    add(sub, table, [], callback)
  end

  @doc """
  Adds a callback subscription to `table` filtered to the given symbols.

  Only updates for symbols in `syms` are delivered to the callback.
  Pass an empty list to receive all symbols.

  Returns `{:ok, ref}` — pass `ref` to `remove/2` to unsubscribe.

  ## Examples

      {:ok, ref} = ExeQute.Subscriber.add(:tp, "quote", ["AAPL", "MSFT"], fn {table, data} ->
        IO.inspect(data)
      end)
  """
  @spec add(pid() | atom(), String.t(), [String.t()], (tuple() -> any())) ::
          {:ok, reference()} | {:error, term()}
  def add(sub, table, syms, callback)
      when is_binary(table) and is_list(syms) and is_function(callback, 1) do
    safe_call(sub, fn -> GenServer.call(sub, {:add, table, syms, callback}, @default_timeout) end)
  end

  @doc false
  @spec add_process(pid(), String.t()) :: :ok | {:error, term()}
  def add_process(sub_pid, table) when is_pid(sub_pid) and is_binary(table) do
    safe_call(sub_pid, fn ->
      GenServer.call(sub_pid, {:add_process, table}, @default_timeout)
    end)
  end

  @doc false
  @spec remove_process(pid(), String.t()) :: :ok
  def remove_process(sub_pid, table) when is_pid(sub_pid) and is_binary(table) do
    safe_call(sub_pid, fn ->
      GenServer.call(sub_pid, {:remove_process, table}, @default_timeout)
    end)
  end

  @doc false
  @spec purge_callbacks(pid(), String.t()) :: :ok
  def purge_callbacks(sub_pid, table) when is_pid(sub_pid) and is_binary(table) do
    safe_call(sub_pid, fn ->
      GenServer.call(sub_pid, {:purge_callbacks, table}, @default_timeout)
    end)
  end

  @doc false
  @spec remove_table(pid(), String.t()) :: :ok
  def remove_table(sub_pid, table) when is_pid(sub_pid) and is_binary(table) do
    safe_call(sub_pid, fn ->
      GenServer.call(sub_pid, {:remove_table, table}, @default_timeout)
    end)
  end

  @doc """
  Removes a callback subscription identified by `ref`.

  `ref` is the value returned by `add/3` or `add/4`. Always returns `:ok`,
  even if the subscriber has already stopped.

  ## Examples

      {:ok, ref} = ExeQute.Subscriber.add(:tp, "trade", fn {_t, data} -> ... end)
      ExeQute.Subscriber.remove(:tp, ref)
  """
  @spec remove(pid() | atom(), reference()) :: :ok
  def remove(sub, ref) when is_reference(ref) do
    try do
      GenServer.call(sub, {:remove, ref})
    catch
      :exit, _ -> :ok
    end
  end

  @doc """
  Stops the subscriber and closes its tickerplant connection.

  Always returns `:ok`. Prefer `ExeQute.unsubscribe/2` for removing individual
  subscriptions without tearing down the connection.

  ## Examples

      ExeQute.Subscriber.stop(:tp)
  """
  @spec stop(pid() | atom()) :: :ok
  def stop(sub) do
    try do
      GenServer.stop(sub, :normal, @default_timeout)
    catch
      :exit, _ -> :ok
    end
  end

  def init(opts) do
    state = %{
      socket: nil,
      buffer: <<>>,
      subscriptions: %{},
      subscribed_tables: MapSet.new(),
      table_counts: %{},
      credentials: build_credentials(opts)
    }

    with {:ok, socket} <- tcp_connect(opts),
         :ok <- handshake(socket, state.credentials) do
      {:ok, %{state | socket: socket}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call({:add, table, syms, callback}, _from, state) do
    case do_subscribe_table(table, syms, state) do
      {:ok, new_state} ->
        ref = make_ref()
        subs = Map.put(new_state.subscriptions, ref, {table, callback})
        counts = Map.update(new_state.table_counts, table, 1, &(&1 + 1))
        {:reply, {:ok, ref}, %{new_state | subscriptions: subs, table_counts: counts}}

      {:error, reason} ->
        Logger.error("Subscription add error: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:add_process, table}, _from, state) do
    case do_subscribe_table(table, [], state) do
      {:ok, new_state} ->
        counts = Map.update(new_state.table_counts, table, 1, &(&1 + 1))
        {:reply, :ok, %{new_state | table_counts: counts}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:remove, ref}, _from, state) do
    case Map.pop(state.subscriptions, ref) do
      {{table, _callback}, subs} ->
        new_state = decrement_count(table, %{state | subscriptions: subs})
        {:reply, :ok, new_state}

      {nil, _} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:remove_process, table}, _from, state) do
    {:reply, :ok, decrement_count(table, state)}
  end

  def handle_call({:purge_callbacks, table}, _from, state) do
    subs = Map.filter(state.subscriptions, fn {_ref, {t, _cb}} -> t != table end)
    {:reply, :ok, %{state | subscriptions: subs}}
  end

  def handle_call({:remove_table, table}, _from, state) do
    subs = Map.filter(state.subscriptions, fn {_ref, {t, _cb}} -> t != table end)
    removed = map_size(state.subscriptions) - map_size(subs)
    new_state = %{state | subscriptions: subs}

    new_state =
      if removed > 0 do
        Enum.reduce(1..removed, new_state, fn _, acc -> decrement_count(table, acc) end)
      else
        decrement_count(table, new_state)
      end

    {:reply, :ok, new_state}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    buffer = state.buffer <> data
    {remaining, new_state} = process_messages(buffer, state)
    :inet.setopts(socket, active: :once)
    {:noreply, %{new_state | buffer: remaining}}
  end

  def handle_info({:tcp_closed, _socket}, state) do
    Logger.error("KDB+ subscription connection closed")
    {:stop, :tcp_closed, state}
  end

  def handle_info({:tcp_error, _socket, reason}, state) do
    Logger.error("KDB+ subscription TCP error: #{inspect(reason)}")
    {:stop, {:tcp_error, reason}, state}
  end

  defp process_messages(<<_::8, _::8, _::8, _::8, size::little-32, _::binary>> = buffer, state)
       when byte_size(buffer) >= size do
    <<message::binary-size(size), rest::binary>> = buffer
    new_state = handle_message(message, state)
    process_messages(rest, new_state)
  end

  defp process_messages(buffer, state), do: {buffer, state}

  defp handle_message(message, state) do
    result =
      try do
        Protocol.decode_message(message)
      rescue
        e -> {:error, {:decode_error, Exception.message(e)}}
      end

    case result do
      {:ok, ["upd", table, rows]} ->
        dispatch_callbacks(table, rows, state.subscriptions)
        dispatch_registry(table, rows)

      {:ok, other} ->
        Logger.debug("Subscriber unhandled message: #{inspect(other)}")

      {:error, reason} ->
        Logger.error("Subscriber decode error: #{inspect(reason)}")
    end

    state
  end

  defp decrement_count(table, %{socket: socket, table_counts: counts} = state) do
    new_count = max(Map.get(counts, table, 1) - 1, 0)

    if new_count == 0 do
      try_unsub(socket, table)

      %{
        state
        | table_counts: Map.delete(counts, table),
          subscribed_tables: MapSet.delete(state.subscribed_tables, table)
      }
    else
      %{state | table_counts: Map.put(counts, table, new_count)}
    end
  end

  defp try_unsub(socket, table) do
    query = ".u.unsub[`#{table}]"
    payload = <<10::8, 0::8, byte_size(query)::little-32, query::binary>>
    total_size = byte_size(payload) + 8
    header = <<1::8, 0::8, 0::8, 0::8, total_size::little-32>>
    :gen_tcp.send(socket, [header, payload])
  end

  defp do_subscribe_table(table, syms, %{socket: socket, subscribed_tables: subscribed} = state) do
    if MapSet.member?(subscribed, table) do
      {:ok, state}
    else
      :inet.setopts(socket, active: false)

      result =
        try do
          query = build_sub_query(table, syms)

          with {:ok, encoded} <- Protocol.encode(query),
               :ok <- :gen_tcp.send(socket, encoded),
               {:ok, _schema} <- Protocol.recv(socket, @default_timeout) do
            :ok
          end
        rescue
          e -> {:error, {:encode_error, Exception.message(e)}}
        end

      :inet.setopts(socket, active: :once)

      case result do
        :ok -> {:ok, %{state | subscribed_tables: MapSet.put(subscribed, table)}}
        error -> error
      end
    end
  end

  defp dispatch_callbacks(table, rows, subscriptions) do
    subscriptions
    |> Enum.filter(fn {_ref, {t, _cb}} -> t == table end)
    |> Enum.each(fn {_ref, {_t, callback}} ->
      try do
        callback.({table, rows})
      rescue
        e -> Logger.error("Subscriber callback error: #{Exception.message(e)}")
      catch
        :exit, {:noproc, _} -> :ok
        :exit, reason -> Logger.warning("Subscriber callback exit: #{inspect(reason)}")
      end
    end)
  end

  defp dispatch_registry(table, rows) do
    Registry.dispatch(ExeQute.Registry, {self(), table}, fn entries ->
      Enum.each(entries, fn {pid, _} -> send(pid, {:exe_qute, table, rows}) end)
    end)
  end

  defp build_sub_query(table, []), do: ".u.sub[`#{table};`]"

  defp build_sub_query(table, syms) do
    sym_str = syms |> Enum.map(&"`#{&1}") |> Enum.join()
    ".u.sub[`#{table};#{sym_str}]"
  end

  defp safe_call(_sub, fun) do
    try do
      fun.()
    catch
      :exit, {:noproc, _} -> {:error, :not_connected}
      :exit, {:timeout, _} -> {:error, :timeout}
      :exit, reason -> {:error, {:connection_error, reason}}
    end
  end

  defp tcp_connect(opts) do
    host = Keyword.get(opts, :host, "localhost") |> String.to_charlist()
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    with {:ok, port} <- opts |> Keyword.get(:port, 5001) |> normalize_port() do
      :gen_tcp.connect(
        host,
        port,
        [:binary, active: false, packet: 0, keepalive: true, nodelay: true],
        timeout
      )
    end
  end

  defp normalize_port(port) when is_integer(port), do: {:ok, port}

  defp normalize_port(port) when is_binary(port) do
    case Integer.parse(port) do
      {n, ""} -> {:ok, n}
      _ -> {:error, :invalid_port}
    end
  end

  defp normalize_port(_), do: {:error, :invalid_port}

  defp handshake(socket, nil) do
    msg = <<3::8, 0::8>>

    with :ok <- :gen_tcp.send(socket, msg),
         {:ok, <<cap::8>>} <- :gen_tcp.recv(socket, 1) do
      if cap >= 1, do: :ok, else: {:error, :invalid_capability}
    end
  end

  defp handshake(socket, {user, password}) do
    auth = user <> ":" <> password
    msg = <<auth::binary, 3::8, 0::8>>

    with :ok <- :gen_tcp.send(socket, msg),
         {:ok, <<cap::8>>} <- :gen_tcp.recv(socket, 1) do
      if cap >= 1, do: :ok, else: {:error, :invalid_capability}
    end
  end

  defp build_credentials(opts) do
    username = Keyword.get(opts, :username)
    password = Keyword.get(opts, :password)

    case {username, password} do
      {nil, nil} -> nil
      {u, p} when is_binary(u) and is_binary(p) -> {u, p}
      _ -> raise ArgumentError, "Invalid credentials format"
    end
  end
end
