defmodule ExeQute do
  @moduledoc """
  Elixir client for KDB+, the high-performance time-series database.

  Handles the KDB+ IPC wire protocol, full type system encoding/decoding, and
  tickerplant pub/sub. All functions return `{:ok, result}` or `{:error, reason}` —
  no exceptions escape to the caller.

  ## Querying

  ### One-shot

      {:ok, result} = ExeQute.query("select from trade", host: "kdb-host", port: 5010)

  ### Persistent connection

      {:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010)
      {:ok, result} = ExeQute.query(conn, "select from trade")
      ExeQute.disconnect(conn)

  ### Named connection

      ExeQute.connect(host: "kdb-host", port: 5010, name: :trades)
      {:ok, result} = ExeQute.query(:trades, "select from trade")

  ### Parameterized queries

  Arguments are encoded as KDB+ types on the wire — no string interpolation needed.

      {:ok, result} = ExeQute.query(conn, "{x + y}", [1, 2])
      {:ok, result} = ExeQute.query(conn, ".myns.getquotes", ["USD/JPY", ~D[2024-01-01]])

  ### Publishing (fire-and-forget)

      :ok = ExeQute.publish(conn, ".feed.upd", ["trade", rows])

  ## Pub/Sub

  Subscribe to KDB+ tickerplant push messages. One TCP connection to the tickerplant
  serves any number of local subscribers.

  ### Process-based — receive messages in `handle_info`

      defmodule MyApp.TradeHandler do
        use GenServer

        def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

        def subscribe do
          ExeQute.subscribe("trade", host: "tp-host", port: 5010)
          ExeQute.subscribe("quote", host: "tp-host", port: 5010)
        end

        def unsubscribe do
          ExeQute.unsubscribe("trade", host: "tp-host", port: 5010)
          ExeQute.unsubscribe("quote", host: "tp-host", port: 5010)
        end

        @impl true
        def init(_opts), do: {:ok, %{}}

        @impl true
        def handle_info({:exe_qute, table, data}, state) do
          IO.inspect({table, data})
          {:noreply, state}
        end
      end

      {:ok, _pid} = MyApp.TradeHandler.start_link([])
      MyApp.TradeHandler.subscribe()
      Process.sleep(30_000)
      MyApp.TradeHandler.unsubscribe()

  ### Named subscriber

      ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)

      defmodule MyApp.TradeHandler do
        use GenServer

        def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

        def subscribe do
          ExeQute.subscribe(:tp, "trade")
          ExeQute.subscribe(:tp, "quote")
        end

        def unsubscribe do
          ExeQute.unsubscribe(:tp, "trade")
          ExeQute.unsubscribe(:tp, "quote")
        end

        @impl true
        def init(_opts), do: {:ok, %{}}

        @impl true
        def handle_info({:exe_qute, table, data}, state) do
          IO.inspect({table, data})
          {:noreply, state}
        end
      end

      {:ok, _pid} = MyApp.TradeHandler.start_link([])
      MyApp.TradeHandler.subscribe()
      Process.sleep(30_000)
      MyApp.TradeHandler.unsubscribe()

  ### Callback-based

      ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)

      {:ok, trade_ref} = ExeQute.subscribe(:tp, "trade", fn {table, data} ->
        IO.inspect({table, data})
      end)

      {:ok, quote_ref} = ExeQute.subscribe(:tp, "quote", ["AAPL", "MSFT"], fn {_table, data} ->
        IO.inspect(data)
      end)

      Process.sleep(30_000)

      ExeQute.unsubscribe(:tp, trade_ref)
      ExeQute.unsubscribe(:tp, quote_ref)

  See `ExeQute.Subscriber` for starting a subscriber in a supervision tree.
  """

  alias ExeQute.Connection
  alias ExeQute.Introspect
  alias ExeQute.Subscriber

  @type connect_opts :: [
          host: String.t(),
          port: pos_integer(),
          username: String.t() | nil,
          password: String.t() | nil,
          timeout: pos_integer(),
          encoding: String.t(),
          name: atom() | nil
        ]

  @type table_format :: :maps | :columnar

  @doc """
  Connects to a KDB+ instance.

  ## Options

    * `:host` - hostname or IP address of the KDB+ server (default: `"localhost"`)
    * `:port` - TCP port the KDB+ server is listening on (default: `5001`)
    * `:username` - username for authentication (optional; omit for unauthenticated servers)
    * `:password` - password for authentication (required if `:username` is set)
    * `:timeout` - connection and query timeout in milliseconds (default: `5000`)
    * `:encoding` - character encoding for string data (default: `"utf8"`)
    * `:name` - register the connection under a local atom name so it can be referenced
      without holding the pid (optional)

  ## Examples

      {:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010)

      {:ok, _} = ExeQute.connect(host: "kdb-host", port: 5010, name: :trades)
      ExeQute.query(:trades, "select from trade")

      {:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010, username: "user", password: "pass")

  """
  @spec connect(connect_opts()) :: {:ok, pid()} | {:error, term()}
  def connect(opts \\ []) do
    Connection.start_link(opts)
  end

  @doc """
  Executes a query against a KDB+ instance.

  Accepts either an existing connection (pid or registered name) or connection options
  for a one-shot query.

  ## One-shot query

  Pass the q expression as the first argument and connection options as the second.
  Opens a connection, runs the query, then closes it. Accepts the same options as
  `connect/1`, plus `:table_format` (`:maps` or `:columnar`, default `:maps`).

      {:ok, result} = ExeQute.query("select from trade", host: "kdb-host", port: 5010)
      {:ok, result} = ExeQute.query("select from trade", host: "kdb-host", port: 5010, table_format: :columnar)

  ## Persistent connection

  Pass an already-open connection pid or registered name as the first argument.

      {:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010)
      {:ok, result} = ExeQute.query(conn, "select from trade")

      ExeQute.connect(host: "kdb-host", port: 5010, name: :trades)
      {:ok, result} = ExeQute.query(:trades, "select from trade")

  ## Parameterized queries

  Pass a function name and argument list. Arguments are serialised to q literal syntax
  and sent as a single q expression string — the same form GUI tools like QStudio use.

      {:ok, result} = ExeQute.query(conn, "{x + y}", [1, 2])
      {:ok, result} = ExeQute.query(conn, ".myns.getquotes", ["USD/JPY", ~D[2024-01-01]])
      {:ok, result} = ExeQute.query(conn, ".myns.fn", [])

  """
  @spec query(pid() | atom(), String.t()) :: {:ok, term()} | {:error, term()}
  def query(conn, query) when (is_pid(conn) or is_atom(conn)) and is_binary(query) do
    safe_call(conn, fn -> Connection.query(conn, query) end)
    |> format_result(:maps)
  end

  @spec query(String.t(), connect_opts()) :: {:ok, term()} | {:error, term()}
  def query(query, opts) when is_binary(query) and is_list(opts) do
    {format, conn_opts} = Keyword.pop(opts, :table_format, :maps)

    try do
      with {:ok, conn} <- connect(conn_opts) do
        result = safe_call(conn, fn -> Connection.query(conn, query) end)
        safe_stop(conn)
        format_result(result, format)
      end
    catch
      :exit, reason -> {:error, reason}
    end
  end

  @spec query(pid() | atom(), String.t(), list()) :: {:ok, term()} | {:error, term()}
  def query(conn, func, args)
      when (is_pid(conn) or is_atom(conn)) and is_binary(func) and is_list(args) do
    q = func <> "[" <> Enum.map_join(args, ";", &to_q_literal/1) <> "]"
    safe_call(conn, fn -> Connection.query(conn, q) end)
    |> format_result(:maps)
  end

  @doc """
  Sends a fire-and-forget message to a KDB+ function without waiting for a response.

  Uses the KDB+ async message type — the server receives and processes the call but
  sends no reply. Useful for publishing data into KDB+ feeds or triggering side-effects.

  ## Examples

      ExeQute.publish(conn, ".feed.upd", ["trade", table_data])
      ExeQute.publish(:feed, ".u.pub", ["quote", rows])

  """
  @spec publish(pid() | atom(), String.t(), list()) :: :ok | {:error, term()}
  def publish(conn, func, args)
      when (is_pid(conn) or is_atom(conn)) and is_binary(func) and is_list(args) do
    safe_call(conn, fn -> Connection.publish(conn, [func | args]) end)
  end

  @doc """
  Closes a connection opened with `connect/1`.

  Safe to call on already-dead connections — always returns `:ok`.

  ## Examples

      ExeQute.disconnect(conn)
      ExeQute.disconnect(:trades)

  """
  @spec disconnect(pid() | atom()) :: :ok
  def disconnect(conn) when is_pid(conn) or is_atom(conn) do
    safe_stop(conn)
  end

  @doc """
  Converts raw KDB+ push data to a list of row maps.

  Tickerplant callbacks receive raw decoded data in columnar format.
  Pass the value through this function before iterating over rows.

  ## Examples

      ExeQute.subscribe(tp, "trade", fn {_table, raw} ->
        rows = ExeQute.to_rows(raw)
        Enum.each(rows, &IO.inspect/1)
      end)

  """
  @spec to_rows(term()) :: [map()]
  def to_rows(raw) do
    raw |> to_maps() |> List.wrap() |> Enum.filter(&is_map/1)
  end

  @doc """
  Renders a query result as a tabbed Kino widget.

  Tabs shown depend on the shape of the result:

  - **Table** — shown when result is a non-empty list of maps; uses `Kino.DataTable`
  - **Tree** — shown when result is a non-empty list or map; uses `Kino.Tree`
  - **Raw** — always shown; displays `inspect/1` output as a code block

  ## Examples

      {:ok, result} = ExeQute.query(conn, "select from trade")
      ExeQute.display(result, "select from trade")

  """
  @spec display(term(), String.t()) :: Kino.Layout.t()
  def display(result, label \\ "") do
    raw = Kino.Markdown.new("```elixir\n#{inspect(result, pretty: true, limit: 1000)}\n```")

    tabular? = is_list(result) and match?([%{} | _], result)
    treelike? = tabular? or (is_list(result) and result != []) or (is_map(result) and not is_struct(result))

    tabs =
      []
      |> prepend_if(treelike?, {"Tree", Kino.Tree.new(result)})
      |> prepend_if(tabular?, {"Table", Kino.DataTable.new(result, name: label)})

    Kino.Layout.tabs(tabs ++ [{"Raw", raw}])
  end

  defp prepend_if(list, true, item), do: [item | list]
  defp prepend_if(list, false, _item), do: list

  @doc """
  Lists all namespaces defined on a KDB+ instance.

  Returns names prefixed with `.` — for example `".myns"`, `".q"`, `".Q"`, `".h"`.
  The root namespace itself is not included; use `tables/2`, `functions/2`, and
  `variables/2` without a namespace argument to inspect the root.

  Results are cached per-connection after the first call. Call
  `refresh_introspection/1` to force a fresh fetch after deploying new code.

  See also `functions/2`, `tables/2`, `variables/2`,
  and `ExeQute.Introspect` for the full introspection API.

  ## Examples

      {:ok, namespaces} = ExeQute.namespaces(conn)
      #=> {:ok, [".myns", ".feed", "\.util", ".q", ".Q", ".h"]}

      {:ok, namespaces} = ExeQute.namespaces(:rdb)

  """
  @spec namespaces(pid() | atom()) :: {:ok, [String.t()]} | {:error, term()}
  defdelegate namespaces(conn), to: Introspect

  @doc """
  Lists functions in a namespace with their parameter names and source bodies.

  Pass a namespace string such as `".myns"` to scope the listing, or omit the
  argument (or pass `nil`) to list functions in the root namespace.

  Each entry is a map with three string keys:

  | Key | Description |
  |---|---|
  | `"name"` | Fully qualified function name, e.g. `".myns.getquotes"` |
  | `"params"` | List of parameter name strings; `[]` for zero-arity functions |
  | `"body"` | Verbatim q source of the function body |

  This is useful for building lightweight documentation around a live KDB+
  instance, or for powering autocomplete and signature-help tooling without
  leaving Elixir.

  Results are cached per-connection. Call `refresh_introspection/1` after
  deploying new functions to pick up changes.

  See also `namespaces/1`, `tables/2`, `variables/2`.

  ## Examples

      {:ok, fns} = ExeQute.functions(conn)

      {:ok, fns} = ExeQute.functions(conn, "\.util")
      #=> {:ok, [
      #=>   %{
      #=>     "name"   => "\.util.getquotes",
      #=>     "params" => ["sym", "start", "end"],
      #=>     "body"   => "{[sym;start;end] select from quote where sym=sym, date within (start;end)}"
      #=>   },
      #=>   %{
      #=>     "name"   => "\.util.lasttrade",
      #=>     "params" => ["sym"],
      #=>     "body"   => "{[sym] last select from trade where sym=sym}"
      #=>   },
      #=>   %{
      #=>     "name"   => "\.util.init",
      #=>     "params" => [],
      #=>     "body"   => "{[] ...}"
      #=>   }
      #=> ]}

  Calling a discovered function with its parameter list:

      {:ok, [fn_info | _]} = ExeQute.functions(conn, "\.util")
      name   = fn_info["name"]    #=> "\.util.getquotes"
      params = fn_info["params"]  #=> ["sym", "start", "end"]

      {:ok, result} = ExeQute.query(conn, name, ["EUR/USD", ~D[2024-01-01], ~D[2024-12-31]])

  """
  @spec functions(pid() | atom(), String.t() | nil) :: {:ok, [map()]} | {:error, term()}
  defdelegate functions(conn, namespace \\ nil), to: Introspect

  @doc """
  Lists variable names defined in a namespace.

  Pass a namespace string such as `".myns"` or omit the argument (or pass `nil`)
  to list variables in the root namespace. Returns simple names without the
  namespace prefix.

  Variables are q global values that are not functions and not tables. Use
  `tables/2` to list tables and `functions/2` to list functions.

  Results are cached per-connection. Call `refresh_introspection/1` to force
  a fresh fetch.

  See also `namespaces/1`, `tables/2`, `functions/2`.

  ## Examples

      {:ok, vars} = ExeQute.variables(conn)
      #=> {:ok, ["version", "startTime", "bidSize", "askSize"]}

      {:ok, vars} = ExeQute.variables(conn, ".myns")
      #=> {:ok, ["config", "state"]}

  """
  @spec variables(pid() | atom(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  defdelegate variables(conn, namespace \\ nil), to: Introspect

  @doc """
  Lists table names in a namespace.

  Pass a namespace string such as `".myns"` or omit the argument (or pass `nil`)
  to list tables in the root namespace. Returns simple names without the
  namespace prefix.

  Results are cached per-connection. Call `refresh_introspection/1` to force
  a fresh fetch after schema changes.

  See also `namespaces/1`, `functions/2`, `variables/2`.

  ## Examples

      {:ok, tables} = ExeQute.tables(conn)
      #=> {:ok, ["trade", "quote", "bbo"]}

      {:ok, tables} = ExeQute.tables(conn, ".myns")
      #=> {:ok, ["positions", "orders"]}

      {:ok, tables} = ExeQute.tables(:rdb)

  Querying all rows from a discovered table:

      {:ok, [table | _]} = ExeQute.tables(conn)
      {:ok, rows} = ExeQute.query(conn, "select from \#{table}")

  """
  @spec tables(pid() | atom(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  defdelegate tables(conn, namespace \\ nil), to: Introspect

  @doc """
  Clears the introspection cache on a connection.

  Forces the next call to `namespaces/1`, `functions/2`, `variables/2`, or
  `tables/2` to re-query KDB+. Useful after deploying new functions or tables.

  ## Examples

      ExeQute.refresh_introspection(conn)

  """
  @spec refresh_introspection(pid() | atom()) :: :ok
  def refresh_introspection(conn) do
    safe_call(conn, fn -> Connection.clear_cache(conn) end)
    :ok
  end

  @doc """
  Returns whether a connection process is alive.

  Note: a `true` result means the connection GenServer is running — it does not
  guarantee the underlying TCP socket is still healthy.

  ## Examples

      ExeQute.connected?(conn)   #=> true
      ExeQute.connected?(:trades) #=> false

  """
  @spec connected?(pid() | atom()) :: boolean()
  def connected?(conn) when is_pid(conn), do: Process.alive?(conn)

  def connected?(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  @doc """
  Subscribes to push messages from a KDB+ tickerplant.

  ## Auto-connecting (no prior subscriber needed)

  Pass connection opts as the second argument. The subscriber connection is created
  automatically on first use and reused for subsequent subscriptions to the same
  `host`/`port`. Multiple processes can subscribe to the same table — only one TCP
  connection is opened.

  If you already have a `ExeQute.connect/1` connection open, pass `connection:` to
  reuse its host and port rather than supplying them again:

      ExeQute.subscribe("bbo", connection: :rdb)
      ExeQute.subscribe("trade", fn {_t, data} -> ... end, connection: :rdb)
      ExeQute.unsubscribe("bbo", connection: :rdb)

  Process-based (messages delivered via `handle_info`):

      defmodule MyApp.TradeHandler do
        use GenServer

        def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

        def subscribe do
          ExeQute.subscribe("trade", host: "tp-host", port: 5010)
          ExeQute.subscribe("quote", host: "tp-host", port: 5010)
        end

        def unsubscribe do
          ExeQute.unsubscribe("trade", host: "tp-host", port: 5010)
          ExeQute.unsubscribe("quote", host: "tp-host", port: 5010)
        end

        @impl true
        def init(_opts), do: {:ok, %{}}

        @impl true
        def handle_info({:exe_qute, table, data}, state) do
          IO.inspect({table, data})
          {:noreply, state}
        end
      end

      {:ok, _pid} = MyApp.TradeHandler.start_link([])
      MyApp.TradeHandler.subscribe()
      Process.sleep(30_000)
      MyApp.TradeHandler.unsubscribe()

  ## Existing subscriber

  Pass an already-running subscriber pid or registered name as the first argument.

  Process-based:

      ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)

      defmodule MyApp.TradeHandler do
        use GenServer

        def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

        def subscribe do
          ExeQute.subscribe(:tp, "trade")
          ExeQute.subscribe(:tp, "quote")
        end

        def unsubscribe do
          ExeQute.unsubscribe(:tp, "trade")
          ExeQute.unsubscribe(:tp, "quote")
        end

        @impl true
        def init(_opts), do: {:ok, %{}}

        @impl true
        def handle_info({:exe_qute, table, data}, state) do
          IO.inspect({table, data})
          {:noreply, state}
        end
      end

      {:ok, _pid} = MyApp.TradeHandler.start_link([])
      MyApp.TradeHandler.subscribe()
      Process.sleep(30_000)
      MyApp.TradeHandler.unsubscribe()

  Callback-based:

      ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)

      {:ok, trade_ref} = ExeQute.subscribe(:tp, "trade", fn {table, data} ->
        IO.inspect({table, data})
      end)

      {:ok, quote_ref} = ExeQute.subscribe(:tp, "quote", ["AAPL", "MSFT"], fn {_table, data} ->
        IO.inspect(data)
      end)

      Process.sleep(30_000)

      ExeQute.unsubscribe(:tp, trade_ref)
      ExeQute.unsubscribe(:tp, quote_ref)

  """
  @spec subscribe(String.t(), keyword()) :: :ok | {:error, term()}
  def subscribe(table, opts) when is_binary(table) and is_list(opts) do
    with {:ok, conn_opts} <- resolve_subscriber_opts(opts),
         {:ok, sub_pid} <- Subscriber.start_or_find(conn_opts) do
      register_process(sub_pid, table)
    end
  end

  @spec subscribe(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def subscribe(sub, table) when (is_pid(sub) or is_atom(sub)) and is_binary(table) do
    register_process(resolve_pid(sub), table)
  end

  @spec subscribe(String.t(), (tuple() -> any()), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def subscribe(table, callback, opts)
      when is_binary(table) and is_function(callback, 1) and is_list(opts) do
    with {:ok, conn_opts} <- resolve_subscriber_opts(opts),
         {:ok, sub_pid} <- Subscriber.start_or_find(conn_opts) do
      Subscriber.add(sub_pid, table, callback)
    end
  end

  @spec subscribe(pid() | atom(), String.t(), (tuple() -> any())) ::
          {:ok, reference()} | {:error, term()}
  def subscribe(sub, table, callback)
      when (is_pid(sub) or is_atom(sub)) and is_binary(table) and is_function(callback, 1) do
    Subscriber.add(sub, table, callback)
  end

  @spec subscribe(pid() | atom(), String.t(), [String.t()], (tuple() -> any())) ::
          {:ok, reference()} | {:error, term()}
  def subscribe(sub, table, syms, callback)
      when (is_pid(sub) or is_atom(sub)) and is_binary(table) and is_list(syms) and
             is_function(callback, 1) do
    Subscriber.add(sub, table, syms, callback)
  end

  @doc """
  Removes a subscription.

  For process-based subscriptions, pass the table name — unregisters the calling
  process and notifies the subscriber. If no other process or callback is subscribed
  to that table, `.u.unsub` is sent to the tickerplant.

  For callback subscriptions, pass the reference returned by `subscribe/3` or `/4`.

  ## Examples

      ExeQute.unsubscribe(:tp, "trade")
      ExeQute.unsubscribe(:tp, ref)

      ExeQute.unsubscribe("trade", host: "tp", port: 5010)

  """
  @spec unsubscribe(pid() | atom(), String.t() | reference()) :: :ok
  def unsubscribe(sub, table) when (is_pid(sub) or is_atom(sub)) and is_binary(table) do
    sub_pid = resolve_pid(sub)
    Registry.unregister(ExeQute.Registry, {sub_pid, table})
    Subscriber.remove_process(sub_pid, table)
    Subscriber.purge_callbacks(sub_pid, table)
  end

  def unsubscribe(sub, ref) when (is_pid(sub) or is_atom(sub)) and is_reference(ref) do
    Subscriber.remove(sub, ref)
  end

  @spec unsubscribe(String.t(), keyword()) :: :ok
  def unsubscribe(table, opts) when is_binary(table) and is_list(opts) do
    with {:ok, conn_opts} <- resolve_subscriber_opts(opts),
         {:ok, sub_pid} <- Subscriber.start_or_find(conn_opts) do
      Registry.unregister(ExeQute.Registry, {sub_pid, table})
      Subscriber.remove_table(sub_pid, table)
    else
      _ -> :ok
    end
  end

  defp resolve_subscriber_opts(opts) do
    case Keyword.pop(opts, :connection) do
      {nil, rest} ->
        {:ok, rest}

      {conn, _rest} ->
        try do
          Connection.address(conn)
          |> case do
            {:ok, host, port} -> {:ok, [host: host, port: port]}
            error -> error
          end
        catch
          :exit, {:noproc, _} -> {:error, :not_connected}
          :exit, reason -> {:error, {:connection_error, reason}}
        end
    end
  end

  defp register_process(sub_pid, table) do
    with :ok <- Subscriber.add_process(sub_pid, table) do
      Registry.unregister(ExeQute.Registry, {sub_pid, table})
      Registry.register(ExeQute.Registry, {sub_pid, table}, nil)
      :ok
    end
  end

  defp resolve_pid(pid) when is_pid(pid), do: pid

  defp resolve_pid(name) when is_atom(name) do
    GenServer.whereis(name) ||
      raise ArgumentError, "No process registered under the name #{inspect(name)}"
  end

  @spec format_result({:ok, term()} | {:error, term()}, table_format()) ::
          {:ok, term()} | {:error, term()}
  defp format_result({:ok, result}, :maps), do: {:ok, result |> to_maps() |> unwrap()}
  defp format_result(tagged, _format), do: tagged

  defp unwrap([single]) when is_list(single) or is_map(single), do: single
  defp unwrap(other), do: other

  defp to_maps(%{columns: cols, rows: rows}) do
    Enum.map(rows, fn row -> Enum.zip(cols, row) |> Map.new() end)
  end

  defp to_maps(list) when is_list(list), do: Enum.map(list, &to_maps/1)

  defp to_maps(%{} = map) when not is_struct(map) do
    case Map.keys(map) do
      [%{columns: _, rows: _}] ->
        [{key_table, value_table}] = Map.to_list(map)
        Enum.zip_with(to_maps(key_table), to_maps(value_table), &Map.merge/2)

      _ ->
        Map.new(map, fn {k, v} -> {k, to_maps(v)} end)
    end
  end

  defp to_maps(value), do: value

  defp to_q_literal(s) when is_binary(s) do
    escaped = s |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")
    "\"" <> escaped <> "\""
  end

  defp to_q_literal(n) when is_integer(n), do: Integer.to_string(n)
  defp to_q_literal(f) when is_float(f), do: Float.to_string(f)
  defp to_q_literal(true), do: "1b"
  defp to_q_literal(false), do: "0b"
  defp to_q_literal(a) when is_atom(a), do: "`" <> Atom.to_string(a)

  defp to_q_literal(%Date{year: y, month: m, day: d}) do
    "#{y}.#{zero_pad(m)}.#{zero_pad(d)}"
  end

  defp to_q_literal(%Time{hour: h, minute: min, second: s, microsecond: {us, _}}) do
    ms = div(us, 1_000)
    "#{zero_pad(h)}:#{zero_pad(min)}:#{zero_pad(s)}.#{String.pad_leading(Integer.to_string(ms), 3, "0")}"
  end

  defp to_q_literal(%DateTime{} = dt) do
    %{year: y, month: mo, day: d, hour: h, minute: min, second: s, microsecond: {us, _}} = dt
    ns = us * 1_000
    "#{y}.#{zero_pad(mo)}.#{zero_pad(d)}D#{zero_pad(h)}:#{zero_pad(min)}:#{zero_pad(s)}.#{String.pad_leading(Integer.to_string(ns), 9, "0")}"
  end

  defp to_q_literal(list) when is_list(list) do
    "(" <> Enum.map_join(list, ";", &to_q_literal/1) <> ")"
  end

  defp zero_pad(n), do: String.pad_leading(Integer.to_string(n), 2, "0")

  defp safe_call(_conn, fun) do
    try do
      fun.()
    catch
      :exit, {:noproc, _} -> {:error, :not_connected}
      :exit, {:timeout, _} -> {:error, :timeout}
      :exit, reason -> {:error, {:connection_error, reason}}
    end
  end

  defp safe_stop(conn) do
    try do
      GenServer.stop(conn, :normal, 1000)
    catch
      :exit, _ -> :ok
    end
  end
end
