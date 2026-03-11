# ExeQute

An Elixir client for [KDB+](https://kx.com/products/kdb/), the high-performance time-series database used in financial markets. ExeQute handles the KDB+ IPC wire protocol, type system, and pub/sub — letting you query and subscribe to KDB+ instances from any Elixir application.

The name is a play on **Ex**ir + **Q** (the KDB+ query language) + e**xecute**.

## Installation

```elixir
def deps do
  [
    {:exe_qute, "~> 0.1.0"}
  ]
end
```

## Querying

### One-shot query

Opens a connection, runs the query, closes the connection. Good for infrequent or one-off queries.

```elixir
{:ok, result} = ExeQute.query("select from trade", host: "kdb-host", port: 5010)
```

### Persistent connection

```elixir
{:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010)

{:ok, result} = ExeQute.query(conn, "select from trade")
{:ok, result} = ExeQute.query(conn, "select from trade where sym=`AAPL")

ExeQute.disconnect(conn)
```

### Named connections

Register a connection under an atom so any part of your application can use it without passing the pid around.

```elixir
ExeQute.connect(host: "kdb-host", port: 5010, name: :trades)

{:ok, result} = ExeQute.query(:trades, "select from trade")
```

### Parameterized queries

Pass typed arguments directly rather than interpolating them into strings. Arguments are encoded as KDB+ types on the wire.

```elixir
{:ok, result} = ExeQute.query(conn, "{x + y}", [1, 2])
{:ok, result} = ExeQute.query(conn, ".myns.getquotes", ["USD/JPY", ~D[2024-01-01]])
```

### Publishing (fire-and-forget)

Send data to a KDB+ function asynchronously — no response is expected. Useful for writing to feed handlers or triggering side-effects.

```elixir
:ok = ExeQute.publish(conn, ".feed.upd", ["trade", rows])
```

## Connection options

| Option | Default | Description |
|---|---|---|
| `:host` | `"localhost"` | KDB+ server hostname or IP |
| `:port` | `5001` | KDB+ server port |
| `:username` | `nil` | Username (omit for unauthenticated servers) |
| `:password` | `nil` | Password |
| `:timeout` | `5000` | Connection and query timeout in ms |
| `:encoding` | `"utf8"` | Character encoding for string data |
| `:name` | `nil` | Register connection under this atom name |

## Type mapping

### Decoding (KDB+ → Elixir)

| KDB+ type | Elixir type |
|---|---|
| boolean | `true` / `false` |
| short, int, long | `integer()` |
| real, float | `float()` |
| char | `<<byte>>` |
| symbol | `String.t()` |
| timestamp | `%DateTime{}` (UTC, microsecond precision) |
| date | `%Date{}` |
| time, minute, second | `%Time{}` |
| timespan | `integer()` (nanoseconds) |
| list | `[term()]` |
| dictionary | `%{term() => term()}` |
| table | `[%{String.t() => term()}]` (list of row maps) |
| keyed table | `[%{String.t() => term()}]` (key and value columns merged) |
| null values | `nil` |
| infinity (`0Wf`, `-0Wf`) | `:infinity` / `:neg_infinity` |

### Encoding (Elixir → KDB+)

| Elixir value | KDB+ type |
|---|---|
| `true` / `false` | boolean |
| `integer()` | long (64-bit) |
| `float()` | float (64-bit) |
| `String.t()` | char vector |
| `atom()` | symbol (e.g. `:AAPL` → `` `AAPL ``) |
| `%DateTime{}` | timestamp |
| `%Date{}` | date |
| `%Time{}` | time (e.g. `~T[17:00:00]`) |
| `[term()]` | generic list |
| `%{term() => term()}` | dictionary |

Types not listed (char atom, short/int atoms, timespan, minute, second, guid) cannot be sent as typed parameters. Use an inline q expression string for those cases.

## Pub/Sub

ExeQute supports KDB+ tickerplant subscriptions. One TCP connection to the tickerplant is shared across any number of subscribing processes in your application.

### Process-based subscriptions

Each subscribing process receives `{:exe_qute, table, data}` messages in its own mailbox. This works naturally in LiveViews, GenServers, or any `handle_info`-capable process.

```elixir
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
  def handle_info({:exe_qute, "trade", data}, state) do
    IO.inspect(data, label: "trade")
    {:noreply, state}
  end

  def handle_info({:exe_qute, "quote", data}, state) do
    IO.inspect(data, label: "quote")
    {:noreply, state}
  end
end

{:ok, _pid} = MyApp.TradeHandler.start_link([])
MyApp.TradeHandler.subscribe()

Process.sleep(30_000)

MyApp.TradeHandler.unsubscribe()
```

### Named subscriber

For applications that want explicit control over the subscriber lifecycle — for example, to start it in a supervision tree.

```elixir
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

ExeQute.Subscriber.start_link(host: "tp-host", port: 5010, name: :tp)
{:ok, _pid} = MyApp.TradeHandler.start_link([])
MyApp.TradeHandler.subscribe()

Process.sleep(30_000)

MyApp.TradeHandler.unsubscribe()
```

### Callback-based subscriptions

For cases where you want a function called directly rather than receiving mailbox messages.

```elixir
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
```

### Multiple subscribers, one connection

Multiple processes can subscribe to the same table. Only one `.u.sub` is sent to the tickerplant regardless of how many local processes subscribe. When the last subscriber unsubscribes, `.u.unsub` is sent automatically.

```elixir
# All three processes receive {:exe_qute, "trade", data} independently
ExeQute.subscribe(:tp, "trade")   # from LiveView A
ExeQute.subscribe(:tp, "trade")   # from LiveView B
ExeQute.subscribe(:tp, "trade")   # from a GenServer
```

### Starting the subscriber in a supervision tree

```elixir
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
```

## Introspection

KDB+ instances often accumulate years of q functions across many namespaces. ExeQute lets you explore a live instance programmatically — useful for building admin dashboards, documentation generators, or dynamic query builders that adapt to whatever functions a given server exposes.

Results are cached per-connection after the first call, so repeated introspection is free. The cache is tied to the connection process and clears automatically when it dies. Call `ExeQute.refresh_introspection/1` to force a fresh fetch after deploying new code to KDB+.

### Namespaces

```elixir
{:ok, ns} = ExeQute.namespaces(conn)
#=> [".myns", ".feed", ".dash", ".q", ".Q", ".h"]
```

### Functions

Returns each function's name, parameter list, and source body. The body is the verbatim q source — useful for displaying what a function does without leaving Elixir, or for building lightweight documentation tooling around a KDB+ instance.

```elixir
{:ok, fns} = ExeQute.functions(conn, ".dash")
#=> [
#=>   %{
#=>     "name"   => ".dash.getquotes",
#=>     "params" => ["sym", "start", "end"],
#=>     "body"   => "{[sym;start;end] select from quote where sym=sym, date within (start;end)}"
#=>   },
#=>   %{
#=>     "name"   => ".dash.lasttrade",
#=>     "params" => ["sym"],
#=>     "body"   => "{[sym] last select from trade where sym=sym}"
#=>   }
#=> ]
```

Omit the namespace argument to list functions in the root namespace:

```elixir
{:ok, fns} = ExeQute.functions(conn)
```

### Variables and tables

```elixir
{:ok, vars}   = ExeQute.variables(conn, ".myns")
{:ok, tables} = ExeQute.tables(conn)
```

### Refreshing the cache

```elixir
ExeQute.refresh_introspection(conn)
```

## Livebook integration (work in progress)

### Interactive explorer

`ExeQute.Explorer` renders a QStudio-style widget inside a Livebook cell — connect to a server, browse namespaces, inspect tables and functions, run ad-hoc queries, and see results as tables or charts, all without leaving the notebook.

```elixir
ExeQute.Explorer.new(host: "kdb-host", port: 5010)
```

Results can be captured and used in subsequent cells:

```elixir
ExeQute.Explorer.new()
# ... interact in the UI, assign result to "my_data" ...

my_data = ExeQute.Explorer.get("my_data")
```

### Live chart widget

`ExeQute.EChart` is a low-overhead [Apache ECharts](https://echarts.apache.org) widget for Livebook, designed for high-frequency streaming data. It is the rendering backend used by the **KDB+ Chart** smart cell and can be driven directly when building custom subscription callbacks.

```elixir
chart = ExeQute.EChart.new(height: 400)
ExeQute.EChart.render(chart, initial_options)

ExeQute.subscribe(:tp, "trade", fn {_table, raw} ->
  rows = ExeQute.to_rows(raw)
  cfg = %{x_field: "time", x_type: :temporal, y_field: "price",
          y_type: :quantitative, color_field: "", chart_type: :line, window: 500}
  {buf, _} = ExeQute.EChart.update_buffer({[], %{}}, rows, cfg)
  ExeQute.EChart.push(chart, ExeQute.EChart.options_from_buffer(cfg, {buf, %{}}))
end)
```

> **Note:** Both modules are functional but not yet fully polished — APIs may change in future releases.

## Error handling

All public functions return tagged tuples — no exceptions reach your code under normal operation.

```elixir
case ExeQute.query(:trades, "select from trade") do
  {:ok, result}            -> handle(result)
  {:error, :not_connected} -> reconnect()
  {:error, :timeout}       -> retry()
  {:error, reason}         -> Logger.error(inspect(reason))
end
```

Errors returned by KDB+ itself (e.g. `'type`) are returned as `{:error, {:kdb_error, "type"}}`.
