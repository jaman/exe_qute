# Quick Start

## Installation

Add ExeQute to your `mix.exs` deps:

```elixir
def deps do
  [
    {:exe_qute, "~> 0.1.0"}
  ]
end
```

For Livebook smart cells, also add:

```elixir
{:kino, "~> 0.14"},
{:vega_lite, "~> 0.1"},
{:kino_vega_lite, "~> 0.1"}
```

## Connect and query

```elixir
{:ok, conn} = ExeQute.connect(host: "localhost", port: 5001)

{:ok, rows} = ExeQute.query(conn, "select from trade")
# => [%{"sym" => "AAPL", "price" => 182.5, "size" => 100}, ...]

ExeQute.disconnect(conn)
```

Results from table queries are returned as a list of maps with string keys,
one map per row. Scalar results are returned as-is.

## Named connections

Register a connection under an atom to share it across your application
without passing pids around:

```elixir
ExeQute.connect(host: "localhost", port: 5001, name: :rdb)

{:ok, rows} = ExeQute.query(:rdb, "select from trade")
```

## Parameterized queries

Pass typed arguments directly rather than interpolating strings.
Arguments are encoded as KDB+ types on the wire:

```elixir
{:ok, result} = ExeQute.query(conn, ".myns.getquotes", ["EUR/USD", ~D[2024-01-01]])
{:ok, result} = ExeQute.query(conn, "{x + y}", [10, 20])
```

## Subscribe to a tickerplant

```elixir
{:ok, tp} = ExeQute.Subscriber.start_or_find(host: "tp-host", port: 9600)

{:ok, ref} = ExeQute.subscribe(tp, "trade", fn {_table, raw} ->
  rows = ExeQute.to_rows(raw)
  IO.inspect(rows)
end)

ExeQute.unsubscribe(tp, ref)
```

`ExeQute.to_rows/1` converts the raw columnar wire format into a list of
row maps, the same shape as query results.

## Publish (fire-and-forget)

Send data to a KDB+ function without waiting for a reply:

```elixir
:ok = ExeQute.publish(conn, ".u.upd", ["trade", rows])
```

## Error handling

All public functions return `{:ok, value}` or `{:error, reason}`:

```elixir
case ExeQute.query(:rdb, "select from trade") do
  {:ok, rows}              -> process(rows)
  {:error, :not_connected} -> reconnect()
  {:error, :timeout}       -> retry_later()
  {:error, reason}         -> Logger.error(inspect(reason))
end
```

KDB+ errors (e.g. `'type`) come back as `{:error, {:kdb_error, "type"}}`.
