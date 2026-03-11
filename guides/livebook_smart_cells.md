# Livebook Smart Cells

ExeQute ships four Livebook smart cells that appear in the `+Smart` dropdown
once the package is loaded in a notebook. They require `kino`, `vega_lite`,
and `kino_vega_lite` to be available.

```elixir
Mix.install([
  {:exe_qute, "~> 0.1"},
  {:kino, "~> 0.14"},
  {:vega_lite, "~> 0.1"},
  {:kino_vega_lite, "~> 0.1"}
])
```

---

## KDB+ Connection

Creates a persistent connection to a KDB+ instance and binds it to a
Livebook variable.

**Fields**

| Field | Description |
|---|---|
| Variable | Name of the Elixir variable the connection pid is bound to |
| Host | KDB+ server hostname or IP |
| Port | TCP port |
| Username | Optional; leave blank for unauthenticated servers |
| Password secret | Livebook secret name (without the `LB_` prefix) |

**Generated code**

```elixir
{:ok, conn} = ExeQute.connect(host: "localhost", port: 5001)
```

With credentials:

```elixir
{:ok, conn} = ExeQute.connect(
  host: "tp-host",
  port: 9600,
  username: "trader",
  password: System.fetch_env!("LB_KDB_PASS")
)
```

The password is never stored in the notebook — it is read from the Livebook
secrets store at evaluation time.

---

## KDB+ Query

Runs a q expression against a connection and displays the result as a tabbed
widget with **Table**, **Tree**, and **Raw** views.

**Fields**

| Field | Description |
|---|---|
| Name | Variable the result is bound to |
| Connection | Connection variable discovered from the current notebook scope |
| Namespace | KDB+ namespace for the table/function browser (`.` for root) |
| Row limit | Passed as `select[N] from table` at the KDB+ level |
| Q expression | Any valid q expression |

**Sidebar browser**

The left sidebar lists all tables and functions in the selected namespace.
Clicking a table populates the query area with `select[N] from tablename`.
Clicking a function populates it with `funcname[]`.

**Generated code**

```elixir
{:ok, result} = ExeQute.query(conn, "select from trade")
ExeQute.display(result, "select from trade")
```

`ExeQute.display/2` renders a `Kino.Layout.tabs` widget. The **Table** tab
uses `Kino.DataTable` and is shown when the result is a list of maps. The
**Tree** tab uses `Kino.Tree`. **Raw** always shows the inspected value.

---

## KDB+ Subscribe

Opens a long-lived tickerplant subscriber connection and binds it to a
variable. The subscriber is started via `ExeQute.Subscriber.start_or_find/1`,
which means re-evaluating the cell reuses the existing TCP connection rather
than opening a new one.

**Fields**

| Field | Description |
|---|---|
| Variable | Name bound to the subscriber pid |
| Host | Tickerplant hostname or IP |
| Port | Tickerplant TCP port |
| Username | Optional |
| Password secret | Livebook secret name (without the `LB_` prefix) |

**Generated code**

```elixir
{:ok, tp} = ExeQute.Subscriber.start_or_find(host: "tp-host", port: 9600)
```

Pass the variable to a **KDB+ Chart** cell or use it directly with
`ExeQute.subscribe/3`:

```elixir
{:ok, ref} = ExeQute.subscribe(tp, "trade", fn {_table, raw} ->
  rows = ExeQute.to_rows(raw)
  IO.inspect(rows)
end)
```

---

## KDB+ Chart

Subscribes to a tickerplant table and renders a live-updating
`Kino.VegaLite` chart alongside a `Kino.DataTable`. Both update on every
push from the tickerplant.

**Fields**

| Field | Description |
|---|---|
| Variable | Name bound to the output `Kino.Layout.tabs` widget |
| Subscriber | Subscriber variable from a **KDB+ Subscribe** cell |
| Table | KDB+ table name to subscribe to |
| Symbols | Comma-separated symbol filter; leave blank for all |
| Window | Maximum number of data points to keep in the chart |
| X field / X type | Column name and Vega-Lite type for the x-axis |
| Y field / Y type | Column name and Vega-Lite type for the y-axis |
| Color field | Optional column for colour encoding (e.g. `sym`) |
| Chart type | `line`, `point`, or `bar` |

**Tabs**

- **Chart** — live `Kino.VegaLite` updating on every tickerplant message
- **Data** — `Kino.DataTable` showing the most recent rows; useful for
  confirming data is arriving before the chart axes are tuned

**Changing parameters without re-evaluating**

All visual parameters (x/y field, type, chart type, color field, window) can
be changed directly in the smart cell UI after the cell has been evaluated.
The change takes effect on the next data tick — the subscription is not
interrupted. Re-evaluation is only required when the subscriber, table, or
symbol filter changes.

**X type behaviour**

| X type | Buffer strategy |
|---|---|
| `temporal` / `quantitative` | Rolling window — last N rows appended |
| `nominal` / `ordinal` | Latest-per-key — one row per distinct x value, updated on each tick (suitable for bar charts showing current value per symbol) |

**Generated code**

```elixir
{:ok, chart} =
  ExeQute.subscribe(tp, "trade", ["AAPL", "MSFT"], fn {_table, raw} ->
    rows = ExeQute.to_rows(raw)
    ...
  end)
```

The full generated code is more involved; it sets up the VegaLite spec,
a `Kino.Frame` for live re-rendering, an `Agent` buffer, and an ETS config
table that the smart cell server writes to when you change UI fields.

---

## Typical notebook layout

A minimal Livebook notebook for live KDB+ monitoring looks like this:

```
[KDB+ Connection]   → binds `conn`
[KDB+ Subscribe]    → binds `tp`
[KDB+ Query]        → ad-hoc exploration using `conn`
[KDB+ Chart]        → live chart from `tp`, table "trade", x: time, y: price
[KDB+ Chart]        → live chart from `tp`, table "bbo",   x: sym,  y: bid
```

The connection and subscriber cells only need to be evaluated once per
session. Chart cells can be re-evaluated to change the subscription target;
visual parameters can be adjusted in the cell UI at any time without
interrupting the data feed.
