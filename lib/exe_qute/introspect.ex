defmodule ExeQute.Introspect do
  @moduledoc """
  Live introspection of a KDB+ instance's namespaces, tables, functions, and variables.

  All four functions are available as top-level delegates on `ExeQute` — that is
  the recommended entry point for most callers:

      {:ok, namespaces} = ExeQute.namespaces(conn)
      {:ok, tables}     = ExeQute.tables(conn, ".myns")
      {:ok, fns}        = ExeQute.functions(conn, ".myns")
      {:ok, vars}       = ExeQute.variables(conn, ".myns")

  See `ExeQute.namespaces/1`, `ExeQute.tables/2`, `ExeQute.functions/2`, and
  `ExeQute.variables/2` for full documentation including examples.

  ## Caching

  Results are cached inside the connection process after the first call. Repeated
  calls for the same namespace key are free — no round-trip to KDB+. The cache is
  scoped to the connection and cleared automatically when the connection stops.

  Call `ExeQute.refresh_introspection/1` to invalidate the cache and force a
  fresh fetch, for example after deploying new q code:

      ExeQute.refresh_introspection(conn)

  ## Typical workflow

      {:ok, conn} = ExeQute.connect(host: "kdb-host", port: 5010)

      {:ok, namespaces} = ExeQute.namespaces(conn)
      # [".myns", ".feed", ".q", ".Q", ".h"]

      {:ok, tables} = ExeQute.tables(conn, ".myns")
      # ["trade", "quote"]

      {:ok, fns} = ExeQute.functions(conn, ".myns")
      # [%{"name" => ".myns.getquotes", "params" => ["sym", "date"], "body" => "{...}"}]

      name   = hd(fns)["name"]
      params = hd(fns)["params"]
      # Call it with typed arguments:
      {:ok, result} = ExeQute.query(conn, name, ["EUR/USD", ~D[2024-01-01]])
  """

  alias ExeQute.Connection

  @doc """
  Lists all namespaces defined on the KDB+ instance.

  Returns namespace names prefixed with `.` (e.g. `".myns"`, `".q"`, `".Q"`).

  ## Examples

      {:ok, ns} = ExeQute.namespaces(conn)
      #=> {:ok, [".q", ".Q", ".h", ".myns"]}

  """
  @spec namespaces(pid() | atom()) :: {:ok, [String.t()]} | {:error, term()}
  def namespaces(conn) do
    case cached(conn, :namespaces, ~s|key `|) do
      {:ok, names} -> {:ok, Enum.map(names, &("." <> &1))}
      error -> error
    end
  end

  @doc """
  Lists functions in a namespace along with their parameter names.

  Pass a namespace string such as `".myns"` or omit/`nil` for the root namespace.

  Returns a list of maps with:

  - `"name"` — fully qualified function name
  - `"params"` — list of parameter name strings; empty for zero-arity or non-lambda values
  - `"body"` — function source code as a string; empty string for non-lambda values

  ## Examples

      {:ok, fns} = ExeQute.functions(conn)
      {:ok, fns} = ExeQute.functions(conn, ".myns")
      #=> {:ok, [
      #=>   %{"name" => ".myns.getquotes", "params" => ["sym", "start", "end"], "body" => "{[sym;start;end] ...}"},
      #=>   %{"name" => ".myns.upd",       "params" => ["t", "x"],             "body" => "{[t;x] ...}"},
      #=>   %{"name" => ".myns.init",      "params" => [],                     "body" => "{[] ...}"},
      #=> ]}

  """
  @spec functions(pid() | atom(), String.t() | nil) :: {:ok, [map()]} | {:error, term()}
  def functions(conn, namespace \\ nil)

  def functions(conn, nil) do
    q = ~s|{k:system"f";flip`name`params`body!(k;{@[{(value get x)1};x;`$()]}each k;{@[{string get x};x;""]}each k)}[]|
    case cached(conn, :functions_root, q) do
      {:ok, table} -> {:ok, table |> table_to_maps() |> Enum.map(&normalize_function(".", &1))}
      error -> error
    end
  end

  def functions(conn, ns) when is_binary(ns) do
    q = ~s|{ns:`$x;k:` sv'ns,'system"f ",x;flip`name`params`body!(k;{@[{(value get x)1};x;`$()]}each k;{@[{string get x};x;""]}each k)}["#{ns}"]|
    case cached(conn, {:functions, ns}, q) do
      {:ok, table} -> {:ok, table |> table_to_maps() |> Enum.map(&normalize_function(ns, &1))}
      error -> error
    end
  end

  @doc """
  Lists variable names in a namespace.

  Pass a namespace string such as `".myns"` or omit/`nil` for the root namespace.

  ## Examples

      {:ok, vars} = ExeQute.variables(conn)
      {:ok, vars} = ExeQute.variables(conn, ".myns")
      #=> {:ok, ["bidSize", "askSize"]}

  """
  @spec variables(pid() | atom(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  def variables(conn, namespace \\ nil)

  def variables(conn, nil), do: cached(conn, :variables_root, ~s|system "v"|)
  def variables(conn, ns) when is_binary(ns), do: cached(conn, {:variables, ns}, ~s|system "v #{ns}"|)

  @doc """
  Lists table names in a namespace.

  Pass a namespace string such as `".myns"` or omit/`nil` for the root namespace.

  ## Examples

      {:ok, tables} = ExeQute.tables(conn)
      {:ok, tables} = ExeQute.tables(conn, ".myns")
      #=> {:ok, ["trade", "quote"]}

  """
  @spec tables(pid() | atom(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  def tables(conn, namespace \\ nil)

  def tables(conn, nil), do: cached(conn, :tables_root, ~s|system "a"|)
  def tables(conn, ns) when is_binary(ns), do: cached(conn, {:tables, ns}, ~s|system "a #{ns}"|)

  defp cached(conn, key, query) do
    try do
      Connection.cached_query(conn, key, query)
    catch
      :exit, {:noproc, _} -> {:error, :not_connected}
      :exit, {:timeout, _} -> {:error, :timeout}
      :exit, reason -> {:error, {:connection_error, reason}}
    end
  end

  defp normalize_function(ns, %{"name" => name, "params" => params} = fn_map) do
    %{fn_map | "name" => normalize_name(ns, name), "params" => normalize_params(params)}
  end

  defp normalize_name(ns, name) do
    expected_prefix = ns <> "."
    double_prefix = ns <> ns <> "."

    cond do
      String.starts_with?(name, double_prefix) -> ns <> "." <> String.slice(name, String.length(double_prefix)..-1//1)
      String.starts_with?(name, expected_prefix) -> name
      true -> expected_prefix <> name
    end
  end

  defp normalize_params(nil), do: []
  defp normalize_params(""), do: []
  defp normalize_params(s) when is_binary(s), do: [s]
  defp normalize_params(list) when is_list(list), do: Enum.reject(list, &(&1 == "" or is_nil(&1)))

  defp table_to_maps(%{columns: cols, rows: rows}) do
    Enum.map(rows, fn row -> Enum.zip(cols, row) |> Map.new() end)
  end

  defp table_to_maps(other), do: other
end
