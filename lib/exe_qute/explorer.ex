defmodule ExeQute.Explorer do
  @moduledoc """
  Interactive KDB+ explorer widget for Livebook.

  > #### Work in progress {: .warning}
  >
  > This module is functional but not yet fully polished. The API and behaviour
  > may change in future releases.

  Aims to provide a QStudio-style interface directly inside Livebook: connect to
  a server, browse namespaces, inspect tables and functions, run ad-hoc queries,
  and visualise results — all from a single `ExeQute.Explorer.new/1` call.

  ## Requirements

  Add to your Livebook notebook's Mix section:

      {:kino, "~> 0.14"},
      {:vega_lite, "~> 0.1"},
      {:kino_vega_lite, "~> 0.1"}

  ## Usage

      ExeQute.Explorer.new()

  For a Livebook secret password, pass it directly:

      ExeQute.Explorer.new(password: System.fetch_env!("LB_KDB_PASSWORD"))
  """

  alias VegaLite, as: Vl

  @store :exe_qute_explorer_results

  @doc """
  Retrieves a query result previously stored by the Explorer widget.

  Call this in a subsequent Livebook cell using the variable name you entered
  in the "Assign to" field:

      my_data = ExeQute.Explorer.get("my_data")

  Returns `nil` if no result has been stored under that name.
  """
  @spec get(String.t()) :: term()
  def get(name) when is_binary(name) do
    ensure_store()

    case :ets.lookup(@store, name) do
      [{^name, value}] -> value
      [] -> nil
    end
  end

  defp ensure_store do
    if :ets.whereis(@store) == :undefined do
      :ets.new(@store, [:named_table, :public, :set])
    end
  end

  defp store_result("", _value), do: :ok

  defp store_result(name, value) do
    ensure_store()
    :ets.insert(@store, {name, value})
  end

  @doc """
  Renders the KDB+ explorer widget. Call this in a Livebook cell.

  ## Options

  - `:host` — pre-fill host field
  - `:port` — pre-fill port field
  - `:username` — pre-fill username field
  - `:password` — pre-fill password from a secret (not shown in UI)
  """
  @spec new(keyword()) :: Kino.Layout.t()
  def new(opts \\ []) do
    conn_form = Kino.Control.form(
      [
        host: Kino.Input.text("Host", default: Keyword.get(opts, :host, "localhost")),
        port: Kino.Input.text("Port", default: to_string(Keyword.get(opts, :port, 5001))),
        user: Kino.Input.text("Username", default: Keyword.get(opts, :username, "")),
        pass: Kino.Input.password("Password")
      ],
      submit: "Connect",
      reset_on_submit: []
    )

    status_frame = Kino.Frame.new()
    explorer_frame = Kino.Frame.new()

    secret_pass = Keyword.get(opts, :password)

    Kino.listen(conn_form, fn %{data: %{host: host, port: port, user: user, pass: form_pass}} ->
      pass = if secret_pass && secret_pass != "", do: secret_pass, else: nilify(form_pass)
      user = nilify(user)

      cred_opts = if user, do: [username: user, password: pass], else: []
      conn_opts = [host: host, port: port] ++ cred_opts

      case ExeQute.connect(conn_opts) do
        {:ok, conn} ->
          Kino.Frame.render(status_frame, Kino.Markdown.new("✅ Connected to `#{host}:#{port}`"))
          render_namespace(explorer_frame, conn, ".")

        {:error, reason} ->
          Kino.Frame.render(status_frame, Kino.Markdown.new("❌ `#{inspect(reason)}`"))
      end
    end)

    Kino.Layout.grid([conn_form, status_frame, explorer_frame])
  end

  defp render_namespace(frame, conn, ns) do
    ns_list = ["."] ++ ok_or(ExeQute.namespaces(conn), [])
    tables = ok_or(ExeQute.tables(conn, ns), [])
    functions = ok_or(ExeQute.functions(conn, ns), [])
    variables = ok_or(ExeQute.variables(conn, ns), [])

    ns_options = Enum.map(ns_list, &{&1, &1})
    ns_default = if Enum.any?(ns_list, &(&1 == ns)), do: ns, else: hd(ns_list)
    ns_select = Kino.Input.select("Namespace", ns_options, default: ns_default)
    result_frame = Kino.Frame.new()

    Kino.listen(ns_select, fn %{value: new_ns} ->
      render_namespace(frame, conn, new_ns)
    end)

    tabs = Kino.Layout.tabs([
      {"Tables (#{length(tables)})", tables_panel(conn, tables, result_frame)},
      {"Functions (#{length(functions)})", functions_panel(conn, functions, result_frame)},
      {"Variables (#{length(variables)})", variables_panel(variables)},
      {"Query", query_panel(conn, result_frame)}
    ])

    Kino.Frame.render(frame, Kino.Layout.grid([ns_select, tabs, result_frame]))
  end

  defp tables_panel(_conn, [], _result_frame) do
    Kino.Markdown.new("_No tables in this namespace._")
  end

  defp tables_panel(conn, tables, result_frame) do
    form = Kino.Control.form(
      [
        table: Kino.Input.select("Table", Enum.map(tables, &{&1, &1})),
        limit: Kino.Input.number("Row limit", default: 1000),
        var_name: Kino.Input.text("Assign to", default: "")
      ],
      submit: "Fetch"
    )

    Kino.listen(form, fn %{data: %{table: table, limit: limit, var_name: var_name}} ->
      q = if limit && limit > 0, do: "#{limit} sublist select from #{table}", else: "select from #{table}"
      run_and_render(conn, q, String.trim(var_name), result_frame)
    end)

    form
  end

  defp functions_panel(_conn, [], _result_frame) do
    Kino.Markdown.new("_No functions in this namespace._")
  end

  defp functions_panel(conn, functions, result_frame) do
    func_opts = Enum.map(functions, &{&1["name"], &1["name"]})
    func_select = Kino.Input.select("Function", func_opts)
    params_frame = Kino.Frame.new()

    render_func_form(params_frame, hd(functions), conn, result_frame)

    Kino.listen(func_select, fn %{value: fname} ->
      func = Enum.find(functions, &(&1["name"] == fname))
      render_func_form(params_frame, func, conn, result_frame)
    end)

    Kino.Layout.grid([func_select, params_frame])
  end

  defp render_func_form(frame, func, conn, result_frame) do
    params = func["params"] || []
    body = String.slice(func["body"] || "", 0, 300)

    body_md =
      if body != "",
        do: "\n\n**Body:**\n```q\n#{body}\n```",
        else: ""

    info = Kino.Markdown.new("**#{func["name"]}** — params: #{param_signature(params)}#{body_md}")

    if params == [] do
      form = Kino.Control.form(
        [var_name: Kino.Input.text("Assign to", default: "")],
        submit: "Call #{func["name"]}[]"
      )

      Kino.listen(form, fn %{data: %{var_name: var_name}} ->
        run_and_render(conn, "#{func["name"]}[]", String.trim(var_name), result_frame)
      end)

      Kino.Frame.render(frame, Kino.Layout.grid([info, form]))
    else
      param_inputs =
        Enum.map(params, &{String.to_atom(&1), Kino.Input.text(&1)}) ++
          [{:var_name, Kino.Input.text("Assign to", default: "")}]

      form = Kino.Control.form(param_inputs, submit: "Call #{func["name"]}")

      Kino.listen(form, fn %{data: data} ->
        var_name = data |> Map.fetch!(:var_name) |> String.trim()

        args =
          params
          |> Enum.map(&Map.fetch!(data, String.to_atom(&1)))
          |> Enum.map(&String.trim/1)

        empty = Enum.filter(args, &(&1 == ""))

        if empty != [] do
          Kino.Frame.render(result_frame, Kino.Markdown.new("❌ All parameters are required."))
        else
          q = "#{func["name"]}[#{Enum.join(args, ";")}]"
          run_and_render(conn, q, var_name, result_frame)
        end
      end)

      Kino.Frame.render(frame, Kino.Layout.grid([info, form]))
    end
  end

  defp param_signature([]), do: "_none_"

  defp param_signature(params) do
    params |> Enum.map_join(", ", &"`#{&1}`")
  end

  defp variables_panel([]) do
    Kino.Markdown.new("_No variables in this namespace._")
  end

  defp variables_panel(variables) do
    Kino.Markdown.new(Enum.map_join(variables, "\n", &"- `#{&1}`"))
  end

  defp query_panel(conn, result_frame) do
    form = Kino.Control.form(
      [
        query: Kino.Input.textarea("Q expression"),
        var_name: Kino.Input.text("Assign to", default: "")
      ],
      submit: "Run"
    )

    Kino.listen(form, fn %{data: %{query: q, var_name: var_name}} ->
      q = String.trim(q)

      if q == "" do
        Kino.Frame.render(result_frame, Kino.Markdown.new("_Enter a query above._"))
      else
        run_and_render(conn, q, String.trim(var_name), result_frame)
      end
    end)

    form
  end

  defp run_and_render(conn, query, var_name, frame) do
    Kino.Frame.render(frame, Kino.Markdown.new("_Running…_"))

    case ExeQute.query(conn, query) do
      {:ok, [%{} | _] = maps} ->
        store_result(var_name, maps)
        render_table_result(maps, query, var_name, frame)

      {:ok, []} ->
        store_result(var_name, [])
        Kino.Frame.render(frame, Kino.Markdown.new("_Empty result._" <> var_hint(var_name)))

      {:ok, result} ->
        store_result(var_name, result)

        Kino.Frame.render(
          frame,
          Kino.Markdown.new(
            "```elixir\n#{inspect(result, pretty: true, limit: 500)}\n```" <> var_hint(var_name)
          )
        )

      {:error, reason} ->
        Kino.Frame.render(frame, Kino.Markdown.new("❌ `#{inspect(reason)}`"))
    end
  end

  defp var_hint(""), do: ""

  defp var_hint(name) do
    "\n\n_Stored as **`#{name}`** — retrieve with:_\n```elixir\n#{name} = ExeQute.Explorer.get(\"#{name}\")\n```"
  end

  defp render_table_result(maps, query, var_name, frame) do
    exploded = explode_rows(maps)
    was_exploded = exploded != maps

    hint = if var_name != "", do: [Kino.Markdown.new(var_hint(var_name))], else: []

    raw_tab = Kino.Layout.tabs([
      {"Table", Kino.DataTable.new(maps, name: query)}
    ])

    if was_exploded do
      flat_maps = exploded
      cols = flat_maps |> hd() |> Map.keys()
      flat_dt = Kino.DataTable.new(flat_maps, name: "#{query} (flattened)")

      chart_widgets =
        case chart_widget(flat_maps, cols) do
          nil -> []
          chart -> [chart]
        end

      flat_tab = Kino.Layout.tabs([{"Flattened", flat_dt}])

      Kino.Frame.render(frame, Kino.Layout.grid(hint ++ [raw_tab, flat_tab] ++ chart_widgets))
    else
      cols = maps |> hd() |> Map.keys()

      chart_widgets =
        case chart_widget(maps, cols) do
          nil -> []
          chart -> [chart]
        end

      Kino.Frame.render(frame, Kino.Layout.grid(hint ++ [raw_tab] ++ chart_widgets))
    end
  end

  defp explode_rows(maps) do
    list_cols =
      maps
      |> hd()
      |> Enum.flat_map(fn {k, v} -> if is_list(v), do: [k], else: [] end)

    if list_cols == [] do
      maps
    else
      Enum.flat_map(maps, fn row ->
        depth = row |> Map.get(hd(list_cols)) |> length()

        Enum.map(0..(depth - 1), fn i ->
          Map.new(row, fn {k, v} ->
            if is_list(v), do: {k, Enum.at(v, i)}, else: {k, v}
          end)
        end)
      end)
    end
  end

  defp chart_widget([], _cols), do: nil

  defp chart_widget(maps, cols) do
    numeric_cols =
      Enum.filter(cols, fn col ->
        maps |> Enum.take(5) |> Enum.any?(&is_number(Map.get(&1, col)))
      end)

    time_col =
      Enum.find(cols, fn col ->
        maps |> Enum.take(5) |> Enum.any?(fn row ->
          case Map.get(row, col) do
            %DateTime{} -> true
            %Date{} -> true
            %Time{} -> true
            _ -> false
          end
        end)
      end)

    color_col =
      Enum.find(cols, fn col ->
        vals = maps |> Enum.take(20) |> Enum.map(&Map.get(&1, col)) |> Enum.uniq()
        is_binary(hd(vals)) and length(vals) <= 10 and length(vals) > 1
      end)

    x_col = time_col || hd(cols)
    y_col = List.first(numeric_cols)

    if y_col == nil or x_col == y_col do
      nil
    else
      data = Enum.map(maps, fn row -> Map.new(row, fn {k, v} -> {k, vl_value(v)} end) end)
      x_type = if time_col, do: :temporal, else: :ordinal

      spec =
        Vl.new(width: 700, height: 300, title: "#{y_col} over #{x_col}")
        |> Vl.data_from_values(data)
        |> Vl.mark(:line)
        |> Vl.encode_field(:x, x_col, type: x_type)
        |> Vl.encode_field(:y, y_col, type: :quantitative, aggregate: :mean)

      if color_col do
        Vl.encode_field(spec, :color, color_col, type: :nominal)
      else
        spec
      end
    end
  end

  defp vl_value(%DateTime{} = v), do: DateTime.to_iso8601(v)
  defp vl_value(%Date{} = v), do: Date.to_iso8601(v)
  defp vl_value(%Time{} = v), do: Time.to_iso8601(v)
  defp vl_value(v), do: v

  defp ok_or({:ok, val}, _default), do: val
  defp ok_or(_, default), do: default

  defp nilify(""), do: nil
  defp nilify(v), do: v
end
