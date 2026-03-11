defmodule ExeQute.ChartCell do
  @moduledoc false

  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "KDB+ Chart"

  @visual_fields ~w[x_field x_type y_field y_type color_field chart_type window]

  @impl true
  def init(attrs, ctx) do
    fields = %{
      "variable" => attrs["variable"] || "chart",
      "subscriber" => attrs["subscriber"] || "",
      "table" => attrs["table"] || "",
      "symbols" => attrs["symbols"] || "",
      "x_field" => attrs["x_field"] || "time",
      "x_type" => attrs["x_type"] || "temporal",
      "y_field" => attrs["y_field"] || "price",
      "y_type" => attrs["y_type"] || "quantitative",
      "color_field" => attrs["color_field"] || "",
      "chart_type" => attrs["chart_type"] || "line",
      "window" => attrs["window"] || "1000"
    }

    {:ok, assign(ctx, fields: fields, subscribers: [])}
  end

  @impl true
  def handle_connect(ctx) do
    payload = %{
      fields: ctx.assigns.fields,
      subscribers: ctx.assigns.subscribers
    }

    {:ok, payload, ctx}
  end

  @impl true
  def scan_binding(server, binding, _env) do
    sub_map =
      for {name, val} <- binding,
          is_atom(name),
          subscriber_value?(val),
          into: %{},
          do: {Atom.to_string(name), val}

    send(server, {:subscribers, sub_map})
  end

  defp subscriber_value?(val) when is_pid(val), do: true

  defp subscriber_value?(val) when is_atom(val) do
    val != nil and val != true and val != false and
      Process.whereis(val) != nil
  end

  defp subscriber_value?(_), do: false

  @impl true
  def handle_info({:subscribers, sub_map}, ctx) do
    subs = Map.keys(sub_map)
    ctx = assign(ctx, subscribers: subs, sub_map: sub_map)
    broadcast_event(ctx, "update_subscribers", %{"subscribers" => subs})
    {:noreply, ctx}
  end

  @impl true
  def handle_event("update_field", %{"field" => field, "value" => value}, ctx) do
    fields = Map.put(ctx.assigns.fields, field, value)
    ctx = assign(ctx, fields: fields)
    try_update_live(fields["variable"], field, value)
    {:noreply, ctx}
  end

  defp try_update_live(var, field, value) when field in @visual_fields do
    table = String.to_atom("kdb_chart_#{var}")
    :ets.insert(table, {String.to_atom(field), parse_ets_value(field, value)})

    if field in ~w[chart_type x_field x_type y_field y_type color_field] do
      case :ets.lookup(table, :rebuild) do
        [{:rebuild, rebuild_fn}] -> rebuild_fn.()
        _ -> :ok
      end
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp try_update_live(_, _, _), do: :ok

  defp parse_ets_value("window", v) do
    case Integer.parse(v) do
      {n, _} when n > 0 -> n
      _ -> 1000
    end
  end

  defp parse_ets_value(f, v) when f in ~w[x_type y_type chart_type], do: String.to_atom(v)
  defp parse_ets_value(_, v), do: v

  @impl true
  def to_attrs(ctx), do: ctx.assigns.fields

  @impl true
  def to_source(attrs) do
    %{
      "variable" => var,
      "subscriber" => sub,
      "table" => table,
      "symbols" => symbols_raw,
      "x_field" => x_field,
      "x_type" => x_type,
      "y_field" => y_field,
      "y_type" => y_type,
      "color_field" => color_field,
      "chart_type" => chart_type,
      "window" => window
    } = attrs

    syms =
      symbols_raw
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    syms_arg = if syms == [], do: "", else: inspect(syms) <> ", "

    """
    kdb_ets = String.to_atom("kdb_chart_#{var}")
    kdb_ref_key = String.to_atom("kdb_chart_#{var}_ref")
    kdb_agent_key = String.to_atom("kdb_chart_#{var}_agent")

    if (prev_ref = Process.get(kdb_ref_key)) do
      try do
        ExeQute.unsubscribe(#{sub}, prev_ref)
      rescue
        _ -> :ok
      end
    end

    if (prev_agent = Process.get(kdb_agent_key)) do
      try do
        Agent.stop(prev_agent)
      rescue
        _ -> :ok
      end
    end

    try do
      :ets.new(kdb_ets, [:named_table, :public, :set])
    rescue
      ArgumentError -> :ets.delete_all_objects(kdb_ets)
    end

    :ets.insert(kdb_ets, [
      x_field: "#{x_field}",
      x_type: :#{x_type},
      y_field: "#{y_field}",
      y_type: :#{y_type},
      color_field: "#{color_field}",
      chart_type: :#{chart_type},
      window: #{window}
    ])

    kdb_echart = ExeQute.EChart.new(height: 420)
    kdb_data_frame = Kino.Frame.new()
    {:ok, kdb_row_buffer} = Agent.start_link(fn -> {[], %{}} end)

    kdb_rebuild_chart = fn ->
      cfg = Map.new(:ets.tab2list(kdb_ets))
      buf = Agent.get(kdb_row_buffer, & &1)
      ExeQute.EChart.render(kdb_echart, ExeQute.EChart.options_from_buffer(cfg, buf))
    end

    :ets.insert(kdb_ets, [
      {:echart, kdb_echart},
      {:data_frame, kdb_data_frame},
      {:buffer, kdb_row_buffer},
      {:rebuild, kdb_rebuild_chart}
    ])

    #{var} = Kino.Layout.tabs([{"Chart", kdb_echart}, {"Data", kdb_data_frame}])

    {:ok, kdb_chart_ref} =
      ExeQute.subscribe(#{sub}, "#{table}", #{syms_arg}fn {_table, raw} ->
        try do
          cfg = Map.new(:ets.tab2list(kdb_ets))
          new_rows = ExeQute.to_rows(raw)

          unless new_rows == [] do
            updated_buf =
              Agent.get_and_update(kdb_row_buffer, fn buf ->
                new_buf = ExeQute.EChart.update_buffer(buf, new_rows, cfg)
                {new_buf, new_buf}
              end)

            [{:echart, echart}] = :ets.lookup(kdb_ets, :echart)
            ExeQute.EChart.push(echart, ExeQute.EChart.options_from_buffer(cfg, updated_buf))

            Kino.Frame.render(
              kdb_data_frame,
              Kino.DataTable.new(ExeQute.EChart.buffer_to_rows(updated_buf, cfg))
            )
          end
        rescue
          _ -> :ok
        end
      end)

    Process.put(kdb_ref_key, kdb_chart_ref)
    Process.put(kdb_agent_key, kdb_row_buffer)
    #{var}
    """
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      let { fields, subscribers } = payload;

      ctx.root.innerHTML = `
        <div class="qcell">
          <div class="top-row">
            <label class="field narrow">
              <span>Variable</span>
              <input type="text" id="variable" value="${esc(fields.variable)}" />
            </label>
            <label class="field">
              <span>Subscriber</span>
              <select id="subscriber">${selectOptions(subscribers, fields.subscriber)}</select>
            </label>
            <label class="field">
              <span>Table</span>
              <input type="text" id="table" value="${esc(fields.table)}" />
            </label>
            <label class="field">
              <span>Symbols (comma-sep)</span>
              <input type="text" id="symbols" value="${esc(fields.symbols)}" placeholder="optional" />
            </label>
            <label class="field narrow">
              <span>Window</span>
              <input type="number" id="window" value="${esc(fields.window)}" min="1" step="100" />
            </label>
          </div>
          <div class="mid-row">
            <label class="field">
              <span>X field</span>
              <input type="text" id="x_field" value="${esc(fields.x_field)}" />
            </label>
            <label class="field narrow">
              <span>X type</span>
              <select id="x_type">${selectOptions(["temporal","quantitative","ordinal","nominal"], fields.x_type)}</select>
            </label>
            <label class="field">
              <span>Y field</span>
              <input type="text" id="y_field" value="${esc(fields.y_field)}" />
            </label>
            <label class="field narrow">
              <span>Y type</span>
              <select id="y_type">${selectOptions(["quantitative","temporal","ordinal","nominal"], fields.y_type)}</select>
            </label>
            <label class="field">
              <span>Color field</span>
              <input type="text" id="color_field" value="${esc(fields.color_field)}" placeholder="optional" />
            </label>
            <label class="field narrow">
              <span>Chart type</span>
              <select id="chart_type">${selectOptions(["line","point","bar"], fields.chart_type)}</select>
            </label>
          </div>
        </div>
      `;

      function selectOptions(list, selected) {
        return list.map(c =>
          `<option value="${esc(c)}" ${c === selected ? "selected" : ""}>${esc(c)}</option>`
        ).join("");
      }

      function push(field, value) {
        ctx.pushEvent("update_field", { field, value });
      }

      const ids = ["variable", "subscriber", "table", "symbols", "window",
                   "x_field", "x_type", "y_field", "y_type", "color_field", "chart_type"];

      for (const id of ids) {
        const el = ctx.root.querySelector(`#${id}`);
        if (el) el.addEventListener("change", e => push(id, e.target.value));
      }

      ctx.handleEvent("update_subscribers", ({ subscribers: list }) => {
        subscribers = list;
        const subSelect = ctx.root.querySelector("#subscriber");
        const prev = subSelect.value;
        subSelect.innerHTML = selectOptions(list, prev);
        if (!subSelect.value && list.length > 0) {
          subSelect.value = list[0];
          push("subscriber", list[0]);
        }
      });

      ctx.handleSync(() => {
        for (const id of ids) {
          const el = ctx.root.querySelector(`#${id}`);
          if (el) push(id, el.value);
        }
      });
    }

    function esc(s) {
      return String(s ?? "")
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/</g, "&lt;");
    }
    """
  end

  asset "main.css" do
    """
    .qcell { font-family: sans-serif; font-size: 13px; }
    .top-row, .mid-row { display: flex; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
    .field { display: flex; flex-direction: column; gap: 3px; flex: 1; min-width: 100px; }
    .field.narrow { max-width: 130px; flex: 0 0 130px; }
    .field span { font-weight: 600; color: #555; }
    .field input, .field select {
      border: 1px solid #ccc; border-radius: 4px; padding: 4px 8px;
      font-size: 13px; width: 100%; box-sizing: border-box;
    }
    """
  end
end
