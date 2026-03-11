defmodule ExeQute.EChart do
  @moduledoc """
  Live [Apache ECharts](https://echarts.apache.org) widget for Livebook.

  > #### Work in progress {: .warning}
  >
  > This module is functional but not yet fully polished. The API and behaviour
  > may change in future releases.

  Wraps Apache ECharts via `Kino.JS.Live`. ECharts options are passed as Elixir
  maps and serialised to JSON; the JavaScript side calls `setOption` on every
  update. The goal is a lower-overhead alternative to VegaLite for high-frequency
  streaming data — ECharts handles incremental series updates without rebuilding
  the entire chart.

  ## Static usage

      options = %{
        "xAxis" => %{"type" => "category", "data" => ["EUR/USD", "USD/JPY"]},
        "yAxis" => %{"type" => "value"},
        "series" => [%{"name" => "lp1", "type" => "bar", "data" => [1.05, 1.08]}]
      }

      chart = ExeQute.EChart.new(options: options)

  ## Live usage

      chart = ExeQute.EChart.new()
      ExeQute.EChart.render(chart, initial_options)
      ExeQute.EChart.push(chart, updated_options)

  ## Chart cell helpers

  `update_buffer/3`, `options_from_buffer/2`, and `buffer_to_rows/2` are used
  by the generated code of `ExeQute.ChartCell` but are public so they can be
  called directly when building custom subscription callbacks.
  """

  use Kino.JS
  use Kino.JS.Live

  @doc """
  Creates a new EChart widget.

  ## Options

    * `:options` - initial ECharts option map (default: `%{}`)
    * `:height` - chart height in pixels (default: `400`)

  """
  @spec new(keyword()) :: Kino.JS.Live.t()
  def new(opts \\ []) do
    Kino.JS.Live.new(__MODULE__, %{
      options: Keyword.get(opts, :options, %{}),
      height: Keyword.get(opts, :height, 400)
    })
  end

  @doc """
  Replaces the chart entirely with `options`.

  Calls `setOption(options, { notMerge: true })` — all existing components
  are discarded. Use this for structural changes (switching chart type, etc.).
  """
  @spec render(Kino.JS.Live.t(), map()) :: :ok
  def render(widget, options) do
    Kino.JS.Live.cast(widget, {:render, options})
  end

  @doc """
  Pushes an incremental update.

  Calls `setOption(options, { replaceMerge: ['series'] })` — series are
  replaced by the new list while axes, legend, and tooltip are preserved.
  Suitable for streaming data updates.
  """
  @spec push(Kino.JS.Live.t(), map()) :: :ok
  def push(widget, options) do
    Kino.JS.Live.cast(widget, {:push, options})
  end

  @doc """
  Updates the internal row buffer with newly arrived rows.

  `cfg` is the config map read from ETS (atom keys). Returns the updated
  buffer. The buffer format is `{list, map}`:

  - `list` — newest-first list of rows, used for temporal/quantitative x axes
  - `map` — keyed by `x_value` or `{x_value, color_value}`, used for nominal axes

  """
  @spec update_buffer({list(), map()}, [map()], map()) :: {list(), map()}
  def update_buffer({list_buf, map_buf}, new_rows, cfg) do
    if cfg.x_type == :nominal do
      updated =
        Enum.reduce(new_rows, map_buf, fn row, acc ->
          key =
            if cfg.color_field != "",
              do: {Map.get(row, cfg.x_field), Map.get(row, cfg.color_field)},
              else: Map.get(row, cfg.x_field)

          Map.put(acc, key, row)
        end)

      {list_buf, updated}
    else
      {Enum.take(new_rows ++ list_buf, cfg.window), map_buf}
    end
  end

  @doc """
  Converts a buffer to a flat list of row maps for `Kino.DataTable`.
  """
  @spec buffer_to_rows({list(), map()}, map()) :: [map()]
  def buffer_to_rows({list_buf, map_buf}, cfg) do
    if cfg.x_type == :nominal, do: Map.values(map_buf), else: list_buf
  end

  @doc """
  Builds a full ECharts option map from the current buffer and config.
  """
  @spec options_from_buffer(map(), {list(), map()}) :: map()
  def options_from_buffer(cfg, {list_buf, map_buf}) do
    if cfg.x_type == :nominal,
      do: nominal_options(cfg, map_buf),
      else: temporal_options(cfg, list_buf)
  end

  defp nominal_options(cfg, map_buf) do
    {x_set, color_set} =
      Enum.reduce(Map.keys(map_buf), {MapSet.new(), MapSet.new()}, fn
        {x, c}, {xs, cs} -> {MapSet.put(xs, x), MapSet.put(cs, c)}
        x, {xs, cs} -> {MapSet.put(xs, x), cs}
      end)

    x_list = x_set |> MapSet.to_list() |> Enum.sort_by(&to_string/1)

    series =
      if MapSet.size(color_set) > 0 do
        color_set
        |> MapSet.to_list()
        |> Enum.sort_by(&to_string/1)
        |> Enum.map(fn color ->
          data =
            Enum.map(x_list, fn x ->
              map_buf |> Map.get({x, color}, %{}) |> Map.get(cfg.y_field)
            end)

          %{
            "name" => to_string(color),
            "type" => to_string(cfg.chart_type),
            "barGap" => 0,
            "emphasis" => %{"focus" => "series"},
            "data" => data
          }
        end)
      else
        data =
          Enum.map(x_list, fn x ->
            map_buf |> Map.get(x, %{}) |> Map.get(cfg.y_field)
          end)

        [
          %{
            "name" => cfg.y_field,
            "type" => to_string(cfg.chart_type),
            "data" => data
          }
        ]
      end

    %{
      "animation" => false,
      "tooltip" => %{"trigger" => "axis", "axisPointer" => %{"type" => "shadow"}},
      "legend" => %{},
      "xAxis" => [%{"type" => "category", "axisTick" => %{"show" => false}, "data" => Enum.map(x_list, &to_string/1)}],
      "yAxis" => [%{"type" => "value"}],
      "series" => series
    }
  end

  defp temporal_options(cfg, list_buf) do
    rows = Enum.reverse(list_buf)

    x_axis_type =
      case cfg.x_type do
        :temporal -> "time"
        :quantitative -> "value"
        _ -> "category"
      end

    series =
      if cfg.color_field != "" do
        rows
        |> Enum.group_by(&Map.get(&1, cfg.color_field))
        |> Enum.map(fn {color, color_rows} ->
          data = Enum.map(color_rows, fn row ->
            [row_x(row, cfg), Map.get(row, cfg.y_field)]
          end)

          %{
            "name" => to_string(color),
            "type" => to_string(cfg.chart_type),
            "data" => data,
            "smooth" => cfg.chart_type == :line
          }
        end)
      else
        data = Enum.map(rows, fn row ->
          [row_x(row, cfg), Map.get(row, cfg.y_field)]
        end)

        [
          %{
            "name" => cfg.y_field,
            "type" => to_string(cfg.chart_type),
            "data" => data,
            "smooth" => cfg.chart_type == :line
          }
        ]
      end

    %{
      "animation" => false,
      "tooltip" => %{"trigger" => "axis"},
      "legend" => %{},
      "xAxis" => [%{"type" => x_axis_type}],
      "yAxis" => [%{"type" => "value"}],
      "series" => series
    }
  end

  defp row_x(row, %{x_type: :temporal, x_field: f}), do: to_string(Map.get(row, f))
  defp row_x(row, %{x_field: f}), do: Map.get(row, f)

  @impl true
  def init(%{options: options, height: height}, ctx) do
    {:ok, assign(ctx, options: options, height: height)}
  end

  @impl true
  def handle_connect(ctx) do
    {:ok, %{options: ctx.assigns.options, height: ctx.assigns.height}, ctx}
  end

  @impl true
  def handle_cast({:render, options}, ctx) do
    broadcast_event(ctx, "render", options)
    {:noreply, assign(ctx, options: options)}
  end

  @impl true
  def handle_cast({:push, options}, ctx) do
    broadcast_event(ctx, "push", options)
    {:noreply, ctx}
  end

  asset "main.js" do
    """
    export async function init(ctx, payload) {
      await ctx.importJS("https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js");

      ctx.root.style.width = "100%";
      ctx.root.innerHTML = `<div style="width:100%;height:${payload.height}px;"></div>`;

      const chart = echarts.init(ctx.root.firstElementChild, null, { renderer: "canvas" });

      const ro = new ResizeObserver(() => chart.resize());
      ro.observe(ctx.root.firstElementChild);

      if (Object.keys(payload.options).length > 0) {
        chart.setOption(payload.options, { notMerge: true });
      }

      ctx.handleEvent("render", (options) => {
        chart.setOption(options, { notMerge: true });
      });

      ctx.handleEvent("push", (options) => {
        chart.setOption(options, { replaceMerge: ["series"] });
      });
    }
    """
  end
end
