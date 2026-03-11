defmodule ExeQute.DataFrame do
  @moduledoc false

  @doc """
  Converts a list of maps (query result) to an `Explorer.DataFrame`.

  Temporal types (`DateTime`, `Date`, `Time`) are converted to ISO 8601 strings.
  Complex values (nested lists, maps) are converted via `inspect/1`.

  Falls back to returning the normalized list of maps if Explorer cannot
  construct a DataFrame (e.g. NIF unavailable, mixed column types).
  """
  @spec from_maps([map()]) :: Explorer.DataFrame.t() | [map()]
  def from_maps([]), do: []

  def from_maps([first | _] = rows) do
    columns = Map.keys(first)

    normalized =
      Enum.map(rows, fn row ->
        Map.new(columns, fn col -> {col, normalize(Map.get(row, col))} end)
      end)

    try_dataframe(normalized, columns)
  end

  defp try_dataframe(normalized, columns) do
    series_map =
      Map.new(columns, fn col ->
        {col, Enum.map(normalized, &Map.fetch!(&1, col))}
      end)

    Explorer.DataFrame.new(series_map)
  rescue
    _ -> normalized
  end

  defp normalize(%DateTime{} = v), do: DateTime.to_iso8601(v)
  defp normalize(%Date{} = v), do: Date.to_iso8601(v)
  defp normalize(%Time{} = v), do: Time.to_iso8601(v)
  defp normalize(v) when is_list(v), do: inspect(v)
  defp normalize(v) when is_map(v) and not is_struct(v), do: inspect(v)
  defp normalize(v), do: v
end
