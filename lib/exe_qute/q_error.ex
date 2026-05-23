defmodule ExeQute.QError do
  @moduledoc """
  Parsed kdb+ backtrace returned when a server's `.Q.trp` error handler
  emits a `** Backtrace:` string as the query response instead of raising
  a wire-level error.

  `ExeQute.query/2` and `query/3` automatically detect such responses and
  return `{:error, %ExeQute.QError{}}` rather than wrapping the backtrace
  in an `:ok` tuple.

  The struct keeps both the raw backtrace text and a list of parsed frames.
  `inspect/1` renders the raw backtrace with line breaks preserved (so it
  shows readably in IEx), and `to_string/1` returns the same text for use
  with `IO.puts/1`.
  """

  @enforce_keys [:raw, :frames]
  defstruct [:raw, :frames]

  @type frame :: %{
          level: non_neg_integer(),
          code: String.t(),
          file: String.t() | nil,
          line: pos_integer() | nil,
          function: String.t() | nil
        }

  @type t :: %__MODULE__{raw: String.t(), frames: [frame()]}

  @prefix "** Backtrace:"

  @doc """
  Returns `true` if `value` is a kdb+ backtrace string.
  """
  @spec backtrace?(term()) :: boolean()
  def backtrace?(@prefix <> _), do: true
  def backtrace?(_), do: false

  @doc """
  Parses a raw kdb+ backtrace string into an `ExeQute.QError` struct.

  Returns `:error` if `raw` does not begin with the `** Backtrace:` marker.

  ## Example

      iex> {:ok, %ExeQute.QError{frames: [%{level: 0} | _]}} =
      ...>   ExeQute.QError.parse("** Backtrace:  [0]  (.Q.trp)\\n")
  """
  @spec parse(String.t()) :: {:ok, t()} | :error
  def parse(@prefix <> _ = raw) do
    frames =
      raw
      |> String.split("\n")
      |> collect_frames([], nil)
      |> Enum.reverse()

    {:ok, %__MODULE__{raw: raw, frames: frames}}
  end

  def parse(_), do: :error

  defp collect_frames([], frames, nil), do: frames
  defp collect_frames([], frames, current), do: [finalize_frame(current) | frames]

  defp collect_frames([line | rest], frames, current) do
    case parse_header(line) do
      {:ok, level, header_rest} ->
        new_frames = close_current(current, frames)
        collect_frames(rest, new_frames, start_frame(level, header_rest))

      :no ->
        collect_frames(rest, frames, append_line(current, line))
    end
  end

  defp close_current(nil, frames), do: frames
  defp close_current(current, frames), do: [finalize_frame(current) | frames]

  defp append_line(nil, _line), do: nil

  defp append_line(current, line) do
    case caret_only?(line) do
      true -> current
      false -> %{current | extra: [line | current.extra]}
    end
  end

  defp caret_only?(line), do: String.match?(line, ~r/^\s*\^\s*$/)

  defp parse_header(line) do
    trimmed =
      line
      |> String.trim_leading()
      |> String.replace_prefix(@prefix, "")
      |> String.trim_leading()

    case Regex.run(~r/^\[(\d+)\]\s*(.*)$/, trimmed) do
      [_, level, rest] -> {:ok, String.to_integer(level), rest}
      _ -> :no
    end
  end

  defp start_frame(level, header_rest) do
    {file, line, function, code} = parse_location(header_rest)
    %{level: level, file: file, line: line, function: function, code: code, extra: []}
  end

  defp parse_location(rest) do
    case Regex.run(~r{^(/[^:\s]+):(\d+):\s*(\S.*?):\s*(.*)$}, rest) do
      [_, file, line_str, func, code] ->
        {file, String.to_integer(line_str), func, code}

      _ ->
        {nil, nil, nil, rest}
    end
  end

  defp finalize_frame(frame) do
    extra_code = frame.extra |> Enum.reverse() |> Enum.join("\n")
    code = join_code(frame.code, extra_code)

    %{
      level: frame.level,
      code: code,
      file: frame.file,
      line: frame.line,
      function: frame.function
    }
  end

  defp join_code(head, ""), do: head
  defp join_code("", extra), do: extra
  defp join_code(head, extra), do: head <> "\n" <> extra

  defimpl Inspect do
    def inspect(%ExeQute.QError{raw: raw}, _opts) do
      "#ExeQute.QError<\n" <> raw <> "\n>"
    end
  end

  defimpl String.Chars do
    def to_string(%ExeQute.QError{raw: raw}), do: raw
  end
end
