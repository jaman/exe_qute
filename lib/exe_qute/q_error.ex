defmodule ExeQute.QError do
  @moduledoc """
  Parsed kdb+ backtrace returned when a server's `.Q.trp` error handler
  emits a `** Backtrace:` string as the query response instead of raising
  a wire-level error.

  `ExeQute.query/2` and `query/3` automatically detect such responses and
  return `{:error, %ExeQute.QError{}}` rather than wrapping the backtrace
  in an `:ok` tuple.

  The struct keeps the raw backtrace text, a list of parsed frames, and —
  if the server emits one — an `:message` containing the error text (e.g.
  `"hop. OS reports: Connection refused"`). Stock kdb+ gateways that only
  emit a backtrace leave `:message` as `nil`; gateways patched to include
  the error inline as `** Backtrace: <error>\\n  [N] ...` populate it.

  `inspect/1` renders the raw backtrace with line breaks preserved (so it
  shows readably in IEx), and `to_string/1` returns the same text for use
  with `IO.puts/1`.
  """

  @enforce_keys [:raw, :frames]
  defstruct [:raw, :frames, message: nil]

  @type frame :: %{
          level: non_neg_integer(),
          code: String.t(),
          file: String.t() | nil,
          line: pos_integer() | nil,
          function: String.t() | nil
        }

  @type t :: %__MODULE__{
          raw: String.t(),
          frames: [frame()],
          message: String.t() | nil
        }

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
  def parse(@prefix <> rest = raw) do
    {message, body_lines} = split_message(rest)

    frames =
      body_lines
      |> collect_frames([], nil)
      |> Enum.reverse()

    {:ok, %__MODULE__{raw: raw, frames: frames, message: message}}
  end

  def parse(_), do: :error

  @doc """
  Converts a `Connection.query` response tuple, replacing any backtrace
  string (whether returned via `:ok` or raised as an `:error`) with a
  `{:error, %ExeQute.QError{}}` tuple.

  Responses that don't carry a backtrace are passed through unchanged.

  ## Examples

      iex> ExeQute.QError.from_response({:ok, [1, 2, 3]})
      {:ok, [1, 2, 3]}

      iex> ExeQute.QError.from_response({:error, :timeout})
      {:error, :timeout}

      iex> {:error, %ExeQute.QError{}} =
      ...>   ExeQute.QError.from_response({:error, "** Backtrace:  [0]  (.Q.trp)"})
  """
  @spec from_response({:ok, term()} | {:error, term()}) ::
          {:ok, term()} | {:error, term()}
  def from_response({status, raw}) when status in [:ok, :error] and is_binary(raw) do
    case parse(raw) do
      {:ok, qerror} -> {:error, qerror}
      :error -> {status, raw}
    end
  end

  def from_response(other), do: other

  @trap_ok "exe_qute_trap_ok"
  @trap_err "exe_qute_trap_err"

  @doc """
  Wraps a q query string in a server-side error trap.

  Use together with `untrap/1` to capture kdb+ errors directly, even on
  gateways that swallow them inside `.Q.trp`. The wrapper installs an
  `@[...]` form that fires *inside* the gateway's handler, so the error
  string reaches the client untouched.

  ## Examples

      query = ExeQute.QError.trap("select from trade")
      {:ok, conn} = ExeQute.connect(host: "kdb", port: 5010)
      ExeQute.query(conn, query) |> ExeQute.QError.untrap()
      #=> {:ok, [...]}   on success
      #=> {:error, "..."} on q-side error, with the original error string
  """
  @spec trap(String.t()) :: String.t()
  def trap(query) when is_binary(query) do
    ~s|@[{(`#{@trap_ok};value x)};"#{escape_q_string(query)}";{(`#{@trap_err};x)}]|
  end

  @doc """
  Unwraps a response previously wrapped with `trap/1`.

  Tagged-success responses become `{:ok, value}`; tagged-error responses
  become `{:error, error_string}`. Anything else is passed through
  unchanged, so `untrap/1` is safe to chain after `ExeQute.query/2` even
  if the trap wrapping ended up bypassed (e.g. by an outer gateway).
  """
  @spec untrap({:ok, term()} | {:error, term()}) :: {:ok, term()} | {:error, term()}
  def untrap({:ok, [@trap_ok, value]}), do: {:ok, value}
  def untrap({:ok, [@trap_err, msg]}), do: {:error, msg}
  def untrap(other), do: other

  defp escape_q_string(str) do
    str
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
  end

  defp split_message(rest) do
    lines = String.split(rest, "\n")

    case lines do
      [first | tail] ->
        case first_frame_in_line(first) do
          :frame -> {nil, lines}
          {:message, msg} -> {msg, tail}
        end

      [] ->
        {nil, []}
    end
  end

  defp first_frame_in_line(line) do
    trimmed = String.trim_leading(line)

    case Regex.run(~r/^\[\d+\]/, trimmed) do
      nil ->
        case String.trim(trimmed) do
          "" -> :frame
          msg -> {:message, msg}
        end

      _ ->
        :frame
    end
  end

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
