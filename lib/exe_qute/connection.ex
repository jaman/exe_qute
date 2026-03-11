defmodule ExeQute.Connection do
  @moduledoc false

  use GenServer
  require Logger
  alias ExeQute.Protocol

  @default_timeout 5000

  def cached_query(conn, cache_key, query) do
    GenServer.call(conn, {:cached_query, cache_key, query}, @default_timeout)
  end

  def clear_cache(conn) do
    GenServer.call(conn, :clear_cache, @default_timeout)
  end

  def address(conn) do
    GenServer.call(conn, :address, @default_timeout)
  end

  def start_link(opts) do
    {name_opt, init_opts} =
      case Keyword.pop(opts, :name) do
        {nil, rest} -> {[], rest}
        {name, rest} -> {[name: name], rest}
      end

    GenServer.start_link(__MODULE__, init_opts, name_opt)
  end

  def query(conn, query) do
    GenServer.call(conn, {:query, query}, @default_timeout)
  end

  def publish(conn, terms) do
    GenServer.call(conn, {:publish, terms}, @default_timeout)
  end

  def init(opts) do
    host = Keyword.get(opts, :host, "localhost")

    state = %{
      socket: nil,
      host: host,
      port: nil,
      encoding: Keyword.get(opts, :encoding, "utf8"),
      credentials: build_credentials(opts),
      cache: %{}
    }

    with {:ok, port} <- opts |> Keyword.get(:port, 5001) |> normalize_port(),
         {:ok, socket} <- tcp_connect(host, port, opts),
         :ok <- handshake(socket, state.credentials) do
      {:ok, %{state | socket: socket, port: port}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call({:query, query}, _from, %{socket: socket} = state) do
    result =
      try do
        with {:ok, encoded} <- Protocol.encode(query),
             :ok <- :gen_tcp.send(socket, encoded),
             {:ok, payload} <- Protocol.recv(socket, @default_timeout) do
          Protocol.decode(payload)
        end
      rescue
        e -> {:error, {:decode_error, Exception.message(e)}}
      end

    reply_or_stop(result, state)
  end

  def handle_call({:cached_query, cache_key, query}, _from, %{socket: socket, cache: cache} = state) do
    case Map.get(cache, cache_key) do
      nil ->
        result =
          try do
            with {:ok, encoded} <- Protocol.encode(query),
                 :ok <- :gen_tcp.send(socket, encoded),
                 {:ok, payload} <- Protocol.recv(socket, @default_timeout) do
              Protocol.decode(payload)
            end
          rescue
            e -> {:error, {:decode_error, Exception.message(e)}}
          end

        case result do
          {:ok, value} ->
            {:reply, {:ok, value}, %{state | cache: Map.put(cache, cache_key, value)}}

          error ->
            reply_or_stop(error, state)
        end

      cached ->
        {:reply, {:ok, cached}, state}
    end
  end

  def handle_call(:clear_cache, _from, state) do
    {:reply, :ok, %{state | cache: %{}}}
  end

  def handle_call(:address, _from, state) do
    {:reply, {:ok, state.host, state.port}, state}
  end

  def handle_call({:publish, terms}, _from, %{socket: socket} = state) do
    result =
      try do
        with {:ok, encoded} <- Protocol.encode_async(terms),
             :ok <- :gen_tcp.send(socket, encoded) do
          :ok
        end
      rescue
        e -> {:error, {:encode_error, Exception.message(e)}}
      end

    reply_or_stop(result, state)
  end

  defp reply_or_stop({:ok, _} = ok, state), do: {:reply, ok, state}
  defp reply_or_stop(:ok, state), do: {:reply, :ok, state}

  defp reply_or_stop({:error, reason} = error, state) when is_atom(reason) do
    Logger.error("Connection lost: #{inspect(reason)}")
    {:stop, :normal, error, state}
  end

  defp reply_or_stop({:error, reason} = error, state) do
    Logger.error("Query error: #{inspect(reason)}")
    {:reply, error, state}
  end

  defp tcp_connect(host, port, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    :gen_tcp.connect(
      String.to_charlist(host),
      port,
      [:binary, active: false, packet: 0, keepalive: true, nodelay: true],
      timeout
    )
  end

  defp normalize_port(port) when is_integer(port), do: {:ok, port}

  defp normalize_port(port) when is_binary(port) do
    case Integer.parse(port) do
      {n, ""} -> {:ok, n}
      _ -> {:error, :invalid_port}
    end
  end

  defp normalize_port(_), do: {:error, :invalid_port}

  defp handshake(socket, nil) do
    msg = <<3::8, 0::8>>

    with :ok <- :gen_tcp.send(socket, msg),
         {:ok, <<cap::8>>} <- :gen_tcp.recv(socket, 1) do
      if cap >= 1, do: :ok, else: {:error, :invalid_capability}
    end
  end

  defp handshake(socket, {user, password}) do
    auth = user <> ":" <> password
    msg = <<auth::binary, 3::8, 0::8>>

    with :ok <- :gen_tcp.send(socket, msg),
         {:ok, <<cap::8>>} <- :gen_tcp.recv(socket, 1) do
      if cap >= 1, do: :ok, else: {:error, :invalid_capability}
    end
  end

  defp build_credentials(opts) do
    username = Keyword.get(opts, :username)
    password = Keyword.get(opts, :password)

    case {username, password} do
      {nil, nil} -> nil
      {u, p} when is_binary(u) and is_binary(p) -> {u, p}
      _ -> raise ArgumentError, "Invalid credentials format"
    end
  end
end
