defmodule ExeQute.Protocol do
  @moduledoc false

  import Bitwise
  require Logger
  alias ExeQute.Types

  @kdb_sync 1
  @kdb_async 0

  @spec encode(String.t() | list()) :: {:ok, iodata()} | {:error, term()}
  def encode(message) when is_binary(message) do
    payload = <<10::8, 0::8, byte_size(message)::little-32, message::binary>>
    total_size = byte_size(payload) + 8
    header = <<1::8, @kdb_sync::8, 0::8, 0::8, total_size::little-32>>
    {:ok, [header, payload]}
  end

  def encode(terms) when is_list(terms) do
    wrap_payload(@kdb_sync, terms)
  end

  @spec encode_async(list()) :: {:ok, iodata()} | {:error, term()}
  def encode_async(terms) when is_list(terms) do
    wrap_payload(@kdb_async, terms)
  end

  defp wrap_payload(msg_type, terms) do
    try do
      payload = IO.iodata_to_binary(Types.encode(terms))
      total_size = byte_size(payload) + 8
      header = <<1::8, msg_type::8, 0::8, 0::8, total_size::little-32>>
      {:ok, [header, payload]}
    rescue
      e -> {:error, {:encode_error, Exception.message(e)}}
    end
  end

  @spec recv(:gen_tcp.socket(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def recv(socket, timeout) do
    with {:ok, <<_endian::8, msg_type::8, compressed::8, _reserved::8, size::little-32>>} <-
           :gen_tcp.recv(socket, 8, timeout),
         {:ok, raw} <- :gen_tcp.recv(socket, size - 8, timeout) do
      payload = if compressed == 1, do: kdb_decompress(raw), else: raw
      {:ok, payload}
    end
  end

  @spec decode_message(binary()) :: {:ok, term()} | {:error, term()}
  def decode_message(
        <<_endian::8, _msg_type::8, compressed::8, _reserved::8, _size::little-32,
          payload::binary>>
      ) do
    data = if compressed == 1, do: kdb_decompress(payload), else: payload
    decode(data)
  end

  def decode_message(_), do: {:error, {:decode_error, :invalid_message}}

  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(payload) do
    case Types.decode(payload) do
      {:ok, value, <<>>} -> {:ok, value}
      {:ok, _value, _rest} -> {:error, {:decode_error, :trailing_bytes}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec kdb_decompress(binary()) :: binary()
  defp kdb_decompress(<<uncompressed_size::little-32, compressed::binary>>) do
    dst = :array.new(uncompressed_size, default: 0)
    aa = :array.new(256, default: 0)
    dst = kdb_decomp(compressed, 0, dst, aa, 8, 8, 0, 0, uncompressed_size)
    for i <- 8..(uncompressed_size - 1), into: <<>>, do: <<:array.get(i, dst)>>
  end

  defp kdb_decomp(src, d, dst, aa, s, p, f, i, usize) when s < usize do
    {f, i, d} = kdb_next_mask(src, d, f, i)

    if (f &&& i) != 0 do
      r = :array.get(:binary.at(src, d), aa)
      d = d + 1
      dst = :array.set(s, :array.get(r, dst), dst)
      dst = :array.set(s + 1, :array.get(r + 1, dst), dst)
      n = :binary.at(src, d)
      d = d + 1
      dst = kdb_copy_backref(dst, r + 2, s + 2, n)
      {dst, aa, _p} = kdb_hash_advance(dst, aa, p, s + 2)
      kdb_decomp(src, d, dst, aa, s + 2 + n, s + 2 + n, f, i * 2, usize)
    else
      dst = :array.set(s, :binary.at(src, d), dst)
      d = d + 1
      {dst, aa, p} = kdb_hash_advance(dst, aa, p, s + 1)
      kdb_decomp(src, d, dst, aa, s + 1, p, f, i * 2, usize)
    end
  end

  defp kdb_decomp(_src, _d, dst, _aa, _s, _p, _f, _i, _usize), do: dst

  defp kdb_copy_backref(dst, _r, _s, 0), do: dst

  defp kdb_copy_backref(dst, r, s, n) do
    dst = :array.set(s, :array.get(r, dst), dst)
    kdb_copy_backref(dst, r + 1, s + 1, n - 1)
  end

  defp kdb_hash_advance(dst, aa, p, limit) when p < limit - 1 do
    key = bxor(:array.get(p, dst), :array.get(p + 1, dst))
    aa = :array.set(key, p, aa)
    kdb_hash_advance(dst, aa, p + 1, limit)
  end

  defp kdb_hash_advance(dst, aa, p, _limit), do: {dst, aa, p}

  defp kdb_next_mask(src, d, _f, i) when i == 0 or i == 256,
    do: {:binary.at(src, d), 1, d + 1}

  defp kdb_next_mask(_src, d, f, i), do: {f, i, d}
end
