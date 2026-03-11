defmodule ExeQute.Types do
  @moduledoc false

  import Bitwise

  @epoch ~N[2000-01-01 00:00:00]

  @type_map %{
    -128 => :error,
    -1 => :boolean,
    -2 => :guid,
    -4 => :byte,
    -5 => :short,
    -6 => :int,
    -7 => :long,
    -8 => :real,
    -9 => :float,
    -10 => :char,
    -11 => :symbol,
    -12 => :timestamp,
    -13 => :month,
    -14 => :date,
    -15 => :datetime,
    -16 => :timespan,
    -17 => :minute,
    -18 => :second,
    -19 => :time,
    0 => :list,
    1 => :boolean_list,
    2 => :guid_list,
    4 => :byte_list,
    5 => :short_list,
    6 => :int_list,
    7 => :long_list,
    8 => :real_list,
    9 => :float_list,
    10 => :char_list,
    11 => :symbol_list,
    12 => :timestamp_list,
    13 => :month_list,
    14 => :date_list,
    15 => :datetime_list,
    16 => :timespan_list,
    17 => :minute_list,
    18 => :second_list,
    19 => :time_list,
    39 => :input_error,
    97 => :string_list,
    98 => :table,
    99 => :dictionary,
    100 => :lambda,
    101 => :unary_prim,
    102 => :operator,
    103 => :iterator,
    104 => :projection,
    105 => :composition,
    106 => :f,
    107 => :dynamic_load,
    108 => :error,
    109 => :partial,
    110 => :aggregation,
    111 => :native,
    112 => :binary
  }

  @kdb_epoch_days 10957
  @kdb_null_long -9_223_372_036_854_775_808
  @kdb_null_int -2_147_483_648
  @kdb_null_short -32_768

  @float64_exponent_mask 0x7FF0000000000000
  @float64_mantissa_mask 0x000FFFFFFFFFFFFF
  @float64_sign_mask 0x8000000000000000

  @spec decode(binary()) :: {:ok, term(), binary()} | {:error, term()}
  def decode(<<type::signed-8, rest::binary>>) do
    case Map.get(@type_map, type) do
      nil when type == 20 -> do_decode(:long_list, rest)
      nil when type >= 21 and type <= 97 -> do_decode(:int_list, rest)
      nil when type == -20 -> do_decode(:long, rest)
      nil when type >= -97 and type <= -21 -> do_decode(:int, rest)
      nil -> {:error, {:unknown_type, type}}
      detected_type -> do_decode(detected_type, rest)
    end
  end

  defp do_decode(:error, data) do
    case split_null_terminated(data) do
      {:ok, msg, rest} -> {:ok, {:kdb_error, msg}, rest}
      error -> error
    end
  end

  defp do_decode(:lambda, data) do
    with {:ok, _name, rest} <- split_null_terminated(data),
         {:ok, _body, rest2} <- decode(rest) do
      {:ok, :lambda, rest2}
    end
  end

  defp do_decode(:unary_prim, <<_v::8, rest::binary>>), do: {:ok, nil, rest}
  defp do_decode(:operator, <<_v::8, rest::binary>>), do: {:ok, :operator, rest}
  defp do_decode(:iterator, <<_v::8, rest::binary>>), do: {:ok, :iterator, rest}
  defp do_decode(:primitive, <<_v::8, rest::binary>>), do: {:ok, :primitive, rest}

  defp do_decode(:projection, <<count::little-32, data::binary>>) do
    skip_objects(data, count, :projection)
  end

  defp do_decode(:composition, <<count::little-32, data::binary>>) do
    skip_objects(data, count, :composition)
  end

  defp do_decode(:f, data), do: skip_objects(data, 1, :f)
  defp do_decode(:dynamic_load, data), do: skip_objects(data, 1, :dynamic_load)
  defp do_decode(:partial, data), do: skip_objects(data, 1, :partial)
  defp do_decode(:aggregation, data), do: skip_objects(data, 1, :aggregation)
  defp do_decode(:native, data), do: skip_objects(data, 1, :native)
  defp do_decode(:binary, data), do: skip_objects(data, 1, :binary)

  defp do_decode(:input_error, data) do
    case split_null_terminated(data) do
      {:ok, msg, rest} -> {:ok, {:kdb_error, msg}, rest}
      error -> error
    end
  end

  defp do_decode(:string_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_symbol_list_items(data, count, [])
  end

  defp do_decode(:function_body, data) do
    case decode(data) do
      {:ok, _, rest} -> {:ok, :function, rest}
      error -> error
    end
  end

  defp do_decode(:boolean, <<value::8, rest::binary>>), do: {:ok, value != 0, rest}
  defp do_decode(:guid, <<value::binary-16, rest::binary>>), do: {:ok, value, rest}
  defp do_decode(:byte, <<value::8, rest::binary>>), do: {:ok, value, rest}

  defp do_decode(:short, <<@kdb_null_short::little-signed-16, rest::binary>>),
    do: {:ok, nil, rest}

  defp do_decode(:short, <<value::little-signed-16, rest::binary>>), do: {:ok, value, rest}
  defp do_decode(:int, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}
  defp do_decode(:int, <<value::little-signed-32, rest::binary>>), do: {:ok, value, rest}
  defp do_decode(:long, <<@kdb_null_long::little-signed-64, rest::binary>>), do: {:ok, nil, rest}
  defp do_decode(:long, <<value::little-signed-64, rest::binary>>), do: {:ok, value, rest}
  defp do_decode(:real, <<0x00, 0x00, 0xC0, 0xFF, rest::binary>>), do: {:ok, nil, rest}
  defp do_decode(:real, <<value::little-float-32, rest::binary>>), do: {:ok, value, rest}

  defp do_decode(:float, <<raw::little-unsigned-64, rest::binary>>) do
    {:ok, decode_float64(raw), rest}
  end

  defp do_decode(:char, <<value::8, rest::binary>>), do: {:ok, <<value>>, rest}

  defp do_decode(:symbol, data) do
    case split_null_terminated(data) do
      {:ok, str, rest} -> {:ok, str, rest}
      error -> error
    end
  end

  defp do_decode(:timestamp, <<@kdb_null_long::little-signed-64, rest::binary>>),
    do: {:ok, nil, rest}

  defp do_decode(:timestamp, <<nanos::little-signed-64, rest::binary>>) do
    epoch_nanos = @kdb_epoch_days * 86_400_000_000_000
    {:ok, DateTime.from_unix!(nanos + epoch_nanos, :nanosecond), rest}
  end

  defp do_decode(:month, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:month, <<months::little-signed-32, rest::binary>>) do
    year = 2000 + div(months, 12)
    month = rem(months, 12) + 1
    {:ok, Date.new!(year, month, 1), rest}
  end

  defp do_decode(:date, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:date, <<days::little-signed-32, rest::binary>>) do
    {:ok, Date.add(@epoch, days), rest}
  end

  defp do_decode(:datetime, <<days::little-float-64, rest::binary>>) when days == days do
    millis = round((days + @kdb_epoch_days) * 86_400_000)
    {:ok, DateTime.from_unix!(millis, :millisecond), rest}
  end

  defp do_decode(:datetime, <<_::binary-8, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:timespan, <<@kdb_null_long::little-signed-64, rest::binary>>),
    do: {:ok, nil, rest}

  defp do_decode(:timespan, <<nanos::little-signed-64, rest::binary>>), do: {:ok, nanos, rest}

  defp do_decode(:minute, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:minute, <<minutes::little-signed-32, rest::binary>>) do
    {:ok, Time.new!(div(minutes, 60), rem(minutes, 60), 0), rest}
  end

  defp do_decode(:second, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:second, <<seconds::little-signed-32, rest::binary>>) do
    {:ok, Time.new!(div(seconds, 3_600), rem(div(seconds, 60), 60), rem(seconds, 60)), rest}
  end

  defp do_decode(:time, <<@kdb_null_int::little-signed-32, rest::binary>>), do: {:ok, nil, rest}

  defp do_decode(:time, <<ms::little-signed-32, rest::binary>>) do
    hours = div(ms, 3_600_000)
    rem1 = rem(ms, 3_600_000)
    minutes = div(rem1, 60_000)
    rem2 = rem(rem1, 60_000)
    seconds = div(rem2, 1_000)
    microseconds = rem(rem2, 1_000) * 1_000
    {:ok, Time.new!(hours, minutes, seconds, {microseconds, 3}), rest}
  end

  defp do_decode(:list, <<_attrs::8, length::little-32, rest::binary>>) do
    decode_list_items(rest, length, [])
  end

  defp do_decode(:char_list, <<_attrs::8, len::little-32, data::binary>>) do
    <<str::binary-size(len), rest::binary>> = data
    {:ok, str, rest}
  end

  defp do_decode(:symbol_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_symbol_list_items(data, count, [])
  end

  defp do_decode(:guid_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:guid, data, count, [])
  end

  defp do_decode(:boolean_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:boolean, data, count, [])
  end

  defp do_decode(:byte_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:byte, data, count, [])
  end

  defp do_decode(:short_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:short, data, count, [])
  end

  defp do_decode(:int_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:int, data, count, [])
  end

  defp do_decode(:long_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:long, data, count, [])
  end

  defp do_decode(:real_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:real, data, count, [])
  end

  defp do_decode(:float_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:float, data, count, [])
  end

  defp do_decode(:timestamp_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:timestamp, data, count, [])
  end

  defp do_decode(:month_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:month, data, count, [])
  end

  defp do_decode(:date_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:date, data, count, [])
  end

  defp do_decode(:datetime_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:datetime, data, count, [])
  end

  defp do_decode(:timespan_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:timespan, data, count, [])
  end

  defp do_decode(:minute_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:minute, data, count, [])
  end

  defp do_decode(:second_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:second, data, count, [])
  end

  defp do_decode(:time_list, <<_attrs::8, count::little-32, data::binary>>) do
    decode_typed_list_items(:time, data, count, [])
  end

  defp do_decode(:dictionary, data) do
    with {:ok, keys, rest1} <- decode(data),
         {:ok, values, rest2} <- decode(rest1) do
      {:ok, build_dict(keys, values), rest2}
    end
  end

  defp do_decode(:table, <<_attrs::8, rest::binary>>) do
    with {:ok, dict, rest2} <- decode(rest) do
      columns = Map.keys(dict)

      rows =
        dict
        |> Map.values()
        |> Enum.map(&col_to_list/1)
        |> Enum.zip()
        |> Enum.map(&Tuple.to_list/1)

      {:ok, %{columns: columns, rows: rows}, rest2}
    end
  end

  defp build_dict(%{columns: key_cols, rows: key_rows}, %{columns: val_cols, rows: val_rows}) do
    %{columns: key_cols ++ val_cols, rows: Enum.zip_with(key_rows, val_rows, &(&1 ++ &2))}
  end

  defp build_dict(keys, values) do
    List.wrap(keys) |> Enum.zip(List.wrap(values)) |> Map.new()
  end

  defp col_to_list(v) when is_binary(v), do: String.graphemes(v)
  defp col_to_list(v), do: v

  defp skip_objects(rest, 0, tag), do: {:ok, tag, rest}

  defp skip_objects(data, n, tag) do
    case decode(data) do
      {:ok, _, rest} -> skip_objects(rest, n - 1, tag)
      error -> error
    end
  end

  defp decode_list_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_list_items(data, count, acc) do
    case decode(data) do
      {:ok, value, rest} -> decode_list_items(rest, count - 1, [value | acc])
      error -> error
    end
  end

  defp decode_typed_list_items(_type, rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_typed_list_items(type, data, count, acc) do
    case decode_typed_item(type, data) do
      {:ok, value, rest} ->
        decode_typed_list_items(type, rest, count - 1, [value | acc])

      {:error, _} = error ->
        error
    end
  end

  defp decode_typed_item(:boolean, <<v::8, rest::binary>>), do: {:ok, v != 0, rest}
  defp decode_typed_item(:guid, <<v::binary-16, rest::binary>>), do: {:ok, v, rest}
  defp decode_typed_item(:byte, <<v::8, rest::binary>>), do: {:ok, v, rest}

  defp decode_typed_item(:short, <<@kdb_null_short::little-signed-16, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:short, <<v::little-signed-16, rest::binary>>), do: {:ok, v, rest}

  defp decode_typed_item(:int, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:int, <<v::little-signed-32, rest::binary>>), do: {:ok, v, rest}

  defp decode_typed_item(:long, <<@kdb_null_long::little-signed-64, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:long, <<v::little-signed-64, rest::binary>>), do: {:ok, v, rest}
  defp decode_typed_item(:real, <<0x00, 0x00, 0xC0, 0xFF, rest::binary>>), do: {:ok, nil, rest}
  defp decode_typed_item(:real, <<v::little-float-32, rest::binary>>), do: {:ok, v, rest}

  defp decode_typed_item(:float, <<raw::little-unsigned-64, rest::binary>>) do
    {:ok, decode_float64(raw), rest}
  end

  defp decode_typed_item(:char, <<v::8, rest::binary>>), do: {:ok, <<v>>, rest}

  defp decode_typed_item(:symbol, data) do
    case split_null_terminated(data) do
      {:ok, str, rest} -> {:ok, str, rest}
      error -> error
    end
  end

  defp decode_typed_item(:timestamp, <<@kdb_null_long::little-signed-64, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:timestamp, <<nanos::little-signed-64, rest::binary>>) do
    epoch_nanos = @kdb_epoch_days * 86_400_000_000_000
    {:ok, DateTime.from_unix!(nanos + epoch_nanos, :nanosecond), rest}
  end

  defp decode_typed_item(:month, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:month, <<months::little-signed-32, rest::binary>>) do
    year = 2000 + div(months, 12)
    month = rem(months, 12) + 1
    {:ok, Date.new!(year, month, 1), rest}
  end

  defp decode_typed_item(:date, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:date, <<days::little-signed-32, rest::binary>>) do
    {:ok, Date.add(~D[2000-01-01], days), rest}
  end

  defp decode_typed_item(:datetime, <<days::little-float-64, rest::binary>>) when days == days do
    millis = round((days + @kdb_epoch_days) * 86_400_000)
    {:ok, DateTime.from_unix!(millis, :millisecond), rest}
  end

  defp decode_typed_item(:datetime, <<_::binary-8, rest::binary>>), do: {:ok, nil, rest}

  defp decode_typed_item(:timespan, <<@kdb_null_long::little-signed-64, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:timespan, <<nanos::little-signed-64, rest::binary>>),
    do: {:ok, nanos, rest}

  defp decode_typed_item(:minute, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:minute, <<minutes::little-signed-32, rest::binary>>) do
    {:ok, Time.new!(div(minutes, 60), rem(minutes, 60), 0), rest}
  end

  defp decode_typed_item(:second, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:second, <<seconds::little-signed-32, rest::binary>>) do
    {:ok, Time.new!(div(seconds, 3_600), rem(div(seconds, 60), 60), rem(seconds, 60)), rest}
  end

  defp decode_typed_item(:time, <<@kdb_null_int::little-signed-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp decode_typed_item(:time, <<ms::little-signed-32, rest::binary>>) do
    hours = div(ms, 3_600_000)
    rem1 = rem(ms, 3_600_000)
    minutes = div(rem1, 60_000)
    rem2 = rem(rem1, 60_000)
    seconds = div(rem2, 1_000)
    microseconds = rem(rem2, 1_000) * 1_000
    {:ok, Time.new!(hours, minutes, seconds, {microseconds, 3}), rest}
  end

  defp decode_typed_item(type, data) do
    {:error, {:unsupported_typed_item, type, byte_size(data)}}
  end

  defp decode_symbol_list_items(rest, 0, acc) do
    {:ok, Enum.reverse(acc), rest}
  end

  defp decode_symbol_list_items(data, count, acc) do
    case split_null_terminated(data) do
      {:ok, sym, rest} -> decode_symbol_list_items(rest, count - 1, [sym | acc])
      error -> error
    end
  end

  defp decode_float64(raw) when (raw &&& @float64_exponent_mask) != @float64_exponent_mask do
    <<v::little-float-64>> = <<raw::little-unsigned-64>>
    v
  end

  defp decode_float64(raw) when (raw &&& @float64_mantissa_mask) != 0, do: nil
  defp decode_float64(raw) when (raw &&& @float64_sign_mask) == 0, do: :infinity
  defp decode_float64(_raw), do: :neg_infinity

  defp split_null_terminated(data) do
    case :binary.split(data, <<0>>) do
      [str, rest] -> {:ok, str, rest}
      _ -> {:error, {:decode_error, :missing_null_terminator}}
    end
  end

  @spec encode(term()) :: iodata()
  def encode(data) do
    case infer_type(data) do
      {:ok, type} -> do_encode(type, data)
      {:error, reason} -> raise "Encoding error: #{reason}"
    end
  end

  defp infer_type(data) do
    cond do
      is_binary(data) -> {:ok, :char_list}
      is_boolean(data) -> {:ok, :boolean}
      is_integer(data) -> {:ok, :long}
      is_float(data) -> {:ok, :float}
      is_atom(data) -> {:ok, :symbol}
      match?(%DateTime{}, data) -> {:ok, :timestamp}
      match?(%Date{}, data) -> {:ok, :date}
      match?(%Time{}, data) -> {:ok, :time}
      is_list(data) -> {:ok, :list}
      is_map(data) and not is_struct(data) -> {:ok, :dictionary}
      true -> {:error, "Unsupported type: #{inspect(data)}"}
    end
  end

  defp do_encode(:char_list, str) when is_binary(str) do
    <<10::8, 0::8, byte_size(str)::little-32, str::binary>>
  end

  defp do_encode(:boolean, true), do: <<-1::signed-8, 1::8>>
  defp do_encode(:boolean, false), do: <<-1::signed-8, 0::8>>

  defp do_encode(:long, n) when is_integer(n), do: <<-7::signed-8, n::little-signed-64>>

  defp do_encode(:float, n) when is_float(n), do: <<-9::signed-8, n::little-float-64>>

  defp do_encode(:symbol, atom) when is_atom(atom) do
    str = Atom.to_string(atom)
    <<-11::signed-8, str::binary, 0::8>>
  end

  defp do_encode(:list, list) when is_list(list) do
    encoded = Enum.map(list, &encode/1)
    [<<0::8, 0::8, length(list)::little-32>> | encoded]
  end

  defp do_encode(:dictionary, map) when is_map(map) do
    {keys, values} = Enum.unzip(map)
    [<<99::8>>, encode(keys), encode(values)]
  end

  defp do_encode(:timestamp, %DateTime{} = dt) do
    epoch_unix_nanos = 946_684_800_000_000_000
    nanos = DateTime.to_unix(dt, :nanosecond) - epoch_unix_nanos
    <<-12::signed-8, nanos::little-signed-64>>
  end

  defp do_encode(:date, %Date{} = date) do
    days = Date.diff(date, ~D[2000-01-01])
    <<-14::signed-8, days::little-signed-32>>
  end

  defp do_encode(:time, %Time{} = t) do
    {microseconds, _} = t.microsecond
    ms = (t.hour * 3_600 + t.minute * 60 + t.second) * 1_000 + div(microseconds, 1_000)
    <<-19::signed-8, ms::little-signed-32>>
  end
end
