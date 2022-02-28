defmodule AvroEx.Decode do
  @moduledoc false

  require Bitwise
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  @doc false
  @spec decode(AvroEx.Schema.t(), binary()) :: {:ok, any()}
  def decode(%Schema{schema: schema, context: context}, avro_message)
      when is_binary(avro_message) do
    {value, ""} = do_decode(schema, context, avro_message)
    {:ok, value}
  end

  defp do_decode(name, %Context{} = context, data) when is_binary(name) do
    do_decode(Context.lookup(context, name), context, data)
  end

  defp do_decode(%Primitive{type: :null}, %Context{}, data) when is_binary(data) do
    {nil, data}
  end

  defp do_decode(%Primitive{type: :boolean}, %Context{}, <<0::8, rest::binary>>) do
    {false, rest}
  end

  defp do_decode(%Primitive{type: :boolean}, %Context{}, <<1::8, rest::binary>>) do
    {true, rest}
  end

  defp do_decode(
         %Primitive{type: :int, metadata: %{"logicalType" => "time-millis"}},
         %Context{},
         data
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 32)
    milliseconds = zigzag_decode(val)

    {:ok, midnight} = Time.new(0, 0, 0)
    time = Time.add(midnight, milliseconds, :millisecond)

    {time, rest}
  end

  defp do_decode(%Primitive{type: :int}, %Context{}, data) when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 32)
    {zigzag_decode(val), rest}
  end

  defp do_decode(
         %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}},
         %Context{},
         data
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    microseconds = zigzag_decode(val)

    {:ok, midnight} = Time.new(0, 0, 0)
    time = Time.add(midnight, microseconds, :microsecond)

    {time, rest}
  end

  defp do_decode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}},
         %Context{},
         data
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    nanoseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(nanoseconds, :nanosecond)
    {date_time, rest}
  end

  defp do_decode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}},
         %Context{},
         data
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    microseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(microseconds, :microsecond)
    {date_time, rest}
  end

  defp do_decode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}},
         %Context{},
         data
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    milliseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(milliseconds, :millisecond)
    {date_time, rest}
  end

  defp do_decode(%Primitive{type: :long}, %Context{}, data) when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    {zigzag_decode(val), rest}
  end

  defp do_decode(%Primitive{type: :float}, %Context{}, data) when is_binary(data) do
    <<float::little-float-size(32), rest::binary>> = data
    {float, rest}
  end

  defp do_decode(%Primitive{type: :double}, %Context{}, data) when is_binary(data) do
    <<float::little-float-size(64), rest::binary>> = data
    {float, rest}
  end

  defp do_decode(%Primitive{type: :bytes}, %Context{} = context, data) when is_binary(data) do
    {byte_count, buffer} = do_decode(%Primitive{type: :long}, context, data)
    bit_count = byte_count * 8

    <<bytes::bitstring-size(bit_count), rest::binary>> = buffer

    {bytes, rest}
  end

  defp do_decode(%Primitive{type: :string}, %Context{} = context, data) when is_binary(data) do
    {str, rest} = do_decode(%Primitive{type: :bytes}, context, data)

    if String.valid?(str) do
      {str, rest}
    else
      raise "Decoding string failed - not a valid UTF-8 string"
    end
  end

  defp do_decode(%Record{} = record, %Context{} = context, data) when is_binary(data) do
    {decoded, buffer} =
      Enum.reduce(record.fields, {[], data}, fn field, {decoded, buffer} ->
        {val, buff} = do_decode(field, context, buffer)
        {[val | decoded], buff}
      end)

    decoded_map =
      decoded
      |> Enum.reverse()
      |> Enum.zip(record.fields)
      |> Enum.map(fn {val, %Field{name: name}} ->
        {name, val}
      end)
      |> Map.new()

    {decoded_map, buffer}
  end

  defp do_decode(%Field{type: type}, %Context{} = context, data) when is_binary(data) do
    do_decode(type, context, data)
  end

  defp do_decode(%Union{possibilities: possibilities}, %Context{} = context, data)
       when is_binary(data) do
    {index, rest} = do_decode(%Primitive{type: :long}, context, data)
    schema = :lists.nth(index + 1, possibilities)

    do_decode(schema, context, rest)
  end

  defp do_decode(%Array{items: item_schema}, %Context{} = context, data) when is_binary(data) do
    {count, buffer} = do_decode(%Primitive{type: :long}, context, data)

    if count > 0 do
      {decoded_items, rest} =
        Enum.reduce(1..count, {[], buffer}, fn _, {decoded_items, buffer} ->
          {decoded_item, buffer} = do_decode(item_schema, context, buffer)
          {[decoded_item | decoded_items], buffer}
        end)

      {Enum.reverse(decoded_items), String.slice(rest, 1..-1)}
    else
      {[], buffer}
    end
  end

  defp do_decode(%AvroEx.Schema.Map{values: value_schema}, %Context{} = context, data) when is_binary(data) do
    {count, buffer} = do_decode(%Primitive{type: :long}, context, data)
    string_schema = %Primitive{type: :string}

    if count > 0 do
      {decoded_values, rest} =
        Enum.reduce(1..count, {[], buffer}, fn _, {decoded_values, buffer} ->
          {decoded_key, buffer} = do_decode(string_schema, context, buffer)
          {decoded_value, buffer} = do_decode(value_schema, context, buffer)
          {[{decoded_key, decoded_value} | decoded_values], buffer}
        end)

      {Map.new(decoded_values), String.slice(rest, 1..-1)}
    else
      {%{}, buffer}
    end
  end

  defp do_decode(%AvroEx.Schema.Enum{symbols: symbols}, %Context{} = context, data) when is_binary(data) do
    {index, rest} = do_decode(%Primitive{type: :long}, context, data)
    {:lists.nth(index + 1, symbols), rest}
  end

  defp do_decode(%Fixed{size: size}, %Context{}, data) when is_binary(data) do
    <<fixed::binary-size(size), rest::binary>> = data
    {fixed, rest}
  end

  @doc false
  @spec zigzag_decode(integer()) :: integer()
  def zigzag_decode(int) do
    int
    |> Bitwise.bsr(1)
    |> Bitwise.bxor(-Bitwise.band(int, 1))
  end

  @doc false
  @spec variable_integer_decode(bitstring(), integer(), integer(), integer()) :: {integer(), bitstring()}
  def variable_integer_decode(<<tag::1, value::7, tail::bitstring>>, acc, acc_bits, max_bits) do
    # assertion
    true = acc_bits < max_bits

    new_acc =
      value
      |> Bitwise.bsl(acc_bits)
      |> Bitwise.bor(acc)

    case tag do
      0 -> {new_acc, tail}
      1 -> variable_integer_decode(tail, new_acc, acc_bits + 7, max_bits)
    end
  end
end
