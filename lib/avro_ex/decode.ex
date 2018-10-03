defmodule AvroEx.Decode do
  require Bitwise
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  def decode(%Schema{schema: schema, context: context}, avro_message)
      when is_binary(avro_message) do
    {value, ""} = do_decode(schema, context, avro_message)
    {:ok, value}
  end

  def do_decode(name, %Context{} = context, data) when is_binary(name) do
    do_decode(Context.lookup(context, name), context, data)
  end

  def do_decode(%Primitive{type: nil}, %Context{}, data) when is_binary(data) do
    {nil, data}
  end

  def do_decode(%Primitive{type: :boolean}, %Context{}, <<0::8, rest::binary>>) do
    {false, rest}
  end

  def do_decode(%Primitive{type: :boolean}, %Context{}, <<1::8, rest::binary>>) do
    {true, rest}
  end

  def do_decode(
        %Primitive{type: :integer, metadata: %{"logicalType" => "time-millis"}} = type,
        %Context{},
        data
      )
      when is_binary(data) do
    {<<val::32>>, rest} = variable_integer_decode(data, <<>>, type)
    milliseconds = zigzag_decode(val)

    {:ok, midnight} = Time.new(0, 0, 0)
    time = Time.add(midnight, milliseconds, :millisecond)

    {time, rest}
  end

  def do_decode(%Primitive{type: :integer} = type, %Context{}, data) when is_binary(data) do
    {<<val::32>>, rest} = variable_integer_decode(data, <<>>, type)
    {zigzag_decode(val), rest}
  end

  def do_decode(
        %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}} = type,
        %Context{},
        data
      )
      when is_binary(data) do
    {<<val::64>>, rest} = variable_integer_decode(data, <<>>, type)
    microseconds = zigzag_decode(val)

    {:ok, midnight} = Time.new(0, 0, 0)
    time = Time.add(midnight, microseconds, :microsecond)

    {time, rest}
  end

  def do_decode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}} = type,
        %Context{},
        data
      )
      when is_binary(data) do
    {<<val::64>>, rest} = variable_integer_decode(data, <<>>, type)
    nanoseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(nanoseconds, :nanosecond)
    {date_time, rest}
  end

  def do_decode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}} = type,
        %Context{},
        data
      )
      when is_binary(data) do
    {<<val::64>>, rest} = variable_integer_decode(data, <<>>, type)
    microseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(microseconds, :microsecond)
    {date_time, rest}
  end

  def do_decode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}} = type,
        %Context{},
        data
      )
      when is_binary(data) do
    {<<val::64>>, rest} = variable_integer_decode(data, <<>>, type)
    milliseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(milliseconds, :millisecond)
    {date_time, rest}
  end

  def do_decode(%Primitive{type: :long} = type, %Context{}, data) when is_binary(data) do
    {<<val::64>>, rest} = variable_integer_decode(data, <<>>, type)
    {zigzag_decode(val), rest}
  end

  def do_decode(%Primitive{type: :float}, %Context{}, data) when is_binary(data) do
    <<float::little-float-size(32), rest::binary>> = data
    {float, rest}
  end

  def do_decode(%Primitive{type: :double}, %Context{}, data) when is_binary(data) do
    <<float::little-float-size(64), rest::binary>> = data
    {float, rest}
  end

  def do_decode(%Primitive{type: :bytes}, %Context{} = context, data) when is_binary(data) do
    {byte_count, buffer} = do_decode(%Primitive{type: :long}, context, data)
    bit_count = byte_count * 8

    <<bytes::bitstring-size(bit_count), rest::binary>> = buffer

    {bytes, rest}
  end

  def do_decode(%Primitive{type: :string}, %Context{} = context, data) when is_binary(data) do
    {str, rest} = do_decode(%Primitive{type: :bytes}, context, data)

    if String.valid?(str) do
      {str, rest}
    else
      raise "Decoding string failed - not a valid UTF-8 string"
    end
  end

  def do_decode(%Record{} = record, %Context{} = context, data) when is_binary(data) do
    {decoded, buffer} =
      Enum.reduce(record.fields, {[], data}, fn field, {decoded, buffer} ->
        {val, buff} = do_decode(field, context, buffer)
        {[val | decoded], buff}
      end)

    decoded =
      decoded
      |> Enum.reverse()
      |> Enum.zip(record.fields)
      |> Enum.map(fn {val, %Field{name: name}} ->
        {name, val}
      end)
      |> Map.new()

    {decoded, buffer}
  end

  def do_decode(%Field{type: type}, %Context{} = context, data) when is_binary(data) do
    do_decode(type, context, data)
  end

  def do_decode(%Union{possibilities: possibilities}, %Context{} = context, data)
      when is_binary(data) do
    {index, rest} = do_decode(%Primitive{type: :long}, context, data)
    schema = :lists.nth(index + 1, possibilities)

    do_decode(schema, context, rest)
  end

  def do_decode(%Array{}, _context, <<0>>), do: {[], ""}

  def do_decode(%Array{items: item_schema}, %Context{} = context, data) when is_binary(data) do
    {count, buffer} = do_decode(%Primitive{type: :long}, context, data)

    {decoded_items, rest} =
      Enum.reduce(1..count, {[], buffer}, fn _, {decoded_items, buffer} ->
        {decoded_item, buffer} = do_decode(item_schema, context, buffer)
        {[decoded_item | decoded_items], buffer}
      end)

    {Enum.reverse(decoded_items), rest}
  end

  def do_decode(%AvroEx.Schema.Map{}, _context, <<0>>), do: {%{}, ""}

  def do_decode(%AvroEx.Schema.Map{values: value_schema}, %Context{} = context, data)
      when is_binary(data) do
    {count, buffer} = do_decode(%Primitive{type: :long}, context, data)
    string_schema = %Primitive{type: :string}

    {decoded_values, rest} =
      Enum.reduce(1..count, {[], buffer}, fn _, {decoded_values, buffer} ->
        {decoded_key, buffer} = do_decode(string_schema, context, buffer)
        {decoded_value, buffer} = do_decode(value_schema, context, buffer)
        {[{decoded_key, decoded_value} | decoded_values], buffer}
      end)

    {Map.new(decoded_values), rest}
  end

  def do_decode(%AvroEx.Schema.Enum{symbols: symbols}, %Context{} = context, data)
      when is_binary(data) do
    {index, rest} = do_decode(%Primitive{type: :long}, context, data)
    {:lists.nth(index + 1, symbols), rest}
  end

  def do_decode(%Fixed{size: size}, %Context{}, data) when is_binary(data) do
    <<fixed::binary-size(size), rest::binary>> = data
    {fixed, rest}
  end

  def zigzag_decode(int) do
    int
    |> Bitwise.bsr(1)
    |> Bitwise.bxor(-(int |> Bitwise.band(1)))
  end

  def variable_integer_decode(<<0::1, n::7, rest::bitstring>>, acc, %Primitive{type: :integer})
      when is_bitstring(acc) and is_bitstring(acc) do
    leading_zero_count = 24 - bit_size(acc)
    val = <<0::size(leading_zero_count), n::8, acc::bitstring>>
    {val, rest}
  end

  def variable_integer_decode(<<0::1, n::7, rest::bitstring>>, acc, %Primitive{type: :long})
      when is_bitstring(acc) and is_bitstring(acc) do
    leading_zero_count = 56 - bit_size(acc)
    val = <<0::size(leading_zero_count), n::8, acc::bitstring>>
    {val, rest}
  end

  def variable_integer_decode(<<1::1, n::7, rest::bitstring>>, acc, type)
      when is_bitstring(acc) and is_bitstring(acc) do
    variable_integer_decode(rest, <<n::7, acc::bitstring>>, type)
  end
end
