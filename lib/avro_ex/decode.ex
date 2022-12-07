defmodule AvroEx.Decode do
  @moduledoc false

  require Bitwise
  alias AvroEx.{DecodeError}
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  @seconds_in_day 24 * 60 * 60

  @doc false
  @spec decode(AvroEx.Schema.t(), binary(), keyword()) :: {:ok, any(), binary()} | {:error, AvroEx.DecodeError.t()}
  def decode(%Schema{schema: schema, context: context}, avro_message, opts \\ [])
      when is_binary(avro_message) do
    try do
      {value, rest} = do_decode(schema, context, avro_message, opts)
      {:ok, value, rest}
    catch
      :throw, %DecodeError{} = e -> {:error, e}
    end
  end

  defp do_decode(%Reference{type: name}, %Context{} = context, data, opts) do
    do_decode(Context.lookup(context, name), context, data, opts)
  end

  defp do_decode(%Primitive{type: :null}, %Context{}, data, _) when is_binary(data) do
    {nil, data}
  end

  defp do_decode(%Primitive{type: :boolean}, %Context{}, <<0::8, rest::binary>>, _) do
    {false, rest}
  end

  defp do_decode(%Primitive{type: :boolean}, %Context{}, <<1::8, rest::binary>>, _) do
    {true, rest}
  end

  defp do_decode(%Primitive{type: :int, metadata: %{"logicalType" => "date"}}, %Context{}, data, _) do
    {val, rest} = variable_integer_decode(data, 0, 0, 32)

    {:ok, datetime} = DateTime.from_unix(@seconds_in_day * zigzag_decode(val))
    date = DateTime.to_date(datetime)

    {date, rest}
  end

  defp do_decode(
         %Primitive{type: :int, metadata: %{"logicalType" => "time-millis"}},
         %Context{},
         data,
         _
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 32)
    milliseconds = zigzag_decode(val)

    {:ok, midnight} = Time.new(0, 0, 0)
    time = Time.add(midnight, milliseconds, :millisecond)

    {time, rest}
  end

  defp do_decode(%Primitive{type: :int}, %Context{}, data, _) when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 32)
    {zigzag_decode(val), rest}
  end

  defp do_decode(
         %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}},
         %Context{},
         data,
         _
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
         data,
         _
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
         data,
         _
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
         data,
         _
       )
       when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    milliseconds = zigzag_decode(val)
    {:ok, date_time} = DateTime.from_unix(milliseconds, :millisecond)
    {date_time, rest}
  end

  defp do_decode(%Primitive{type: :long}, %Context{}, data, _) when is_binary(data) do
    {val, rest} = variable_integer_decode(data, 0, 0, 64)
    {zigzag_decode(val), rest}
  end

  defp do_decode(%Primitive{type: :float}, %Context{}, data, _) when is_binary(data) do
    <<float::little-float-size(32), rest::binary>> = data
    {float, rest}
  end

  defp do_decode(%Primitive{type: :double}, %Context{}, data, _) when is_binary(data) do
    <<float::little-float-size(64), rest::binary>> = data
    {float, rest}
  end

  defp do_decode(%Primitive{type: :bytes}, %Context{} = context, data, opts) when is_binary(data) do
    {byte_count, buffer} = do_decode(%Primitive{type: :long}, context, data, opts)
    bit_count = byte_count * 8

    <<bytes::bitstring-size(bit_count), rest::binary>> = buffer

    {bytes, rest}
  end

  defp do_decode(%Primitive{type: :string}, %Context{} = context, data, opts) when is_binary(data) do
    {str, rest} = do_decode(%Primitive{type: :bytes}, context, data, opts)

    if String.valid?(str) do
      {str, rest}
    else
      error({:invalid_string, str})
    end
  end

  defp do_decode(%Record{} = record, %Context{} = context, data, opts) when is_binary(data) do
    {decoded, buffer} =
      Enum.reduce(record.fields, {[], data}, fn field, {decoded, buffer} ->
        {val, buff} = do_decode(field, context, buffer, opts)
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

  defp do_decode(%Field{type: type}, %Context{} = context, data, opts) when is_binary(data) do
    do_decode(type, context, data, opts)
  end

  defp do_decode(%Union{possibilities: possibilities}, %Context{} = context, data, opts)
       when is_binary(data) do
    {index, index_rest} = do_decode(%Primitive{type: :long}, context, data, opts)
    schema = :lists.nth(index + 1, possibilities)

    {decoded_item, rest} = do_decode(schema, context, index_rest, opts)

    if Keyword.get(opts, :tagged_unions, false) and Map.has_key?(schema, :name) do
      {{schema.name, decoded_item}, rest}
    else
      {decoded_item, rest}
    end
  end

  defp do_decode(%Array{items: item_schema}, %Context{} = context, data, opts) when is_binary(data) do
    {count, buffer} =
      with {count, rest} when count < 0 <-
             do_decode(%Primitive{type: :long}, context, data, opts) do
        {_byte_size, buffer} = do_decode(%Primitive{type: :long}, context, rest, opts)
        {abs(count), buffer}
      end

    if count > 0 do
      {decoded_items, rest} =
        Enum.reduce(1..count, {[], buffer}, fn _, {decoded_items, buffer} ->
          {decoded_item, buffer} = do_decode(item_schema, context, buffer, opts)
          {[decoded_item | decoded_items], buffer}
        end)

      {Enum.reverse(decoded_items), String.slice(rest, 1..-1)}
    else
      {[], buffer}
    end
  end

  defp do_decode(%AvroEx.Schema.Map{values: value_schema}, %Context{} = context, data, opts) when is_binary(data) do
    {count, buffer} = do_decode(%Primitive{type: :long}, context, data, opts)
    string_schema = %Primitive{type: :string}

    if count > 0 do
      {decoded_values, rest} =
        Enum.reduce(1..count, {[], buffer}, fn _, {decoded_values, buffer} ->
          {decoded_key, buffer} = do_decode(string_schema, context, buffer, opts)
          {decoded_value, buffer} = do_decode(value_schema, context, buffer, opts)
          {[{decoded_key, decoded_value} | decoded_values], buffer}
        end)

      {Map.new(decoded_values), String.slice(rest, 1..-1)}
    else
      {%{}, buffer}
    end
  end

  defp do_decode(%AvroEx.Schema.Enum{symbols: symbols}, %Context{} = context, data, opts) when is_binary(data) do
    {index, rest} = do_decode(%Primitive{type: :long}, context, data, opts)
    {:lists.nth(index + 1, symbols), rest}
  end

  defp do_decode(%Fixed{size: size}, %Context{}, data, _) when is_binary(data) do
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

  @compile {:inline, error: 1}
  defp error(error) do
    error |> AvroEx.DecodeError.new() |> throw()
  end
end
