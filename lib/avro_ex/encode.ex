defmodule AvroEx.Encode do
  @moduledoc false

  require Bitwise

  alias AvroEx.EncodeError
  alias AvroEx.{Schema}
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  @seconds_in_day 24 * 60 * 60

  @doc false
  @spec encode(Schema.t(), term) :: {:ok, AvroEx.encoded_avro()} | {:error, EncodeError.t() | Exception.t()}
  def encode(%Schema{context: %Context{} = context, schema: schema}, data) do
    try do
      {:ok, do_encode(schema, context, data)}
    catch
      :throw, %EncodeError{} = e -> {:error, e}
    end
  end

  defp do_encode(%Reference{type: type}, %Context{} = context, data) do
    do_encode(Context.lookup(context, type), context, data)
  end

  defp do_encode(%Primitive{type: :boolean}, %Context{}, true), do: <<1::8>>
  defp do_encode(%Primitive{type: :boolean}, %Context{}, false), do: <<0::8>>
  defp do_encode(%Primitive{type: :null}, %Context{}, nil), do: <<>>

  defp do_encode(%Primitive{type: :float}, %Context{}, float) when is_float(float),
    do: <<float::little-float-size(32)>>

  defp do_encode(%Primitive{type: :double}, %Context{}, double) when is_float(double),
    do: <<double::little-float-size(64)>>

  defp do_encode(
         %Primitive{type: :int, metadata: %{"logicalType" => "date"}} = schema,
         %Context{},
         %Date{} = date
       ) do
    date
    |> DateTime.new!(~T[00:00:00])
    |> DateTime.to_unix(:second)
    |> Kernel.div(@seconds_in_day)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}} = schema,
         %Context{},
         %DateTime{} = dt
       ) do
    dt
    |> DateTime.to_unix(:nanosecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}} = schema,
         %Context{},
         %DateTime{} = dt
       ) do
    dt
    |> DateTime.to_unix(:microsecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}} = schema,
         %Context{},
         %DateTime{} = dt
       ) do
    dt
    |> DateTime.to_unix(:millisecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}} = schema,
         %Context{},
         %Time{} = dt
       ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :microsecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :int, metadata: %{"logicalType" => "time-millis"}} = schema,
         %Context{},
         %Time{} = dt
       ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :millisecond)
    |> encode_integer(schema)
  end

  defp do_encode(%Primitive{type: :long} = schema, %Context{}, long) when is_integer(long) do
    encode_integer(long, schema)
  end

  defp do_encode(%Primitive{type: :int} = schema, %Context{}, integer)
       when is_integer(integer) do
    encode_integer(integer, schema)
  end

  defp do_encode(%Primitive{type: :string} = primitive, %Context{} = context, atom)
       when is_atom(atom) and not (is_nil(atom) or is_boolean(atom)) do
    do_encode(primitive, context, to_string(atom))
  end

  defp do_encode(%Primitive{type: :string}, %Context{} = context, str) when is_binary(str) do
    if String.valid?(str) do
      do_encode(%Primitive{type: :bytes}, context, str)
    else
      error({:invalid_string, str, context})
    end
  end

  defp do_encode(%Primitive{type: :bytes}, %Context{} = context, bin) when is_binary(bin) do
    byte_count = :erlang.size(bin)
    size = do_encode(%Primitive{type: :long}, context, byte_count)
    size <> bin
  end

  defp do_encode(%Fixed{size: size}, %Context{}, bin)
       when is_binary(bin) and byte_size(bin) == size do
    bin
  end

  defp do_encode(%Fixed{} = fixed, %Context{} = context, bin) when is_binary(bin) do
    error({:incorrect_fixed_size, fixed, bin, context})
  end

  defp do_encode(%Record{fields: fields}, %Context{} = context, record) when is_map(record) do
    record =
      Map.new(record, fn
        {k, v} when is_binary(k) -> {k, v}
        {k, v} when is_atom(k) -> {to_string(k), v}
      end)

    Enum.map_join(fields, &do_encode(&1, context, record[&1.name]))
  end

  defp do_encode(%Field{type: type, default: default}, %Context{} = context, nil) do
    do_encode(type, context, default)
  end

  defp do_encode(%Field{type: type}, %Context{} = context, value) do
    do_encode(type, context, value)
  end

  defp do_encode(%Union{possibilities: possibilities} = schema, %Context{} = context, {name, value} = original) do
    index =
      Enum.find_index(possibilities, fn
        %{name: ^name} = possible_schema -> Schema.encodable?(possible_schema, context, value)
        _ -> false
      end)

    do_encode_union(schema, context, value, original, index)
  end

  defp do_encode(%Union{possibilities: possibilities} = schema, %Context{} = context, value) do
    index =
      Enum.find_index(possibilities, fn possible_schema ->
        Schema.encodable?(possible_schema, context, value)
      end)

    do_encode_union(schema, context, value, value, index)
  end

  defp do_encode(%AvroEx.Schema.Map{values: values}, %Context{} = context, map) when is_map(map) do
    case map_size(map) do
      0 ->
        <<0>>

      size ->
        acc = do_encode(%Primitive{type: :long}, context, size)

        encoded_map =
          Enum.reduce(map, acc, fn {k, v}, acc ->
            key = do_encode(%Primitive{type: :string}, context, k)
            value = do_encode(values, context, v)

            acc <> key <> value
          end)

        encoded_map <> <<0>>
    end
  end

  defp do_encode(%Array{items: items}, %Context{} = context, data) when is_list(data) do
    case length(data) do
      0 ->
        <<0>>

      size ->
        acc = do_encode(%Primitive{type: :long}, context, size)

        encoded_array =
          Enum.reduce(data, acc, fn v, acc ->
            value = do_encode(items, context, v)

            acc <> value
          end)

        encoded_array <> <<0>>
    end
  end

  defp do_encode(%AvroEnum{} = enum, %Context{} = context, atom) when is_atom(atom) do
    do_encode(enum, context, to_string(atom))
  end

  defp do_encode(%AvroEnum{symbols: symbols} = enum, %Context{} = context, data) when is_binary(data) do
    if data in symbols do
      index = Enum.find_index(symbols, fn e -> e == data end)
      do_encode(%Primitive{type: :long}, context, index)
    else
      error({:invalid_symbol, enum, data, context})
    end
  end

  defp do_encode(schema, context, data) do
    error({:schema_mismatch, schema, data, context})
  end

  defp do_encode_union(%Union{possibilities: possibilities} = schema, %Context{} = context, value, original, index) do
    if index do
      schema = Enum.at(possibilities, index)

      do_encode(%Primitive{type: :int}, context, index) <> do_encode(schema, context, value)
    else
      error({:schema_mismatch, schema, original, context})
    end
  end

  @doc false
  @spec zigzag_encode(Primitive.t(), integer) :: integer
  def zigzag_encode(%Primitive{type: :int}, int) when is_integer(int) do
    int
    |> Bitwise.bsl(1)
    |> Bitwise.bxor(Bitwise.bsr(int, 31))
  end

  def zigzag_encode(%Primitive{type: :long}, long) when is_integer(long) do
    long
    |> Bitwise.bsl(1)
    |> Bitwise.bxor(Bitwise.bsr(long, 63))
  end

  @doc false
  @spec variable_integer_encode(integer()) :: <<_::8, _::_*8>>
  def variable_integer_encode(value) when value <= 127, do: <<value>>

  def variable_integer_encode(value) do
    <<128 + Bitwise.band(value, 127)>> <> variable_integer_encode(Bitwise.bsr(value, 7))
  end

  defp encode_integer(int, schema) do
    schema
    |> zigzag_encode(int)
    |> variable_integer_encode
  end

  @compile {:inline, error: 1}
  defp error(error) do
    error |> AvroEx.EncodeError.new() |> throw()
  end
end
