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
  @spec encode(Schema.t(), term, keyword()) :: {:ok, AvroEx.encoded_avro()} | {:error, EncodeError.t() | Exception.t()}
  def encode(%Schema{context: %Context{} = context, schema: schema}, data, opts \\ []) do
    try do
      {:ok, do_encode(schema, context, data, opts)}
    catch
      :throw, %EncodeError{} = e -> {:error, e}
    end
  end

  defp do_encode(%Reference{type: type}, %Context{} = context, data, opts) do
    do_encode(Context.lookup(context, type), context, data, opts)
  end

  defp do_encode(%Primitive{type: :boolean}, %Context{}, true, _), do: <<1::8>>
  defp do_encode(%Primitive{type: :boolean}, %Context{}, false, _), do: <<0::8>>
  defp do_encode(%Primitive{type: :null}, %Context{}, nil, _), do: <<>>

  defp do_encode(%Primitive{type: :float}, %Context{}, float, _) when is_float(float),
    do: <<float::little-float-size(32)>>

  defp do_encode(%Primitive{type: :double}, %Context{}, double, _) when is_float(double),
    do: <<double::little-float-size(64)>>

  defp do_encode(
         %Primitive{type: :int, metadata: %{"logicalType" => "date"}} = schema,
         %Context{},
         %Date{} = date,
         _
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
         %DateTime{} = dt,
         _
       ) do
    dt
    |> DateTime.to_unix(:nanosecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}} = schema,
         %Context{},
         %DateTime{} = dt,
         _
       ) do
    dt
    |> DateTime.to_unix(:microsecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}} = schema,
         %Context{},
         %DateTime{} = dt,
         _
       ) do
    dt
    |> DateTime.to_unix(:millisecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}} = schema,
         %Context{},
         %Time{} = dt,
         _
       ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :microsecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :int, metadata: %{"logicalType" => "time-millis"}} = schema,
         %Context{},
         %Time{} = dt,
         _
       ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :millisecond)
    |> encode_integer(schema)
  end

  defp do_encode(
         %Primitive{type: :bytes, metadata: %{"logicalType" => "decimal"} = metadata},
         %Context{} = context,
         value,
         opts
       ) do
    scale = Map.get(metadata, "scale", 0)

    unscaled =
      case value do
        value when is_number(value) ->
          trunc(value / :math.pow(10, -scale))

        %struct{} when struct == Decimal ->
          if value.exp != -scale do
            error({:incompatible_decimal, -scale, value.exp})
          end

          value.coef * value.sign
      end

    number_of_bits = value_size(unscaled)

    bin = <<unscaled::big-signed-integer-size(number_of_bits)>>
    do_encode(%Primitive{type: :bytes}, context, bin, opts)
  end

  defp do_encode(%Primitive{type: :long} = schema, %Context{}, long, _) when is_integer(long) do
    encode_integer(long, schema)
  end

  defp do_encode(%Primitive{type: :int} = schema, %Context{}, integer, _)
       when is_integer(integer) do
    encode_integer(integer, schema)
  end

  defp do_encode(%Primitive{type: :string} = primitive, %Context{} = context, atom, opts)
       when is_atom(atom) and not (is_nil(atom) or is_boolean(atom)) do
    do_encode(primitive, context, to_string(atom), opts)
  end

  defp do_encode(%Primitive{type: :string}, %Context{} = context, str, opts) when is_binary(str) do
    if String.valid?(str) do
      do_encode(%Primitive{type: :bytes}, context, str, opts)
    else
      error({:invalid_string, str, context})
    end
  end

  defp do_encode(%Primitive{type: :bytes}, %Context{} = context, bin, opts) when is_binary(bin) do
    byte_count = :erlang.size(bin)
    size = do_encode(%Primitive{type: :long}, context, byte_count, opts)
    size <> bin
  end

  defp do_encode(%Fixed{size: size}, %Context{}, bin, _)
       when is_binary(bin) and byte_size(bin) == size do
    bin
  end

  defp do_encode(%Fixed{} = fixed, %Context{} = context, bin, _) when is_binary(bin) do
    error({:incorrect_fixed_size, fixed, bin, context})
  end

  defp do_encode(%Record{fields: fields}, %Context{} = context, record, opts) when is_map(record) do
    record =
      Map.new(record, fn
        {k, v} when is_binary(k) -> {k, v}
        {k, v} when is_atom(k) -> {to_string(k), v}
      end)

    Enum.map_join(fields, &do_encode(&1, context, record[&1.name], opts))
  end

  defp do_encode(%Field{type: type, default: default}, %Context{} = context, nil, opts) do
    do_encode(type, context, default, opts)
  end

  defp do_encode(%Field{type: type}, %Context{} = context, value, opts) do
    do_encode(type, context, value, opts)
  end

  defp do_encode(%Union{possibilities: possibilities} = schema, %Context{} = context, {name, value} = original, opts) do
    index =
      Enum.find_index(possibilities, fn
        %{name: ^name} = possible_schema -> Schema.encodable?(possible_schema, context, value)
        _ -> false
      end)

    do_encode_union(schema, context, value, original, index, opts)
  end

  defp do_encode(%Union{possibilities: possibilities} = schema, %Context{} = context, value, opts) do
    index =
      Enum.find_index(possibilities, fn possible_schema ->
        Schema.encodable?(possible_schema, context, value)
      end)

    do_encode_union(schema, context, value, value, index, opts)
  end

  defp do_encode(%AvroEx.Schema.Map{values: values}, %Context{} = context, map, opts) when is_map(map) do
    case map_size(map) do
      0 ->
        <<0>>

      size ->
        acc = do_encode(%Primitive{type: :long}, context, size, opts)

        encoded_map =
          Enum.reduce(map, acc, fn {k, v}, acc ->
            key = do_encode(%Primitive{type: :string}, context, k, opts)
            value = do_encode(values, context, v, opts)

            acc <> key <> value
          end)

        encoded_map <> <<0>>
    end
  end

  defp do_encode(%Array{items: items}, %Context{} = context, data, opts) when is_list(data) do
    case length(data) do
      0 ->
        <<0>>

      size ->
        array_payload =
          Enum.reduce(data, <<>>, fn v, acc ->
            value = do_encode(items, context, v, opts)

            acc <> value
          end)

        header =
          if Keyword.get(opts, :include_block_byte_size, false) do
            negated_count = do_encode(%Primitive{type: :long}, context, -1 * size, opts)
            byte_size = do_encode(%Primitive{type: :long}, context, byte_size(array_payload), opts)
            negated_count <> byte_size
          else
            do_encode(%Primitive{type: :long}, context, size, opts)
          end

        header <> array_payload <> <<0>>
    end
  end

  defp do_encode(%AvroEnum{} = enum, %Context{} = context, atom, opts) when is_atom(atom) do
    do_encode(enum, context, to_string(atom), opts)
  end

  defp do_encode(%AvroEnum{symbols: symbols} = enum, %Context{} = context, data, opts) when is_binary(data) do
    if data in symbols do
      index = Enum.find_index(symbols, fn e -> e == data end)
      do_encode(%Primitive{type: :long}, context, index, opts)
    else
      error({:invalid_symbol, enum, data, context})
    end
  end

  defp do_encode(schema, context, data, _) do
    error({:schema_mismatch, schema, data, context})
  end

  defp do_encode_union(
         %Union{possibilities: possibilities} = schema,
         %Context{} = context,
         value,
         original,
         index,
         opts
       ) do
    if index do
      schema = Enum.at(possibilities, index)

      do_encode(%Primitive{type: :int}, context, index, opts) <> do_encode(schema, context, value, opts)
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

  defp value_size(value, bits \\ 8) when is_number(value) do
    if :math.pow(2, bits) > abs(value) do
      bits
    else
      value_size(value, bits + 8)
    end
  end

  @compile {:inline, error: 1}
  defp error(error) do
    error |> AvroEx.EncodeError.new() |> throw()
  end
end
