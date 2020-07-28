defmodule AvroEx.Encode do
  require Bitwise
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Map, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  @spec encode(Schema.t(), term) ::
          {:ok, AvroEx.encoded_avro()}
          | {:error, :data_does_not_match_schema, term, Schema.t()}
          | {:error, reason}
          | {:error, reason, term}
  def encode(%Schema{context: %Context{} = context, schema: schema}, data) do
    case do_encode(schema, context, data) do
      {:error, :data_does_not_match_schema, _data, _schema} = err -> err
      {:error, _reason, _value} = err -> err
      val -> {:ok, val}
    end
  end

  @spec do_encode(any(), any(), any()) ::
          binary()
          | {:error, :incorrect_fixed_size | :invalid_string | :invalid_symbol,
             binary() | [{any(), any()}, ...] | {binary(), any()}}
          | {:error, :data_does_not_match_schema, any(), any()}
  def do_encode(name, %Context{} = context, data) when is_binary(name),
    do: do_encode(Context.lookup(context, name), context, data)

  def do_encode(%Primitive{type: :boolean}, %Context{}, true), do: <<1::8>>
  def do_encode(%Primitive{type: :boolean}, %Context{}, false), do: <<0::8>>
  def do_encode(%Primitive{type: nil}, %Context{}, nil), do: <<>>

  def do_encode(%Primitive{type: :float}, %Context{}, float) when is_float(float),
    do: <<float::little-float-size(32)>>

  def do_encode(%Primitive{type: :double}, %Context{}, double) when is_float(double),
    do: <<double::little-float-size(64)>>

  def do_encode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}} = schema,
        %Context{},
        %DateTime{} = dt
      ) do
    dt
    |> DateTime.to_unix(:nanosecond)
    |> encode_integer(schema)
  end

  def do_encode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}} = schema,
        %Context{},
        %DateTime{} = dt
      ) do
    dt
    |> DateTime.to_unix(:microsecond)
    |> encode_integer(schema)
  end

  def do_encode(
        %Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}} = schema,
        %Context{},
        %DateTime{} = dt
      ) do
    dt
    |> DateTime.to_unix(:millisecond)
    |> encode_integer(schema)
  end

  def do_encode(
        %Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}} = schema,
        %Context{},
        %Time{} = dt
      ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :microsecond)
    |> encode_integer(schema)
  end

  def do_encode(
        %Primitive{type: :integer, metadata: %{"logicalType" => "time-millis"}} = schema,
        %Context{},
        %Time{} = dt
      ) do
    {:ok, midnight} = Time.new(0, 0, 0)

    dt
    |> Time.diff(midnight, :millisecond)
    |> encode_integer(schema)
  end

  def do_encode(%Primitive{type: :long} = schema, %Context{}, long) when is_integer(long) do
    encode_integer(long, schema)
  end

  def do_encode(%Primitive{type: :integer} = schema, %Context{}, integer)
      when is_integer(integer) do
    encode_integer(integer, schema)
  end

  def do_encode(%Primitive{type: :string}, %Context{} = context, str) when is_binary(str) do
    if String.valid?(str) do
      do_encode(%Primitive{type: :bytes}, context, str)
    else
      {:error, :invalid_string, str}
    end
  end

  def do_encode(%Primitive{type: :bytes}, %Context{} = context, bin) when is_binary(bin) do
    byte_count = :erlang.size(bin)
    size = do_encode(%Primitive{type: :long}, context, byte_count)
    size <> bin
  end

  def do_encode(%Fixed{size: size}, %Context{}, bin)
      when is_binary(bin) and byte_size(bin) == size do
    bin
  end

  def do_encode(%Fixed{size: size, name: name}, %Context{}, bin) when is_binary(bin) do
    {:error, :incorrect_fixed_size, [expected: size, got: byte_size(bin), name: name]}
  end

  def do_encode(%Record{fields: fields}, %Context{} = context, record) when is_map(record) do
    fields
    |> Enum.map(fn field -> do_encode(field, context, record[field.name]) end)
    |> Enum.join()
  end

  def do_encode(%Field{type: type, default: default}, %Context{} = context, nil) do
    do_encode(type, context, default)
  end

  def do_encode(%Field{type: type}, %Context{} = context, value) do
    do_encode(type, context, value)
  end

  def do_encode(%Union{possibilities: possibilities} = schema, %Context{} = context, value) do
    index =
      Enum.find_index(possibilities, fn possible_schema ->
        Schema.encodable?(possible_schema, context, value)
      end)

    if index do
      schema = Enum.at(possibilities, index)

      do_encode(%Primitive{type: :integer}, context, index) <> do_encode(schema, context, value)
    else
      {:error, :data_does_not_match_schema, value, schema}
    end
  end

  def do_encode(%Map{values: values}, %Context{} = context, map) when is_map(map) do
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

  def do_encode(%Array{items: items}, %Context{} = context, data) when is_list(data) do
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

  def do_encode(%AvroEnum{symbols: symbols}, %Context{} = context, data) when is_binary(data) do
    if data in symbols do
      index = Enum.find_index(symbols, fn e -> e == data end)
      do_encode(%Primitive{type: :long}, context, index)
    else
      {:error, :invalid_symbol, {data, symbols}}
    end
  end

  def do_encode(schema, _, data) do
    {:error, :data_does_not_match_schema, data, schema}
  end

  @spec zigzag_encode(Primitive.t(), integer) :: binary
  def zigzag_encode(%Primitive{type: :integer}, int) when is_integer(int) do
    int
    |> Bitwise.bsl(1)
    |> Bitwise.bxor(Bitwise.bsr(int, 31))
  end

  def zigzag_encode(%Primitive{type: :long}, long) when is_integer(long) do
    long
    |> Bitwise.bsl(1)
    |> Bitwise.bxor(Bitwise.bsr(long, 63))
  end

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
end
