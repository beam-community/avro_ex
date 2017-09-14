defmodule AvroEx.Encode do
  require Bitwise
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Map, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  @type reason :: term

  @spec encode(Schema.t, term) :: {:ok, Avro.avro} | {:error, reason} | {:error, reason, term}
  def encode(%Schema{context: %Context{} = context, schema: schema}, data) do
    if Schema.encodable?(schema, context, data) do
      case do_encode(schema, context, data) do
        {:error, _reason, _value} = err -> err
        val -> {:ok, val}
      end
    else
      {:error, "data does not match schema"}
    end
  end


  def do_encode(%Primitive{type: :boolean}, %Context{}, true), do: <<1::8>>
  def do_encode(%Primitive{type: :boolean}, %Context{}, false), do: <<0::8>>
  def do_encode(%Primitive{type: nil}, %Context{}, nil), do: <<>>
  def do_encode(%Primitive{type: :float}, %Context{}, float) when is_float(float), do: <<float::little-float-size(32)>>
  def do_encode(%Primitive{type: :double}, %Context{}, double) when is_float(double), do: <<double::little-float-size(64)>>
  def do_encode(%Primitive{type: type} = schema, %Context{}, int)
  when is_integer(int) and (type == :integer or type == :long) do
    schema
    |> zigzag_encode(int)
    |> variable_integer_encode
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

  def do_encode(%Fixed{size: size}, %Context{}, bin) when is_binary(bin) and byte_size(bin) == size do
    bin
  end

  def do_encode(%Fixed{size: size}, %Context{}, bin) when is_binary(bin) do
    {:error, :incorrect_fixed_size, [expected: size, got: byte_size(bin)]}
  end

  def do_encode(%Record{fields: fields}, %Context{} = context, record) when is_map(record) do
    fields
    |> Enum.map(fn(field) -> do_encode(field, context, record[field.name]) end)
    |> Enum.join
  end

  def do_encode(%Field{type: type}, %Context{} = context, value) do
    do_encode(type, context, value)
  end

  def do_encode(%Union{possibilities: possibilities}, %Context{} = context, value)  do
    index =
      Enum.find_index(possibilities, fn(possible_schema) ->
        Schema.encodable?(possible_schema, context, value)
      end)

    schema = Enum.at(possibilities, index)

    do_encode(%Primitive{type: :integer}, context, index) <> do_encode(schema, context, value)
  end

  def do_encode(%Map{values: values}, %Context{} = context, map) when is_map(map) do
    case map_size(map) do
      0 -> <<0>>
      size ->
        acc = do_encode(%Primitive{type: :long}, context, size)
        Enum.reduce(map, acc, fn({k, v}, acc) ->
          key = do_encode(%Primitive{type: :string}, context, k)
          value = do_encode(values, context, v)

          acc <> key <> value
        end)
    end
  end

  def do_encode(%Array{items: items}, %Context{} = context, data) when is_list(data) do
    case length(data) do
      0 -> <<0>>
      size ->
        acc = do_encode(%Primitive{type: :long}, context, size)
        Enum.reduce(data, acc, fn(v, acc) ->
          value = do_encode(items, context, v)

          acc <> value
        end)
    end
  end

  def do_encode(%AvroEnum{symbols: symbols}, %Context{} = context, data) when is_binary(data) do
    if data in symbols do
      index = Enum.find_index(symbols, fn(e) -> e == data end)
      do_encode(%Primitive{type: :long}, context, index)
    else
      {:error, :invalid_symbol, {data, symbols}}
    end
  end

  @spec zigzag_encode(Primitive.t, integer) :: binary
  def zigzag_encode(%Primitive{type: :integer}, int) when is_integer(int) do
    value =
      int
      |> Bitwise.bsl(1)
      |> Bitwise.bxor(int |> Bitwise.bsr(31))

    <<value::32>>
  end

  def zigzag_encode(%Primitive{type: :long}, long) when is_integer(long) do
    value =
      long
      |> Bitwise.bsl(1)
      |> Bitwise.bxor(long |> Bitwise.bsr(61))

    <<value::64>>
  end

  def variable_integer_encode(<<0::32>>), do: <<0::8>>
  def variable_integer_encode(<<0::25, byte::7>>), do: <<byte::8>>
  def variable_integer_encode(<<0::18, byte1::7, byte2::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte2, byte1>>
  end

  def variable_integer_encode(<<0::11, byte1::7, byte2::7, byte3::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::4, byte1::7, byte2::7, byte3::7, byte4::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<byte1::4, byte2::7, byte3::7, byte4::7, byte5::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::64>>), do: <<0::8>>
  def variable_integer_encode(<<0::57, byte1::7>>), do: <<byte1::8>>
  def variable_integer_encode(<<0::50, byte1::7, byte2::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte2, byte1>>
  end

  def variable_integer_encode(<<0::43, byte1::7, byte2::7, byte3::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::36, byte1::7, byte2::7, byte3::7, byte4::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::29, byte1::7, byte2::7, byte3::7, byte4::7, byte5::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::22, byte1::7, byte2::7, byte3::7, byte4::7, byte5::7, byte6::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte6::8>> = wrap(byte6)
    <<byte6, byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::15, byte1::7, byte2::7, byte3::7, byte4::7, byte5::7, byte6::7, byte7::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte6::8>> = wrap(byte6)
    <<byte7::8>> = wrap(byte7)
    <<byte7, byte6, byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::8, byte1::7, byte2::7, byte3::7, byte4::7, byte5::7, byte6::7, byte7::7, byte8::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte6::8>> = wrap(byte6)
    <<byte7::8>> = wrap(byte7)
    <<byte8::8>> = wrap(byte8)
    <<byte8, byte7, byte6, byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<0::1, byte1::7, byte2::7, byte3::7, byte4::7, byte5::7, byte6::7, byte7::7, byte8::7, byte9::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte6::8>> = wrap(byte6)
    <<byte7::8>> = wrap(byte7)
    <<byte8::8>> = wrap(byte8)
    <<byte9::8>> = wrap(byte9)
    <<byte9, byte8, byte7, byte6, byte5, byte4, byte3, byte2, byte1>>
  end

  def variable_integer_encode(<<byte1::1, byte2::7, byte3::7, byte4::7, byte5::7, byte6::7, byte7::7, byte8::7, byte9::7, byte10::7>>) do
    <<byte2::8>> = wrap(byte2)
    <<byte3::8>> = wrap(byte3)
    <<byte4::8>> = wrap(byte4)
    <<byte5::8>> = wrap(byte5)
    <<byte6::8>> = wrap(byte6)
    <<byte7::8>> = wrap(byte7)
    <<byte8::8>> = wrap(byte8)
    <<byte9::8>> = wrap(byte9)
    <<byte10::8>> = wrap(byte10)
    <<byte10, byte9, byte8, byte7, byte6, byte5, byte4, byte3, byte2, byte1>>
  end

  def wrap(byte), do: <<1::1, byte::7>>
end
