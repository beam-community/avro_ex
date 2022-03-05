defmodule AvroEx do
  @moduledoc """
  AvroEx is a library for encoding and decoding data with Avro schemas.
  Supports parsing schemas, encoding data, and decoding data.

  For encoding and decoding, the following type chart should be referenced:

  | Avro Types | Elixir Types |
  |------------|:------------:|
  | boolean | boolean |
  | integer | integer |
  | long | integer |
  | float | decimal |
  | double | decimal |
  | bytes | binary |
  | string | String.t, atom |
  | null | nil |
  | Record | map |
  | Enum | String.t, atom (corresponding to the enum's symbol list) |
  """
  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  @type encoded_avro :: binary

  @doc """
  Checks to see if the given data is encodable using the given schema. Helpful for unit testing.

      iex> AvroEx.encodable?(%Schema{schema: %Primitive{type: :string}}, "wut")
      true

      iex> AvroEx.encodable?(%Schema{schema: %Primitive{type: :string}}, 12345)
      false
  """
  @spec encodable?(AvroEx.Schema.t(), any) :: boolean
  defdelegate encodable?(schema, data), to: AvroEx.Schema

  @spec parse_schema(Schema.json_schema()) :: {:ok, Schema.t()} | {:error, AvroEx.Schema.DecodeError.t()}
  @deprecated "Use AvroEx.decode_schema/1 instead"
  def parse_schema(json), do: decode_schema(json)

  @spec parse_schema!(Schema.json_schema()) :: Schema.t() | no_return
  @deprecated "Use AvroEx.decode_schema!/1 instead"
  def parse_schema!(json), do: decode_schema!(json)

  @doc """
  Given an Elixir or JSON-encoded schema, parses the schema and returns a `t:AvroEx.Schema.t/0` struct representing the schema.

  Errors for invalid JSON, invalid schemas, and bad name references.

  ## Examples

      iex> AvroEx.decode_schema("string")
      {:ok, %AvroEx.Schema{schema: %AvroEx.Schema.Primitive{type: :string}}}

      iex> json= ~S({\"fields\":[{\"name\":\"a\",\"type\":\"string\"}],\"name\":\"my_type\",\"type\":\"record\"})
      iex> {:ok, %Schema{schema: record}} = AvroEx.decode_schema(json)
      iex> match?(%Record{}, record)
      true
  """
  @spec decode_schema(term()) :: {:ok, Schema.t()} | {:error, AvroEx.Schema.DecodeError.t()}
  def decode_schema(schema) do
    try do
      {:ok, decode_schema!(schema)}
    rescue
      error -> {:error, error}
    end
  end

  @doc """
  Same as `AvroEx.decode_schema/1`, but raises an exception on failure instead of
  returning an error tuple.

  ## Examples

      iex> AvroEx.decode_schema!("int")
      %AvroEx.Schema{schema: %AvroEx.Schema.Primitive{type: :int}}

  """
  @spec decode_schema!(term()) :: Schema.t()
  def decode_schema!(schema) do
    if is_binary(schema) and not Schema.Parser.primitive?(schema) do
      schema
      |> Jason.decode!()
      |> Schema.Parser.parse!()
    else
      Schema.Parser.parse!(schema)
    end
  end

  @doc """
  Given `t:AvroEx.Schema.t/0` and `term()`, takes the data and encodes it according to the schema.

  ## Examples

      iex> schema = AvroEx.decode_schema!("int")
      iex> AvroEx.encode(schema, 1234)
      {:ok, <<164, 19>>}
  """
  @spec encode(Schema.t(), term) ::
          {:ok, encoded_avro} | {:error, AvroEx.EncodeError.t() | Exception.t()}
  def encode(schema, data) do
    AvroEx.Encode.encode(schema, data)
  end

  @doc """
  Same as `encode/2`, but returns the encoded value directly and raises on errors

  ## Examples

      iex> schema = AvroEx.decode_schema!("boolean")
      iex> AvroEx.encode!(schema, true)
      <<1>>
  """
  @spec encode!(Schema.t(), term()) :: encoded_avro()
  def encode!(schema, data) do
    case AvroEx.Encode.encode(schema, data) do
      {:ok, data} -> data
      {:error, error} -> raise error
    end
  end

  @doc """
  Given an encoded message and its accompanying schema, decodes the message.

      iex> schema = AvroEx.decode_schema!("boolean")
      iex> AvroEx.decode(schema, <<1>>)
      {:ok, true}

  """
  @spec decode(Schema.t(), encoded_avro) ::
          {:ok, term}
          | {:error, term()}
  def decode(schema, message) do
    AvroEx.Decode.decode(schema, message)
  end

  @deprecated "Use AvroEx.Schema.Context.lookup/2"
  @spec named_type(Schema.full_name(), Schema.t() | Context.t()) :: nil | Schema.schema_types()
  def named_type(name, %Schema{context: %Context{} = context}) when is_binary(name) do
    named_type(name, context)
  end

  def named_type(name, %Context{} = context) do
    Context.lookup(context, name)
  end
end
