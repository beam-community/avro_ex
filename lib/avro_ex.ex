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
  Checks to see if the given data is encodable using the given schema. Great in
  unit tests.
  """
  defdelegate encodable?(schema, data), to: AvroEx.Schema

  @deprecated "Use AvroEx.decode_schema/1 instead"
  def parse_schema(json), do: decode_schema(json)

  @deprecated "Use AvroEx.decode_schema!/1 instead"
  def parse_schema!(json), do: decode_schema!(json)

  @doc """
  Given a JSON-formatted schema, parses the schema and returns a `%AvroEx.Schema{}` struct representing the schema.
  Errors if the JSON is invalid, or if a named record is referenced that doesn't exist.
  """
  @spec decode_schema(Schema.json_schema()) ::
          {:ok, Schema.t()}
          | {:error, :unnamed_record}
          | {:error, :invalid_json}

  def decode_schema(json) do
    Schema.parse(json)
  end

  @doc """
  Same as `AvroEx.decode_schema/1`, but raises an exception on failure instead of
  returning an error tuple.
  """
  @spec decode_schema!(Schema.json_schema()) :: Schema.t() | no_return
  def decode_schema!(json) do
    case decode_schema(json) do
      {:ok, schema} -> schema
      _ -> raise "Parsing schema failed"
    end
  end

  @doc """
  Given a %Schema{} and some data, takes the data and encodes it according to the schema.
  Checks that the data is encodable before beginning encoding.
  """
  @spec encode(Schema.t(), term) ::
          {:ok, encoded_avro} | {:error, AvroEx.EncodeError.t() | Exception.t()}
  def encode(schema, data) do
    AvroEx.Encode.encode(schema, data)
  end

  @doc """
  Same as `encode/2`, but returns the encoded value directly and raises on errors
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
  """
  @spec decode(Schema.t(), encoded_avro) ::
          {:ok, term}
          | {:error, term()}
  def decode(schema, message) do
    AvroEx.Decode.decode(schema, message)
  end

  @spec named_type(Schema.full_name(), Schema.t() | Context.t()) :: nil | Schema.schema_types()
  def named_type(name, %Schema{context: %Context{} = context}) when is_binary(name) do
    named_type(name, context)
  end

  def named_type(name, %Context{} = context) do
    Context.lookup(context, name)
  end
end
