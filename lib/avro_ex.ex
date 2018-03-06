defmodule AvroEx do
  @moduledoc """
  The main interface for the library. Supports parsing schemas, encoding data,
  and decoding data.

  For encoding and decoding, the following type chart should be referenced:

  | Avro Types | Elixir Types |
  |------------|:------------:|
  | boolean | boolean |
  | integer | integer |
  | long | integer |
  | float | decimal |
  | double | decimal |
  | bytes | binary |
  | string | String.t |
  | null | nil |
  | Record | map |
  | Enum | String (corresponding to the enum's symbol list) |
  """
  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  @type encoded_avro :: binary

  @doc """
  Checks to see if the given data is encodable using the given schema. Great in
  unit tests.
  """
  defdelegate encodable?(schema, data), to: AvroEx.Schema

  @spec parse_schema(Avro.Schema.json_schema)
  :: {:ok, Schema.t}
  | {:error, :unnamed_record}
  | {:error, :invalid_json}
  @doc """
  Given a JSON-formatted schema, parses the schema and returns a %Schema{} struct representing the schema.
  Errors if the JSON is invalid, or if a named record is referenced that doesn't exist.
  """
  def parse_schema(json_schema) do
    Schema.parse(json_schema)
  end

  @spec parse_schema!(Avro.Schema.json_schema) :: Schema.t | no_return
  @doc """
  Same as `AvroEx.parse_schema/1`, but raises an exception on failure instead of
  returning an error tuple.
  """
  def parse_schema!(json_schema) do
    case parse_schema(json_schema) do
      {:ok, schema} -> schema
      _ -> raise "Parsing schema failed"
    end
  end

  @doc """
  Given a %Schema{} and some data, takes the data and encodes it according to the schema.
  Checks that the data is encodable before beginning encoding.
  """
  @spec encode(Avro.Schema.t, term)
  :: {:ok, encoded_avro}
  | {:error, :unmatching_schema}
  | {:error, AvroEx.Encode.reason, term}
  def encode(schema, data) do
    AvroEx.Encode.encode(schema, data)
  end

  @doc """
  Given an encoded message and its accompanying schema, decodes the message.
  """
  @spec decode(AvroEx.Schema.t, encoded_avro)
  :: {:ok, term}
  | {:error, AvroEx.Decode.reason}
  def decode(schema, message) do
    AvroEx.Decode.decode(schema, message)
  end

  @spec named_type(Schema.full_name, Schema.t | Context.t) :: nil | Schema.schema_types
  def named_type(name, %Schema{context: %Context{} = context}) when is_binary(name) do
    named_type(name, context)
  end

  def named_type(name, %Context{} = context) do
    Context.lookup(context, name)
  end
end
