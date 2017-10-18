defmodule AvroEx do
  alias AvroEx.Schema

  @type avro :: binary

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
  def encode(schema, data) do
    AvroEx.Encode.encode(schema, data)
  end

  @doc """
  Given an encoded message and its accompanying schema, decodes the message.
  """
  def decode(schema, message) do
    AvroEx.Decode.decode(schema, message)
  end
end
