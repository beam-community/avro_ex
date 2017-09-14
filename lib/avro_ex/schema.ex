defmodule AvroEx.Schema do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Map, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  defstruct [:context, :schema]

  @type schema_types :: Primitive.t
  | Record.t
  | Union.t
  | Map.t

  @type t :: %__MODULE__{
    context: Context.t,
    schema: term
  }
  @type json_schema :: String.t

  @spec parse(json_schema) :: t
  def parse(json_schema, %Context{} = context \\ %Context{}) do
    with {:ok, schema} <- Poison.decode(json_schema),
         {:ok, schema} <- cast(schema),
         {:ok, context} <- expand(schema, context),
         {:ok, schema} <- validate(schema, context) do
      {:ok, %__MODULE__{schema: schema, context: context}}
    end
  end

  def cast(nil), do: Primitive.cast(nil)
  def cast("null"), do: Primitive.cast("null")
  def cast("boolean"), do: Primitive.cast("boolean")
  def cast("int"), do: Primitive.cast("int")
  def cast("long"), do: Primitive.cast("long")
  def cast("float"), do: Primitive.cast("float")
  def cast("double"), do: Primitive.cast("double")
  def cast("bytes"), do: Primitive.cast("bytes")
  def cast("string"), do: Primitive.cast("string")
  def cast(%{"type" => nil} = data), do: Primitive.cast(data)
  def cast(%{"type" => "null"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "boolean"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "int"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "long"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "float"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "double"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "bytes"} = data), do: Primitive.cast(data)
  def cast(%{"type" => "string"} = data), do: Primitive.cast(data)
  def cast(str) when is_binary(str), do: {:ok, str}
  def cast(data) when is_list(data), do: Union.cast(data)
  def cast(%{"type" => "record"} = data), do: Record.cast(data)
  def cast(%{"type" => "map"} = data), do: Map.cast(data)
  def cast(%{"type" => "array"} = data), do: Array.cast(data)
  def cast(%{"type" => "fixed"} = data), do: Fixed.cast(data)
  def cast(%{"type" => "enum"} = data), do: AvroEnum.cast(data)

  def expand(schema, context) do
    {:ok, Context.add_schema(context, schema)}
  end

  def validate(schema, _context), do: {:ok, schema}

  def encodable?(%Schema{schema: schema, context: context}, data) do
    encodable?(schema, context, data)
  end

  def encodable?(%Primitive{type: nil}, _, nil), do: true
  def encodable?(%Primitive{type: :boolean}, _, bool) when is_boolean(bool), do: true
  def encodable?(%Primitive{type: :integer}, _, n) when is_integer(n), do: true
  def encodable?(%Primitive{type: :long}, _, n) when is_integer(n), do: true
  def encodable?(%Primitive{type: :float}, _, n) when is_float(n), do: true
  def encodable?(%Primitive{type: :double}, _, n) when is_float(n), do: true
  def encodable?(%Primitive{type: :bytes}, _, bytes) when is_binary(bytes), do: true
  def encodable?(%Primitive{type: :string}, _, str) when is_binary(str), do: String.valid?(str)
  def encodable?(%Record{} = record, %Context{} = context, data) when is_map(data), do: Record.match?(record, context, data)
  def encodable?(%Field{} = field, %Context{} = context, data), do: Field.match?(field, context, data)
  def encodable?(%Union{} = union, %Context{} = context, data), do: Union.match?(union, context, data)
  def encodable?(%Fixed{} = fixed, %Context{} = context, data), do: Fixed.match?(fixed, context, data)
  def encodable?(%Map{} = schema, %Context{} = context, data) when is_map(data) do
    Map.match?(schema, context, data)
  end
  def encodable?(%Array{} = schema, %Context{} = context, data) when is_list(data) do
    Array.match?(schema, context, data)
  end
  def encodable?(%AvroEnum{} = schema, %Context{} = context, data) when is_binary(data) do
    AvroEnum.match?(schema, context, data)
  end
  def encodable?(_, _, _), do: false
end
