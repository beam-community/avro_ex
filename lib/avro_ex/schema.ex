defmodule AvroEx.Schema do
  alias AvroEx.{Schema}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.Record.Field
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}

  defstruct [:context, :schema]

  @type schema_types ::
          Array.t()
          | Enum.t()
          | Fixed.t()
          | AvroMap.t()
          | Record.t()
          | Primitive.t()
          | Union.t()

  @type named_type ::
          AvroEnum.t()
          | Fixed.t()
          | Record.t()

  @type name :: String.t()
  @type namespace :: nil | String.t()
  @type full_name :: String.t()
  @type doc :: nil | String.t()
  @type metadata :: %{String.t() => String.t()}
  @type alias :: name

  @type t :: %__MODULE__{
          context: Context.t(),
          schema: term
        }
  @type json_schema :: String.t()

  @spec encodable?(AvroEx.Schema.t(), any()) :: boolean()
  def encodable?(%Schema{schema: schema, context: context}, data) do
    encodable?(schema, context, data)
  end

  @spec encodable?(any(), any(), any()) :: boolean()
  def encodable?(%Primitive{type: :null}, _, nil), do: true
  def encodable?(%Primitive{type: :boolean}, _, bool) when is_boolean(bool), do: true
  def encodable?(%Primitive{type: :int}, _, n) when is_integer(n), do: true
  def encodable?(%Primitive{type: :long}, _, n) when is_integer(n), do: true
  def encodable?(%Primitive{type: :float}, _, n) when is_float(n), do: true
  def encodable?(%Primitive{type: :double}, _, n) when is_float(n), do: true
  def encodable?(%Primitive{type: :bytes}, _, bytes) when is_binary(bytes), do: true
  def encodable?(%Primitive{type: :string}, _, str) when is_binary(str), do: String.valid?(str)

  def encodable?(%Primitive{type: :string}, _, atom) when is_atom(atom) do
    if is_nil(atom) or is_boolean(atom) do
      false
    else
      atom |> to_string() |> String.valid?()
    end
  end

  def encodable?(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}}, _, %DateTime{}), do: true
  def encodable?(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}}, _, %DateTime{}), do: true
  def encodable?(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}}, _, %DateTime{}), do: true
  def encodable?(%Primitive{type: :long, metadata: %{"logicalType" => "time-micros"}}, _, %Time{}), do: true
  def encodable?(%Primitive{type: :int, metadata: %{"logicalType" => "time-millis"}}, _, %Time{}), do: true

  def encodable?(%Record{} = record, %Context{} = context, data) when is_map(data),
    do: Record.match?(record, context, data)

  def encodable?(%Field{} = field, %Context{} = context, data),
    do: Field.match?(field, context, data)

  def encodable?(%Union{} = union, %Context{} = context, data),
    do: Union.match?(union, context, data)

  def encodable?(%Fixed{} = fixed, %Context{} = context, data),
    do: Fixed.match?(fixed, context, data)

  def encodable?(%AvroMap{} = schema, %Context{} = context, data) when is_map(data) do
    AvroMap.match?(schema, context, data)
  end

  def encodable?(%Array{} = schema, %Context{} = context, data) when is_list(data) do
    Array.match?(schema, context, data)
  end

  def encodable?(%AvroEnum{} = schema, %Context{} = context, data) when is_atom(data) do
    AvroEnum.match?(schema, context, to_string(data))
  end

  def encodable?(%AvroEnum{} = schema, %Context{} = context, data) when is_binary(data) do
    AvroEnum.match?(schema, context, data)
  end

  def encodable?(%Reference{type: name}, %Context{} = context, data) do
    schema = Context.lookup(context, name)
    encodable?(schema, context, data)
  end

  def encodable?(_, _, _), do: false

  @doc """
  The fully-qualified name of the type
  iex> full_name(%Primitive{type: "string"})
  nil
  iex> full_name(%Record{name: "foo", namespace: "beam.community"})
  "beam.community.foo"
  """
  @spec full_name(schema_types()) :: nil | String.t()
  def full_name(%struct{}) when struct in [Array, AvroMap, Primitive, Union], do: nil

  def full_name(%Record.Field{name: name}) do
    name
  end

  def full_name(%struct{name: name, namespace: namespace}) when struct in [AvroEnum, Fixed, Record] do
    full_name(namespace, name)
  end

  @spec full_name(namespace, name) :: full_name
  def full_name(nil, name) when is_binary(name) do
    name
  end

  def full_name(namespace, name) when is_binary(namespace) and is_binary(name) do
    if String.match?(name, ~r/\./) do
      name
    else
      "#{namespace}.#{name}"
    end
  end

  @doc """
  The name of the schema type

  iex> type_name(%Primitive{type: "string"})
  "string"

  iex> type_name(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}})
  "timestamp-millis"

  iex> type_name(%AvroEnum{name: "switch", symbols: []})
  "Enum<name=switch>"

  iex> type_name(%Array{items: %Primitive{type: "integer"}})
  "Array<items=integer>"

  iex> type_name(%Fixed{size: 2, name: "double"})
  "Fixed<name=double, size=2>"

  iex> type_name(%Union{possibilities: [%Primitive{type: "string"}, %Primitive{type: "int"}]})
  "Union<possibilities=string|int>"

  iex> type_name(%Record{name: "foo"})
  "Record<name=foo>"
  """
  @spec type_name(schema_types()) :: String.t()
  def type_name(%Primitive{type: :null}), do: "null"
  def type_name(%Primitive{metadata: %{"logicalType" => type}}), do: type
  def type_name(%Primitive{type: type}), do: to_string(type)

  def type_name(%Array{items: type}), do: "Array<items=#{type_name(type)}>"
  def type_name(%Union{possibilities: types}), do: "Union<possibilities=#{Enum.map_join(types, "|", &type_name/1)}>"
  def type_name(%Record{} = record), do: "Record<name=#{full_name(record)}>"
  def type_name(%Record.Field{} = field), do: "Field<name=#{full_name(field)}>"
  def type_name(%Fixed{size: size} = fixed), do: "Fixed<name=#{full_name(fixed)}, size=#{size}>"
  def type_name(%AvroEnum{} = enum), do: "Enum<name=#{full_name(enum)}>"
  def type_name(%AvroMap{values: values}), do: "Map<values=#{type_name(values)}>"
end
