defmodule AvroEx.Schema do
  use TypedStruct

  alias AvroEx.{Schema}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.Record.Field
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}

  @type schema_types ::
          Array.t()
          | Enum.t()
          | Fixed.t()
          | AvroMap.t()
          | Record.t()
          | Primitive.t()
          | Union.t()
          | Reference.t()

  @type named_type ::
          AvroEnum.t()
          | Fixed.t()
          | Record.t()

  typedstruct do
    field :context, Context.t(), default: %Context{}
    field :schema, schema_types()
  end

  @type name :: String.t()
  @type namespace :: nil | String.t()
  @type full_name :: String.t()
  @type doc :: nil | String.t()
  @type metadata :: %{String.t() => String.t()}
  @type alias :: name

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
  def encodable?(%Primitive{type: :int, metadata: %{"logicalType" => "date"}}, _, %Date{}), do: true

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
  The namespace of the given Schema type

  ## Examples
      iex> namespace(%Primitive{type: :string})
      nil

      iex> namespace(%Record{name: "MyRecord"}, "namespace")
      "namespace"

      iex> namespace(%Record{name: "MyRecord", namespace: "inner"}, "namespace")
      "inner"

      iex> namespace(%Record{name: "qualified.MyRecord", namespace: "inner"}, "namespace")
      "qualified"
  """
  @spec namespace(t(), namespace()) :: namespace()
  def namespace(schema, parent_namespace \\ nil)
  def namespace(%Record.Field{}, parent_namespace), do: parent_namespace

  def namespace(%{name: name, namespace: namespace}, parent_namespace) do
    split_name = split_name(name)

    cond do
      # if it has at least two values, its a fullname
      # e.g. "namespace.Name" would be `["namespace", "Name"]`
      match?([_, _ | _], split_name) ->
        split_name |> :lists.droplast() |> Enum.join(".")

      is_nil(namespace) ->
        parent_namespace

      true ->
        namespace
    end
  end

  def namespace(_schema, parent_namespace), do: parent_namespace

  @doc """
  The fully-qualified name of the type


  ## Examples
      iex> full_name(%Primitive{type: "string"})
      nil

      iex> full_name(%Record{name: "foo", namespace: "beam.community"})
      "beam.community.foo"

      iex> full_name(%Record{name: "foo"}, "top.level.namespace")
      "top.level.namespace.foo"
  """
  @spec full_name(schema_types() | name(), namespace()) :: nil | String.t()
  def full_name(schema, parent_namespace \\ nil)

  def full_name(%{name: name, namespace: namespace}, parent_namespace) do
    if is_nil(namespace) do
      full_name(name, parent_namespace)
    else
      full_name(name, namespace)
    end
  end

  def full_name(%Record.Field{name: name}, _parent_namespace) do
    name
  end

  def full_name(name, namespace) when is_binary(name) do
    cond do
      is_nil(namespace) ->
        name

      String.contains?(name, ".") ->
        name

      true ->
        "#{namespace}.#{name}"
    end
  end

  def full_name(_name, _namespace), do: nil

  @doc """
  The name of the schema type

  ## Examples

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

  # split a full name into its parts
  defp split_name(string) do
    pattern = :binary.compile_pattern(".")
    String.split(string, pattern)
  end
end
