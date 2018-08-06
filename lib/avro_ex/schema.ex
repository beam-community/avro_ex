defmodule AvroEx.Schema.Macros do
  defmacro cast_schema(data_fields: fields) do
    quote do
      def cast(data) do
        AvroEx.Schema.cast_schema(__MODULE__, data, unquote(fields))
      end
    end
  end
end

defmodule AvroEx.Schema do
  alias AvroEx.{Schema, Error}
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.Record.Field

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
  @type alias :: name

  @type t :: %__MODULE__{
          context: Context.t(),
          schema: term
        }
  @type json_schema :: String.t()

  @spec parse(json_schema) :: t
  def parse(json_schema, %Context{} = context \\ %Context{}) do
    with {:ok, schema} <- Poison.decode(json_schema),
         {:ok, schema} <- cast(schema),
         {:ok, schema} <- namespace(schema),
         {:ok, context} <- expand(schema, context) do
      {:ok, %__MODULE__{schema: schema, context: context}}
    end
  end

  def parse!(json_schema) do
    case parse(json_schema) do
      {:ok, %__MODULE__{} = schema} -> schema
      _ -> raise "Parsing schema failed"
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
  def cast(%{"type" => "map"} = data), do: AvroMap.cast(data)
  def cast(%{"type" => "array"} = data), do: Array.cast(data)
  def cast(%{"type" => "fixed"} = data), do: Fixed.cast(data)
  def cast(%{"type" => "enum"} = data), do: AvroEnum.cast(data)

  def expand(schema, context) do
    {:ok, Context.add_schema(context, schema)}
  end

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

  def encodable?(%AvroEnum{} = schema, %Context{} = context, data) when is_binary(data) do
    AvroEnum.match?(schema, context, data)
  end

  def encodable?(name, %Context{} = context, data) when is_binary(name) do
    schema = Context.lookup(context, name)
    encodable?(schema, context, data)
  end

  def encodable?(_, _, _), do: false

  def namespace(schema) do
    {:ok, namespace(schema, nil)}
  end

  def namespace(%Primitive{} = primitive, _parent_namespace), do: primitive

  def namespace(%Record{} = record, parent_namespace) do
    record = qualify_namespace(record)

    fields =
      Enum.map(record.fields, fn field ->
        namespace(field, record.namespace || parent_namespace)
      end)

    full_names = full_names(record, parent_namespace)

    %Record{record | fields: fields, qualified_names: full_names}
  end

  def namespace(%Field{} = field, parent_namespace) do
    type = namespace(field.type, parent_namespace)
    %Field{field | type: type}
  end

  def namespace(%Union{possibilities: possibilities} = union, parent_namespace) do
    possibilities = Enum.map(possibilities, &namespace(&1, parent_namespace))
    %Union{union | possibilities: possibilities}
  end

  def namespace(%Fixed{} = fixed, parent_namespace) do
    fixed = qualify_namespace(fixed)
    %Fixed{fixed | qualified_names: full_names(fixed, parent_namespace)}
  end

  def namespace(%AvroMap{} = map, parent_namespace) do
    values = namespace(map.values, parent_namespace)
    %AvroMap{map | values: values}
  end

  def namespace(%Array{} = array, parent_namespace) do
    %Array{array | items: namespace(array.items, parent_namespace)}
  end

  def namespace(%AvroEnum{} = enum, _parent_namespace) do
    enum = qualify_namespace(enum)
    %AvroEnum{enum | qualified_names: full_names(enum, enum.namespace)}
  end

  def namespace(name, nil) do
    name
  end

  def namespace(str, parent_namespace) when is_binary(str) do
    if String.match?(str, ~r/\./) do
      str
    else
      "#{parent_namespace}.#{str}"
    end
  end

  defp qualify_namespace(%{name: name} = schema) do
    if String.match?(name, ~r/\./) do
      namespace =
        name
        |> String.split(".")
        |> Enum.reverse()
        |> tail
        |> Enum.reverse()
        |> Enum.join(".")

      %{schema | namespace: namespace}
    else
      schema
    end
  end

  defp tail(list) do
    :lists.nthtail(1, list)
  end

  @spec full_names(t, namespace) :: [full_name]
  def full_names(%{aliases: aliases, namespace: namespace} = record, parent_namespace \\ nil)
      when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn name ->
        full_name(namespace || parent_namespace, name)
      end)

    [full_name(namespace || parent_namespace, record.name) | full_aliases]
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

  def cast_schema(module, data, fields) do
    metadata = Map.delete(data, "type")

    metadata =
      Enum.reduce(fields, metadata, fn field, meta ->
        Map.delete(meta, Atom.to_string(field))
      end)

    params = Map.update(data, "metadata", metadata, & &1)

    cs =
      module
      |> struct
      |> module.changeset(params)

    if cs.valid? do
      {:ok, Ecto.Changeset.apply_changes(cs)}
    else
      {:error, Error.errors(cs)}
    end
  end
end
