defmodule AvroEx.Schema do
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.Record.Field
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}

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

  @spec parse(json_schema, Context.t()) :: {:ok, t} | {:error, term}
  def parse(json_schema, %Context{} = context \\ %Context{}) do
    with {:ok, schema} <- Jason.decode(json_schema),
         {:ok, schema} <- cast(schema),
         {:ok, schema} <- propagate_namespace(schema),
         {:ok, context} <- expand(schema, context) do
      {:ok, %__MODULE__{schema: schema, context: context}}
    end
  end

  @spec parse!(binary()) :: AvroEx.Schema.t()
  def parse!(json_schema) do
    case parse(json_schema) do
      {:ok, %__MODULE__{} = schema} -> schema
      _ -> raise "Parsing schema failed"
    end
  end

  @spec cast(nil | binary() | maybe_improper_list() | map()) :: :error | {:error, any()} | {:ok, binary() | map()}
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

  @spec expand(
          binary()
          | %{
              __struct__:
                AvroEx.Schema.Array
                | AvroEx.Schema.Enum
                | AvroEx.Schema.Fixed
                | AvroEx.Schema.Map
                | AvroEx.Schema.Primitive
                | AvroEx.Schema.Record
                | AvroEx.Schema.Union
            },
          AvroEx.Schema.Context.t()
        ) :: {:ok, any()}
  def expand(schema, context) do
    {:ok, Context.add_schema(context, schema)}
  end

  defp propagate_namespace(schema) do
    {:ok, propagate_namespace(schema, nil)}
  end

  defp propagate_namespace(%Primitive{} = primitive, _parent_namespace), do: primitive

  defp propagate_namespace(%Record{} = record, parent_namespace) do
    record = qualify_namespace(record)

    fields =
      Enum.map(record.fields, fn field ->
        propagate_namespace(field, record.namespace || parent_namespace)
      end)

    full_names = full_names(record, parent_namespace)

    %Record{record | fields: fields, qualified_names: full_names}
  end

  defp propagate_namespace(%Field{} = field, parent_namespace) do
    type = propagate_namespace(field.type, parent_namespace)
    %Field{field | type: type}
  end

  defp propagate_namespace(%Union{possibilities: possibilities} = union, parent_namespace) do
    possibilities = Enum.map(possibilities, &propagate_namespace(&1, parent_namespace))
    %Union{union | possibilities: possibilities}
  end

  defp propagate_namespace(%Fixed{} = fixed, parent_namespace) do
    fixed = qualify_namespace(fixed)
    %Fixed{fixed | qualified_names: full_names(fixed, parent_namespace)}
  end

  defp propagate_namespace(%AvroMap{} = map, parent_namespace) do
    values = propagate_namespace(map.values, parent_namespace)
    %AvroMap{map | values: values}
  end

  defp propagate_namespace(%Array{} = array, parent_namespace) do
    %Array{array | items: propagate_namespace(array.items, parent_namespace)}
  end

  defp propagate_namespace(%AvroEnum{} = enum, _) do
    enum = qualify_namespace(enum)
    %AvroEnum{enum | qualified_names: full_names(enum, enum.namespace)}
  end

  defp propagate_namespace(name, nil) do
    name
  end

  defp propagate_namespace(str, parent_namespace) when is_binary(str) do
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

  @spec full_names(Enum.t() | Fixed.t() | Record.t(), namespace) :: [full_name]
  def full_names(%{aliases: aliases, namespace: namespace} = record, parent_namespace \\ nil)
      when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn name ->
        full_name(namespace || parent_namespace, name)
      end)

    [full_name(namespace || parent_namespace, record.name) | full_aliases]
  end

  @doc """
  The fully-qualified name of the type

  iex> full_name(%Primitive{type: "string"})
  nil

  iex> full_name(%Record{name: "foo", namespace: "beam.community"})
  "beam.community.foo"
  """
  @spec full_name(schema_types()) :: nil | String.t()
  def full_name(%struct{}) when struct in [Array, AvroMap, Primitive, Union], do: nil

  def full_name(%struct{name: name, namespace: namespace}) when struct in [Fixed, Record, AvroEnum] do
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

  iex> type_name(%AvroEnum{name: "switch"})
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
  def type_name(%Primitive{type: nil}), do: "null"
  def type_name(%Primitive{metadata: %{"logicalType" => type}}), do: type
  def type_name(%Primitive{type: type}), do: to_string(type)

  def type_name(%Array{items: type}), do: "Array<items=#{type_name(type)}>"
  def type_name(%Union{possibilities: types}), do: "Union<possibilities=#{Enum.map_join(types, "|", &type_name/1)}>"
  def type_name(%Record{} = record), do: "Record<name=#{full_name(record)}>"
  def type_name(%Fixed{size: size} = fixed), do: "Fixed<name=#{full_name(fixed)}, size=#{size}>"
  def type_name(%AvroEnum{} = enum), do: "Enum<name=#{full_name(enum)}>"
  def type_name(%AvroMap{values: values}), do: "Map<values=#{type_name(values)}>"

  @spec cast_schema(atom(), map(), any()) :: {:error, any()} | {:ok, map()}
  def cast_schema(module, data, fields) do
    metadata = Map.delete(data, "type")

    reduced_metadata =
      Enum.reduce(fields, metadata, fn field, meta ->
        Map.delete(meta, Atom.to_string(field))
      end)

    params = Map.update(data, "metadata", reduced_metadata, & &1)

    cs =
      module
      |> struct
      |> module.changeset(params)

    if cs.valid? do
      {:ok, Ecto.Changeset.apply_changes(cs)}
    else
      {:error, errors(cs)}
    end
  end

  @doc false
  @spec errors(Ecto.Changeset.t(), atom()) :: [any()]
  def errors(%Ecto.Changeset{} = cs, field) do
    cs.errors
    |> Keyword.get_values(field)
    |> Enum.map(fn
      {err, _} when is_binary(err) -> err
      any -> any
    end)
  end

  @doc false
  @spec errors(Ecto.Changeset.t()) :: any()
  def errors(%Ecto.Changeset{} = cs) do
    Enum.reduce(cs.errors, %{}, fn {field, {value, _}}, acc ->
      Map.update(acc, field, [value], fn tail -> [value | tail] end)
    end)
  end
end
