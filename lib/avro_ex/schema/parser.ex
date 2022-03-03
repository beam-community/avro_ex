defmodule AvroEx.Schema.Parser do
  @doc false
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap

  # TODO convert to atom
  @primitives [
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "string"
  ]

  def primitives, do: @primitives

  def primitive?(p) when p in @primitives, do: true

  def primitive?(_), do: false

  @spec parse!(term()) :: AvroEx.Schema.t()
  def parse!(data) do
    try do
      type = do_parse(data)
      context = build_context(type, %Context{})

      %Schema{schema: type, context: context}
    catch
      :throw, %AvroEx.Schema.DecodeError{} = err -> raise err
    end
  end

  # do_parse_ref/1 handles types that might be a %Reference{}
  defp do_parse_ref(term) do
    if is_binary(term) and not primitive?(term) do
      Reference.new(term)
    else
      do_parse(term)
    end
  end

  defp do_parse(nil), do: %Primitive{type: :null}

  for p <- @primitives do
    defp do_parse(unquote(p)) do
      %Primitive{type: unquote(String.to_atom(p))}
    end
  end

  defp do_parse(list) when is_list(list) do
    {possibilities, _} =
      Enum.map_reduce(list, MapSet.new(), fn type, seen ->
        %struct{} = parsed = do_parse_ref(type)

        if match?(%Union{}, parsed) do
          error({:nested_union, parsed, list})
        end

        set_key =
          if struct in [AvroEnum, Fixed, Record] do
            {struct, parsed.name}
          else
            parsed.type
          end

        if MapSet.member?(seen, set_key) do
          error({:duplicate_union_type, parsed, list})
        end

        {parsed, MapSet.put(seen, set_key)}
      end)

    struct!(Union, possibilities: possibilities)
  end

  defp do_parse(%{"type" => primitive} = type) when primitive in @primitives do
    data =
      type
      |> cast(Primitive, [])
      |> drop([:type])
      |> extract_metadata()

    struct!(Primitive, Map.put(data, :type, String.to_existing_atom(primitive)))
  end

  defp do_parse(%{"type" => "map"} = map) do
    data =
      map
      |> cast(AvroMap, [:values, :default])
      |> validate_required([:values])
      |> drop([:type])
      |> extract_data()
      |> update_in([:values], &do_parse_ref/1)

    struct!(AvroMap, data)
  end

  defp do_parse(%{"type" => "enum", "symbols" => symbols} = enum) when is_list(symbols) do
    data =
      enum
      |> cast(AvroEnum, [:aliases, :doc, :name, :namespace, :symbols])
      |> drop([:type])
      |> validate_required([:name, :symbols])
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_data()

    Enum.reduce(symbols, MapSet.new(), fn symbol, set ->
      if MapSet.member?(set, symbol) do
        error({:duplicate_symbol, symbol, enum})
      end

      unless valid_name?(symbol) do
        error({:invalid_name, {:symbols, symbol}, enum})
      end

      MapSet.put(set, symbol)
    end)

    struct!(AvroEnum, data)
  end

  defp do_parse(%{"type" => "array"} = array) do
    data =
      array
      |> cast(Array, [:items, :default])
      |> drop([:type])
      |> validate_required([:items])
      |> extract_data()
      |> update_in([:items], &do_parse_ref/1)

    struct!(Array, data)
  end

  defp do_parse(%{"type" => "fixed"} = fixed) do
    data =
      fixed
      |> cast(Fixed, [:aliases, :doc, :name, :namespace, :size])
      |> drop([:type])
      |> validate_required([:name, :size])
      |> validate_integer(:size)
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_data()

    struct!(Fixed, data)
  end

  defp do_parse(%{"type" => "record", "fields" => fields} = record) when is_list(fields) do
    data =
      record
      |> cast(Record, [:aliases, :doc, :name, :namespace, :fields])
      |> drop([:type])
      |> validate_required([:name, :fields])
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_data()
      |> update_in([:fields], fn fields -> Enum.map(fields, &parse_fields/1) end)

    struct!(Record, data)
  end

  defp do_parse(other) do
    error({:invalid_format, other})
  end

  defp parse_fields(%{"type" => type} = field) do
    data =
      field
      |> cast(Record.Field, [:aliases, :doc, :default, :name, :namespace, :order, :type])
      |> validate_required([:name, :type])
      |> validate_aliases()
      |> extract_data()
      |> put_in([:type], do_parse_ref(type))

    struct!(Record.Field, data)
  end

  defp cast(data, type, keys) do
    info = {type, data}

    Enum.reduce(keys, {%{}, data, info}, fn key, {data, rest, info} ->
      case Map.pop(rest, to_string(key)) do
        {nil, rest} ->
          {data, rest, info}

        {value, rest} ->
          {Map.put(data, key, value), rest, info}
      end
    end)
  end

  defp validate_required({data, rest, {type, raw} = info}, keys) do
    Enum.each(keys, fn k ->
      unless data[k] do
        error({:missing_required, k, type, raw})
      end
    end)

    {data, rest, info}
  end

  defp validate_field({data, _rest, _info} = input, field, func) do
    case Map.fetch(data, field) do
      {:ok, _value} ->
        # Only validate if it has the field
        func.(data[field])

      :error ->
        :ok
    end

    input
  end

  defp validate_integer({_data, _rest, {_type, raw}} = input, field) do
    validate_field(input, field, fn value ->
      unless is_integer(value) do
        error({:invalid_type, {field, value}, %Primitive{type: :integer}, raw})
      end
    end)
  end

  defp validate_name({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :name, fn value ->
      unless valid_name?(value) do
        error({:invalid_name, {:name, value}, raw})
      end
    end)
  end

  defp validate_aliases({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :aliases, fn aliases ->
      unless is_list(aliases) and Enum.all?(aliases, &valid_name?/1) do
        error({:invalid_name, {:aliases, aliases}, raw})
      end
    end)
  end

  defp validate_namespace({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :namespace, fn value ->
      unless valid_namespace?(value) do
        error({:invalid_name, {:namespace, value}, raw})
      end
    end)
  end

  defp validate_default(%{default: default} = schema) when not is_nil(default) do
    case AvroEx.encode(%Schema{schema: schema, context: %Context{}}, schema.default) do
      {:ok, _data} -> :ok
      {:error, reason} -> error({:invalid_default, schema, reason})
    end

    schema
  end

  defp validate_default(schema), do: schema

  defp extract_metadata({data, rest, _info}) do
    Map.put(data, :metadata, rest)
  end

  defp extract_data({data, rest, {type, raw}}) do
    if rest != %{} do
      # TODO this violates the spec
      # where typeName is either a primitive or derived type name, as defined below. Attributes not defined in this document are permitted as metadata, but must not affect the format of serialized data.
      error({:unrecognized_fields, Map.keys(rest), type, raw})
    end

    data
  end

  defp drop({data, rest, info}, keys) do
    {data, Map.drop(rest, Enum.map(keys, &to_string/1)), info}
  end

  defp build_context(type, context) do
    context = capture_context(type, context)

    type
    |> validate_default()
    |> do_build_context(context)
  end

  defp do_build_context(%Union{} = union, context) do
    build_inner_context(union, :possibilities, context)
  end

  defp do_build_context(%Record{} = record, context) do
    build_inner_context(record, :fields, context)
  end

  defp do_build_context(%Record.Field{} = field, context) do
    build_inner_context(field, :type, context)
  end

  defp do_build_context(%Array{} = array, context) do
    build_inner_context(array, :items, context)
  end

  defp do_build_context(%AvroMap{} = map, context) do
    build_inner_context(map, :values, context)
  end

  defp do_build_context(%Reference{} = ref, context) do
    unless Map.has_key?(context.names, ref.type) do
      error({:missing_ref, ref, context})
    end

    context
  end

  defp do_build_context(_schema, context), do: context

  defp build_inner_context(type, field, context) do
    %{^field => inner} = type

    if is_list(inner) do
      Enum.reduce(inner, context, &build_context/2)
    else
      build_context(inner, context)
    end
  end

  defp capture_context(%Record.Field{}, context), do: context

  defp capture_context(%{name: _name} = schema, context) do
    name = AvroEx.Schema.full_name(schema)

    if Map.has_key?(context.names, name) do
      error({:duplicate_name, name, schema})
    end

    if match?(%Record{}, schema) do
      Enum.reduce(schema.fields, MapSet.new(), fn field, set ->
        if MapSet.member?(set, field.name) do
          error({:duplicate_name, field.name, schema})
        end

        MapSet.put(set, field.name)
      end)
    end

    # TODO needs to propagate
    parent_namespace = nil

    context =
      schema
      |> aliases(parent_namespace)
      |> Enum.reduce(context, fn name, context ->
        put_context(context, name, schema)
      end)

    put_context(context, name, schema)
  end

  defp capture_context(_type, context), do: context

  defp put_context(context, name, schema) do
    put_in(context.names[name], schema)
  end

  defp aliases(%{aliases: aliases, namespace: namespace} = record, parent_namespace)
       when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn name ->
        AvroEx.Schema.full_name(namespace || parent_namespace, name)
      end)

    [AvroEx.Schema.full_name(namespace || parent_namespace, record.name) | full_aliases]
  end

  defp aliases(_schema, _parent_namespace), do: []

  defp error(info) do
    info |> AvroEx.Schema.DecodeError.new() |> throw()
  end

  defp valid_name?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, name)
  end

  defp valid_name?(_), do: false

  defp valid_namespace?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z_](\.?[A-Za-z0-9_]+)*$/, name)
  end

  defp valid_namespace?(_), do: false
end
