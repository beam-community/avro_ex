defmodule AvroEx.Schema.Parser do
  @doc false
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Reference, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap

  @primitives [
    :null,
    :boolean,
    :int,
    :long,
    :float,
    :double,
    :bytes,
    :string
  ]

  @str_primitives Enum.map(@primitives, &to_string/1)

  @spec primitives :: list(atom())
  def primitives, do: @primitives

  @spec primitive?(String.t() | atom()) :: boolean()
  for p <- @primitives do
    def primitive?(unquote(p)), do: true
    def primitive?(unquote(to_string(p))), do: true
  end

  def primitive?(_), do: false

  @spec parse!(term(), Keyword.t()) :: AvroEx.Schema.t()
  def parse!(data, opts \\ []) do
    config = %{namespace: nil, strict?: Keyword.get(opts, :strict, false)}

    try do
      type = do_parse(data, config)
      context = build_context(type, %Context{})

      %Schema{schema: type, context: context}
    catch
      :throw, %AvroEx.Schema.DecodeError{} = err -> raise err
    end
  end

  # do_parse_ref/1 handles types that might be a %Reference{}
  defp do_parse_ref(term, config) do
    if is_binary(term) and not primitive?(term) do
      term
      |> full_name(config.namespace)
      |> Reference.new()
    else
      do_parse(term, config)
    end
  end

  defp do_parse(nil, _config), do: %Primitive{type: :null}

  for p <- @primitives do
    defp do_parse(unquote(to_string(p)), _config) do
      %Primitive{type: unquote(p)}
    end
  end

  defp do_parse(list, config) when is_list(list) do
    {possibilities, _} =
      Enum.map_reduce(list, MapSet.new(), fn type, seen ->
        %struct{} = parsed = do_parse_ref(type, config)

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

  defp do_parse(%{"type" => primitive} = type, config) when primitive in @str_primitives do
    data =
      type
      |> cast(Primitive, [])
      |> drop([:type])
      |> extract_metadata(config)

    struct!(Primitive, Map.put(data, :type, String.to_existing_atom(primitive)))
  end

  defp do_parse(%{"type" => "map"} = map, config) do
    data =
      map
      |> cast(AvroMap, [:values, :default])
      |> validate_required([:values])
      |> drop([:type])
      |> extract_metadata(config)
      |> update_in([:values], &do_parse_ref(&1, config))

    struct!(AvroMap, data)
  end

  defp do_parse(%{"type" => "enum", "symbols" => symbols} = enum, config) when is_list(symbols) do
    data =
      enum
      |> cast(AvroEnum, [:aliases, :doc, :name, :namespace, :symbols])
      |> drop([:type])
      |> validate_required([:name, :symbols])
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_metadata(config)

    # credo:disable-for-lines:11 Credo.Check.Warning.UnusedEnumOperation
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

  defp do_parse(%{"type" => "array"} = array, config) do
    data =
      array
      |> cast(Array, [:items, :default])
      |> drop([:type])
      |> validate_required([:items])
      |> extract_metadata(config)
      |> update_in([:items], &do_parse_ref(&1, config))

    struct!(Array, data)
  end

  defp do_parse(%{"type" => "fixed"} = fixed, config) do
    data =
      fixed
      |> cast(Fixed, [:aliases, :doc, :name, :namespace, :size])
      |> drop([:type])
      |> validate_required([:name, :size])
      |> validate_integer(:size)
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_data(config)

    struct!(Fixed, data)
  end

  defp do_parse(%{"type" => "record", "fields" => fields} = record, config) when is_list(fields) do
    data =
      record
      |> cast(Record, [:aliases, :doc, :name, :namespace, :fields])
      |> drop([:type])
      |> validate_required([:name, :fields])
      |> validate_name()
      |> validate_namespace()
      |> validate_aliases()
      |> extract_metadata(config)

    config = Map.update!(config, :namespace, &namespace(data, &1))

    struct!(
      Record,
      update_in(data[:fields], fn fields -> Enum.map(fields, &parse_fields(&1, config)) end)
    )
  end

  defp do_parse(other, _config) do
    error({:invalid_format, other})
  end

  defp parse_fields(%{"type" => type} = field, config) do
    data =
      field
      |> cast(Record.Field, [:aliases, :doc, :default, :name, :namespace, :order, :type])
      |> validate_required([:name, :type])
      |> validate_aliases()
      |> extract_data(config)
      |> put_in([:type], do_parse_ref(type, config))

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
      unless valid_full_name?(value) do
        error({:invalid_name, {:name, value}, raw})
      end
    end)
  end

  defp validate_aliases({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :aliases, fn aliases ->
      unless is_list(aliases) and Enum.all?(aliases, &valid_full_name?/1) do
        error({:invalid_name, {:aliases, aliases}, raw})
      end
    end)
  end

  defp validate_namespace({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :namespace, fn value ->
      unless valid_full_name?(value) do
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

  defp extract_metadata({data, rest, _info}, _config) do
    Map.put(data, :metadata, rest)
  end

  defp extract_data({data, rest, {type, raw}}, config) do
    if config.strict? and rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), type, raw})
    end

    data
  end

  defp drop({data, rest, info}, keys) do
    {data, Map.drop(rest, Enum.map(keys, &to_string/1)), info}
  end

  defp build_context(type, context, namespace \\ nil)

  defp build_context(type, context, namespace) do
    namespace = namespace(type, namespace)
    context = capture_context(type, context, namespace)

    type
    |> validate_default()
    |> do_build_context(context, namespace)
  end

  defp do_build_context(%Union{} = union, context, namespace) do
    build_inner_context(union, :possibilities, context, namespace)
  end

  defp do_build_context(%Record{} = record, context, namespace) do
    build_inner_context(record, :fields, context, namespace)
  end

  defp do_build_context(%Record.Field{} = field, context, namespace) do
    build_inner_context(field, :type, context, namespace)
  end

  defp do_build_context(%Array{} = array, context, namespace) do
    build_inner_context(array, :items, context, namespace)
  end

  defp do_build_context(%AvroMap{} = map, context, namespace) do
    build_inner_context(map, :values, context, namespace)
  end

  defp do_build_context(%Reference{} = ref, context, _namespace) do
    unless Map.has_key?(context.names, ref.type) do
      error({:missing_ref, ref, context})
    end

    context
  end

  defp do_build_context(_schema, context, _namespace), do: context

  defp build_inner_context(type, field, context, namespace) do
    %{^field => inner} = type

    if is_list(inner) do
      Enum.reduce(inner, context, &build_context(&1, &2, namespace))
    else
      build_context(inner, context, namespace)
    end
  end

  defp capture_context(%Record.Field{}, context, _namespace), do: context

  defp capture_context(%{name: _name} = schema, context, namespace) do
    name = full_name(schema, namespace)

    if Map.has_key?(context.names, name) do
      error({:duplicate_name, name, schema})
    end

    if match?(%Record{}, schema) do
      # credo:disable-for-lines:8 Credo.Check.Warning.UnusedEnumOperation
      Enum.reduce(schema.fields, MapSet.new(), fn field, set ->
        if MapSet.member?(set, field.name) do
          error({:duplicate_name, field.name, schema})
        end

        MapSet.put(set, field.name)
      end)
    end

    context =
      schema
      |> aliases(namespace)
      |> Enum.reduce(context, fn name, context ->
        put_context(context, name, schema)
      end)

    put_context(context, name, schema)
  end

  defp capture_context(_type, context, _namespace), do: context

  defp put_context(context, name, schema) do
    put_in(context.names[name], schema)
  end

  defp aliases(%{aliases: aliases, namespace: namespace}, parent_namespace)
       when is_list(aliases) do
    Enum.map(aliases, fn name ->
      full_name(name, namespace || parent_namespace)
    end)
  end

  defp aliases(_schema, _parent_namespace), do: []

  defp error(info) do
    info |> AvroEx.Schema.DecodeError.new() |> throw()
  end

  defp valid_name?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, name)
  end

  defp valid_name?(_), do: false

  defp valid_full_name?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z_](\.?[A-Za-z0-9_]+)*$/, name)
  end

  defp valid_full_name?(_), do: false

  # split a full name into its parts
  defp split_name(string) do
    pattern = :binary.compile_pattern(".")
    String.split(string, pattern)
  end

  defp namespace(schema, parent_namespace)
  defp namespace(%Record.Field{}, parent_namespace), do: parent_namespace

  defp namespace(%{name: name, namespace: namespace}, parent_namespace) do
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

  defp namespace(_schema, parent_namespace), do: parent_namespace

  defp full_name(%{name: name, namespace: namespace}, parent_namespace) do
    if is_nil(namespace) do
      full_name(name, parent_namespace)
    else
      full_name(name, namespace)
    end
  end

  defp full_name(%Record.Field{name: name}, _parent_namespace) do
    name
  end

  defp full_name(name, namespace) when is_binary(name) do
    cond do
      is_nil(namespace) ->
        name

      String.contains?(name, ".") ->
        name

      true ->
        "#{namespace}.#{name}"
    end
  end

  defp full_name(_name, _namespace), do: nil
end
