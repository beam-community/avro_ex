defmodule AvroEx.Schema.Parser do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap

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

  # Parses a schema from an elixir (string) map representation
  #
  # Will throw DecodeError on structural and type issues,
  # but will not do any validation of semantic information,
  # i.e. is this logicalType valid?
  @spec parse!(term()) :: AvroEx.Schema.t()
  def parse!(data) do
    try do
      type = do_parse(data)

      %Schema{schema: type, context: %Context{}}
    catch
      :throw, %AvroEx.Schema.DecodeError{} = err -> raise err
    end
  end

  for p <- @primitives do
    defp do_parse(unquote(p)) do
      %Primitive{type: unquote(String.to_atom(p))}
    end
  end

  defp do_parse(list) when is_list(list) do
    {possibilities, _} =
      Enum.map_reduce(list, MapSet.new(), fn type, seen ->
        %struct{} = parsed = do_parse(type)

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
      |> update_in([:values], &do_parse/1)

    struct!(AvroMap, data)
  end

  defp do_parse(%{"type" => "enum", "symbols" => symbols} = enum) when is_list(symbols) do
    data =
      enum
      |> cast(AvroEnum, [:aliases, :doc, :name, :namespace, :symbols])
      |> drop([:type])
      |> validate_required([:name, :symbols])
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
      |> update_in([:items], &do_parse/1)

    struct!(Array, data)
  end

  defp do_parse(%{"type" => "fixed"} = fixed) do
    data =
      fixed
      |> cast(Fixed, [:aliases, :doc, :name, :namespace, :size])
      |> drop([:type])
      |> validate_required([:name, :size])
      |> validate_name()
      |> validate_namespace()
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
      |> extract_data()
      |> put_in([:type], do_parse(type))

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

  defp validate_name({_data, _rest, {_type, raw}} = input) do
    validate_field(input, :name, fn value ->
      unless valid_name?(value) do
        error({:invalid_name, {:name, value}, raw})
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

  defp extract_metadata({data, rest, _info}) do
    Map.put(data, :metadata, rest)
  end

  defp extract_data({data, rest, {type, raw}}) do
    if rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), type, raw})
    end

    data
  end

  defp drop({data, rest, info}, keys) do
    {data, Map.drop(rest, Enum.map(keys, &to_string/1)), info}
  end

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
