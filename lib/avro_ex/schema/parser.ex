defmodule AvroEx.Schema.Parser do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum

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
    {data, rest} = extract_keys(type, [], [], [:type])

    struct!(Primitive, data |> Map.put(:metadata, rest) |> Map.put(:type, String.to_existing_atom(primitive)))
  end

  defp do_parse(%{"type" => "enum", "symbols" => symbols} = enum) when is_list(symbols) do
    {data, rest} = extract_keys(enum, [:name, :symbols], [:namespace, :doc, :aliases], [:type])

    if rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), AvroEnum, enum})
    end

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

  defp do_parse(%{"type" => "record", "fields" => fields} = record) when is_list(fields) do
    {data, rest} = extract_keys(record, [:name, :fields], [:namespace, :doc, :aliases], [:type])

    if rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), Record, record})
    end

    data = update_in(data, [:fields], fn fields -> Enum.map(fields, &parse_fields/1) end)

    struct!(Record, data)
  end

  defp do_parse(other) do
    error({:invalid_format, other})
  end

  defp parse_fields(%{"type" => type} = field) do
    inner_type = do_parse(type)

    {data, rest} = extract_keys(field, [:name, :type], [:doc, :default, :namespace], [:type])

    if rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), Record.Field, field})
    end

    struct!(Record.Field, Map.put(data, :type, inner_type))
  end

  defp cast(data, keys) do
  end

  defp validate_required(data, keys) do
  end

  defp extract_metadata({data, rest}) do
    Map.put(data, :metadata, rest)
  end

  defp extract_data({data, rest}, type, raw) do
    if rest != %{} do
      error({:unrecognized_fields, Map.keys(rest), type, raw})
    end

    data
  end

  defp extract_keys(data, required, optional, drop) do
    {data, rest} = extract_required(data, required, %{})
    {data, rest} = extract_optional(rest, optional, data)
    rest = drop(rest, drop)
    {data, rest}
  end

  defp extract_required(data, required, into) do
    Enum.reduce(required, {into, data}, fn k, {required, data} ->
      case Map.pop(data, to_string(k)) do
        {nil, data} ->
          error({:missing_required, k, data})

        {value, data} ->
          {Map.put(required, k, value), data}
      end
    end)
  end

  defp extract_optional(data, optional, into) do
    Enum.reduce(optional, {into, data}, fn k, {optional, data} ->
      case Map.pop(data, to_string(k)) do
        {nil, data} -> {optional, data}
        {value, data} -> {Map.put(optional, k, value), data}
      end
    end)
  end

  defp drop(data, drop) do
    Map.drop(data, Enum.map(drop, &to_string/1))
  end

  # convert maps keys to known atoms
  defp atom_keys(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_existing_atom(k), v}
      {k, v} when is_atom(k) -> {k, v}
    end)
  end

  defp error(info) do
    info |> AvroEx.Schema.DecodeError.new() |> throw()
  end

  defp valid_name?(name) when is_binary(name) do
    Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, name)
  end

  defp valid_name?(_), do: false
end
