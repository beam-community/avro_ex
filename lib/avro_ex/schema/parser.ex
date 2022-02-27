defmodule AvroEx.Schema.Parser do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}

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
  end

  defp do_parse(%{"type" => primitive} = type) when primitive in @primitives do
    {data, rest} = extract_keys(type, [], [], [:type])

    struct!(Primitive, data |> Map.put(:metadata, rest) |> Map.put(:type, String.to_existing_atom(primitive)))
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
end
