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
    data = type |> extract_keys([]) |> Map.put(:type, String.to_existing_atom(primitive))
    struct(Primitive, data)
  end

  defp do_parse(%{"type" => "record", "fields" => fields} = record) when is_list(fields) do
    data =
      record
      |> extract_keys(["name", "namespace", "doc", "qualified_names"])
      |> Map.put(:fields, parse_fields(fields))

    struct(Record, data)
  end

  defp do_parse(other) do
    throw(AvroEx.Schema.DecodeError.new(reason: :invalid_format, data: other))
  end

  defp parse_fields(%{"type" => type}) do
    %{}
  end

  # TODO add required and optional
  defp extract_keys(data, fields) do
    {fields, metadata} = Map.split(data, fields)

    fields
    |> atom_keys()
    |> Map.put(:metadata, metadata)
  end

  # convert maps keys to known atoms
  defp atom_keys(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_existing_atom(k), v}
      {k, v} when is_atom(k) -> {k, v}
    end)
  end
end
