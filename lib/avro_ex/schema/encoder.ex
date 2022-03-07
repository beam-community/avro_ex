defmodule AvroEx.Schema.Encoder do
  @moduledoc false

  require Jason.Helpers

  alias AvroEx.Schema
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.{Array, Fixed, Primitive, Record, Record.Field, Reference, Union}

  @spec encode(Schema.t(), Keyword.t()) :: String.t()
  def encode(%Schema{schema: schema}, opts) do
    config = %{canonical?: Keyword.get(opts, :canonical, false), namespace: Schema.namespace(schema)}

    schema |> do_encode(config) |> Jason.encode!()
  end

  defp do_encode(%Primitive{} = primitive, config) do
    if config.canonical? do
      primitive.type
    else
      Map.put(primitive.metadata, :type, primitive.type)
    end
  end

  defp do_encode(%Reference{type: type}, _config) do
    type
  end

  defp do_encode(%Union{possibilities: possibilities}, config) do
    Enum.map(possibilities, &do_encode(&1, config))
  end

  defp do_encode(binary, _config) when is_binary(binary), do: binary

  defp do_encode(struct, config) do
    process(struct, config)
  end

  defp process(%struct_type{} = struct, config) do
    config = update_in(config.namespace, &Schema.namespace(struct, &1))

    data =
      for {k, v} <- extract(struct), not empty?(v), keep?(k, config), into: %{} do
        case k do
          k when k in [:values, :items, :type] -> {k, do_encode(v, config)}
          :fields -> {k, Enum.map(v, &do_encode(&1, config))}
          _ -> {k, v}
        end
      end

    if config.canonical? do
      full_name = Schema.full_name(struct, config.namespace)

      data
      |> Map.put(:name, full_name)
      |> Map.delete(:metadata)
      |> order_json_keys(struct_type)
    else
      merge_metadata(data)
    end
  end

  defp empty?([]), do: true
  defp empty?(nil), do: true
  defp empty?(""), do: true
  defp empty?(map) when map == %{}, do: true
  defp empty?(_), do: false

  defp keep?(k, %{canonical?: true}) do
    k in ~w(type name fields symbols items values size)a
  end

  defp keep?(_k, _config), do: true

  defp merge_metadata(%{metadata: _} = data) do
    {metadata, data} = Map.pop(data, :metadata)
    Map.merge(metadata, data)
  end

  defp merge_metadata(data), do: data

  defp extract(%struct{} = data) do
    type =
      case struct do
        AvroEnum -> "enum"
        AvroMap -> "map"
        Array -> "array"
        Fixed -> "fixed"
        Record -> "record"
        Field -> data.type
      end

    data |> Map.from_struct() |> Map.put(:type, type)
  end

  # name, type, fields, symbols, items, values, size
  defp order_json_keys(data, type) do
    case type do
      Array ->
        Jason.Helpers.json_map_take(data, [:type, :items])

      AvroEnum ->
        Jason.Helpers.json_map_take(data, [:name, :type, :symbols])

      AvroMap ->
        Jason.Helpers.json_map_take(data, [:type, :values])

      Fixed ->
        Jason.Helpers.json_map_take(data, [:name, :type, :size])

      Field ->
        Jason.Helpers.json_map_take(data, [:name, :type])

      Record ->
        Jason.Helpers.json_map_take(data, [:name, :type, :fields])
    end
  end
end
