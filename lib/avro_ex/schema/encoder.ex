defmodule AvroEx.Schema.Encoder do
  @doc false

  alias AvroEx.Schema
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.{Array, Fixed, Primitive, Record, Record.Field, Reference, Union}

  @spec encode(Schema.t(), Keyword.t()) :: String.t()
  def encode(%Schema{schema: schema}, opts) do
    config = %{canonical?: Keyword.get(opts, :canonical, false)}

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

  defp process(struct, config) do
    data =
      for {k, v} <- extract(struct), not empty?(v), keep?(k, config), into: %{} do
        case k do
          k when k in [:values, :items, :type] -> {k, do_encode(v, config)}
          :fields -> {k, Enum.map(v, &do_encode(&1, config))}
          _ -> {k, v}
        end
      end

    if config.canonical? do
      Map.delete(data, :metadata)
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
end
