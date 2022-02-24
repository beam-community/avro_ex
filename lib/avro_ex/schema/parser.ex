defmodule AvroEx.Schema.Parser do
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

  for p <- @primitives do
    def primitive?(unquote(p)), do: true
  end

  def primitive?(_), do: false

  @spec decode!(term()) :: AvroEx.Schema.t()
  def decode!(data) do
    try do
      do_decode(data)
    catch
      :throw, %AvroEx.Schema.DecodeError{} = err -> raise err
    end
  end

  defp do_decode(binary) when is_binary(binary) do
  end

  defp do_decode(list) when is_list(list) do
  end

  defp do_decode(map) when is_map(map) do
  end

  defp do_decode(other) do
    throw(AvroEx.Schema.DecodeError.new(reason: other))
  end
end
