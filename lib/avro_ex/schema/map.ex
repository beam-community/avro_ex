defmodule AvroEx.Schema.Map do
  use TypedStruct

  alias AvroEx.{Schema}
  alias AvroEx.Schema.{Context, Primitive}

  typedstruct do
    field :metadata, Schema.metadata(), default: %{}
    field :values, Schema.schema_types(), enforce: true
    field :default, map(), default: %{}
  end

  @spec match?(any(), any(), any()) :: boolean()
  def match?(%__MODULE__{values: value_type}, %Context{} = context, data) when is_map(data) do
    Enum.all?(data, fn {key, value} ->
      Schema.encodable?(%Primitive{type: :string}, context, key) and
        Schema.encodable?(value_type, context, value)
    end)
  end

  def match?(_map, _context, _data), do: false
end
