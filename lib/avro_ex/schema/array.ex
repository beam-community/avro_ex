defmodule AvroEx.Schema.Array do
  use TypedStruct

  alias AvroEx.{Schema, Schema.Context}

  typedstruct do
    field :items, Schema.schema_types(), enforce: true
    field :default, [Schema.schema_types()], default: []
    field :metadata, Schema.metadata(), default: %{}
  end

  @spec match?(any(), any(), any()) :: boolean()
  def match?(%__MODULE__{items: item_type}, %Context{} = context, data) when is_list(data) do
    Enum.all?(data, fn item ->
      Schema.encodable?(item_type, context, item)
    end)
  end

  def match?(_array, _context, _data), do: false
end
