defmodule AvroEx.Schema.Union do
  use TypedStruct

  alias AvroEx.{Schema, Schema.Context}

  typedstruct enforce: true do
    field :possibilities, [Schema.schema_types()], enforce: true
  end

  @spec match?(AvroEx.Schema.Union.t(), Context.t(), any()) :: boolean()
  def match?(%__MODULE__{} = union, %Context{} = context, data) do
    Enum.any?(union.possibilities, fn schema ->
      Schema.encodable?(schema, context, data)
    end)
  end
end
