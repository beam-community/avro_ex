defmodule AvroEx.Schema.Record.Field do
  use TypedStruct

  alias AvroEx.{Schema}
  alias AvroEx.Schema.Context

  typedstruct do
    field :name, String.t(), enforce: true
    field :doc, String.t()
    field :type, Schema.schema_types(), enforce: true
    field :default, Schema.schema_types()
    field :aliases, [Schema.alias()], default: []
    field :metadata, Schema.metadata(), default: %{}
  end

  @spec match?(AvroEx.Schema.Record.Field.t(), AvroEx.Schema.Context.t(), any()) :: boolean()
  def match?(%__MODULE__{type: type}, %Context{} = context, data) do
    Schema.encodable?(type, context, data)
  end
end
