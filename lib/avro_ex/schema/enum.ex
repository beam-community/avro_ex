defmodule AvroEx.Schema.Enum do
  use TypedStruct

  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  typedstruct do
    field :aliases, [Schema.alias()]
    field :doc, Schema.doc()
    field :metadata, Schema.metadata(), default: %{}
    field :name, Schema.name(), enforce: true
    field :namespace, Schema.namespace()
    field :symbols, [String.t()], enforce: true
  end

  @spec match?(any(), any(), any()) :: boolean()
  def match?(%__MODULE__{symbols: symbols}, %Context{}, data) when is_binary(data) do
    data in symbols
  end

  def match?(_enum, _context, _data), do: false
end
