defmodule AvroEx.Schema.Fixed do
  use TypedStruct

  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  typedstruct do
    field :aliases, [Schema.alias()], default: []
    field :metadata, Schema.metadata(), default: %{}
    field :name, String.t(), enforce: true
    field :doc, String.t()
    field :namespace, String.t()
    field :size, integer(), enforce: true
  end

  @spec match?(t, Context.t(), term) :: boolean
  def match?(%__MODULE__{size: size}, %Context{}, data)
      when is_binary(data) and byte_size(data) == size do
    true
  end

  def match?(_fixed, _context, _data), do: false
end
