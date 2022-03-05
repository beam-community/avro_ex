defmodule AvroEx.Schema.Context do
  use TypedStruct
  alias AvroEx.Schema

  typedstruct do
    field :names, %{String.t() => Schema.schema_types()}, default: %{}
  end

  @spec lookup(t(), String.t()) :: Schema.schema_types() | nil
  def lookup(%__MODULE__{names: names}, name) do
    names[name]
  end
end
