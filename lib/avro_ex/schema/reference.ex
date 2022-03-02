defmodule AvroEx.Schema.Reference do
  defstruct [:type]

  def new(type) when is_binary(type) do
    %__MODULE__{type: type}
  end
end
