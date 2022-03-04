defmodule AvroEx.Schema.Reference do
  defstruct [:type]

  @type t :: %__MODULE__{}

  @spec new(String.t()) :: t()
  def new(type) when is_binary(type) do
    %__MODULE__{type: type}
  end
end
