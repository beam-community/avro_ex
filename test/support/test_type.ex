defmodule TestType do
  @doc false

  @enforce_keys [:status, :details]
  defstruct status: 0, details: ""

  @type t :: %__MODULE__{status: Integer.t(), details: String.t()}
end
