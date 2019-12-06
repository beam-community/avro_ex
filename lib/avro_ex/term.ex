defmodule AvroEx.Term do
  use Ecto.Type

  def cast(type), do: {:ok, type}
  def load(data), do: {:ok, data}
  def dump(data), do: {:ok, data}
  def type, do: :term
end
