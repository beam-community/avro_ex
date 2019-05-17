defmodule AvroEx.Term do
  use Ecto.Type

  @spec cast(any()) :: {:ok, any()}
  def cast(type), do: {:ok, type}

  @spec load(any()) :: {:ok, any()}
  def load(data), do: {:ok, data}

  @spec dump(any()) :: {:ok, any()}
  def dump(data), do: {:ok, data}

  @spec type() :: :term
  def type, do: :term
end
