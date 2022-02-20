defmodule AvroEx.Error do
  @moduledoc false

  @doc false
  @spec errors(Ecto.Changeset.t(), atom()) :: [any()]
  def errors(%Ecto.Changeset{} = cs, field) do
    cs.errors
    |> Keyword.get_values(field)
    |> Enum.map(fn
      {err, _} when is_binary(err) -> err
      any -> any
    end)
  end

  @doc false
  @spec errors(Ecto.Changeset.t()) :: any()
  def errors(%Ecto.Changeset{} = cs) do
    Enum.reduce(cs.errors, %{}, fn {field, {value, _}}, acc ->
      Map.update(acc, field, [value], fn tail -> [value | tail] end)
    end)
  end

  @doc false
  @spec error(any()) :: any()
  def error(message) do
    message
  end
end
