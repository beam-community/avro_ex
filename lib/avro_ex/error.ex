defmodule AvroEx.Error do
  @spec errors(Ecto.Changeset.t(), atom()) :: [any()]
  def errors(%Ecto.Changeset{} = cs, field) do
    cs.errors
    |> Keyword.get_values(field)
    |> Enum.map(fn
      {err, _} when is_binary(err) -> err
      any -> any
    end)
  end

  @spec errors(Ecto.Changeset.t()) :: any()
  def errors(%Ecto.Changeset{} = cs) do
    Enum.reduce(cs.errors, %{}, fn {field, {value, _}}, acc ->
      Map.update(acc, field, [value], fn tail -> [value | tail] end)
    end)
  end

  @spec error(any()) :: any()
  def error(message) do
    message
  end
end
