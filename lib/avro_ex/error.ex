defmodule AvroEx.Error do
  def errors(%Ecto.Changeset{} = cs, field) do
    cs.errors
    |> Keyword.get_values(field)
    |> Enum.map(fn
      {err, _} when is_binary(err) -> err
      any -> any
    end)
  end

  def errors(%Ecto.Changeset{} = cs) do
    Enum.reduce(cs.errors, %{}, fn
      {field, {value, _}}, acc -> Map.update(acc, field, [value], fn tail -> [value | tail] end)
    end)
  end

  def error(message) do
    message
  end
end
