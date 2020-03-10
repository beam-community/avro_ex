defmodule AvroEx.Validation do
  alias Ecto.Changeset

  @spec validate_string(Ecto.Changeset.t(), atom()) :: Ecto.Changeset.t()
  def validate_string(changeset, field) do
    value = Changeset.get_field(changeset, field)

    if is_binary(value) do
      changeset
    else
      Changeset.add_error(changeset, field, "must be a string")
    end
  end
end
