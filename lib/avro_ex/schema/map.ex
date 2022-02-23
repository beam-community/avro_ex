defmodule AvroEx.Schema.Map do
  use Ecto.Schema

  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset

  alias AvroEx.{Schema, Term}
  alias Ecto.Changeset

  @primary_key false
  @required_fields [:metadata, :values]

  embedded_schema do
    field(:metadata, :map, default: %{})
    field(:values, Term)
  end

  @type t :: %__MODULE__{
          metadata: %{String.t() => String.t()},
          values: Schema.schema_types()
        }

  SchemaMacros.cast_schema(data_fields: [:values])

  @spec changeset(
          AvroEx.Schema.Map.t(),
          :invalid | %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}
        ) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = struct, params) do
    struct
    |> cast(params, @required_fields)
    |> validate_required(@required_fields)
    |> encode_values
  end

  defp encode_values(%Changeset{} = cs) do
    values =
      cs
      |> get_field(:values)
      |> Schema.cast()

    case values do
      {:ok, value} -> put_change(cs, :values, value)
      {:error, reason} -> add_error(cs, :values, reason)
    end
  end
end
