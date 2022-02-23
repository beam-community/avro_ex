defmodule AvroEx.Schema.Array do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros
  alias AvroEx.{Schema, Term}
  alias Ecto.Changeset
  import Ecto.Changeset

  @primary_key false
  @required_fields [:items]
  @optional_fields [:metadata]

  embedded_schema do
    field(:items, Term)
    field(:metadata, :map, default: %{})
  end

  @type t :: %__MODULE__{
          items: Schema.schema_types(),
          metadata: %{String.t() => String.t()}
        }

  SchemaMacros.cast_schema(data_fields: [:items])

  @spec changeset(
          AvroEx.Schema.Array.t(),
          :invalid | %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}
        ) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = struct, params) do
    struct
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> encode_items
  end

  defp encode_items(%Changeset{} = cs) do
    items =
      cs
      |> get_field(:items)
      |> Schema.cast()

    case items do
      {:ok, item} -> put_change(cs, :items, item)
      {:error, reason} -> add_error(cs, :items, reason)
    end
  end
end
