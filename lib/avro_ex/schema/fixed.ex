defmodule AvroEx.Schema.Fixed do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset

  alias AvroEx.Schema

  embedded_schema do
    field(:aliases, {:array, :string}, default: [])
    field(:metadata, {:map, :string}, default: %{})
    field(:name, :string)
    field(:namespace, :string)
    field(:qualified_names, {:array, :string}, default: [])
    field(:size, :integer)
  end

  @type t :: %__MODULE__{
          aliases: [Schema.alias()],
          metadata: %{String.t() => String.t()},
          name: Schema.name(),
          namespace: Schema.namespace(),
          size: integer
        }

  @required_fields [:name, :size]
  @optional_fields [:aliases, :metadata, :namespace]

  SchemaMacros.cast_schema(data_fields: [:aliases, :name, :namespace, :size, :qualified_names])

  @spec changeset(AvroEx.Schema.Fixed.t(), %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}) ::
          Ecto.Changeset.t()
  def changeset(%__MODULE__{} = fixed, %{"type" => "fixed"} = params) do
    fixed
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
  end
end
