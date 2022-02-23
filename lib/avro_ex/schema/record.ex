defmodule AvroEx.Schema.Record do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset
  alias __MODULE__.Field
  alias AvroEx.{Schema}

  embedded_schema do
    field(:aliases, {:array, :string}, default: [])
    field(:doc, :string)
    field(:name, :string)
    field(:namespace, :string)
    field(:qualified_names, {:array, :string}, default: [])
    field(:metadata, :map, default: %{})

    embeds_many(:fields, Field)
  end

  @type t :: %__MODULE__{
          aliases: [Schema.alias()],
          doc: Schema.doc(),
          name: Schema.name(),
          namespace: Schema.namespace(),
          metadata: %{String.t() => String.t()}
        }

  @required_fields [:name]
  @optional_fields [:namespace, :doc, :aliases, :metadata]

  SchemaMacros.cast_schema(data_fields: [:aliases, :doc, :fields, :name, :namespace, :qualified_names, :symbols])

  @spec changeset(AvroEx.Schema.Record.t(), %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}) ::
          map()
  def changeset(%__MODULE__{} = record, %{"type" => "record"} = params) do
    record
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> cast_embed(:fields)
  end
end
