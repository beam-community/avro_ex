defmodule AvroEx.Schema.Fixed do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset

  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  embedded_schema do
    field(:aliases, {:array, :string}, default: [])
    field(:metadata, {:map, :string}, default: %{})
    field(:name, :string)
    field(:doc, :string, default: "")
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
  @optional_fields [:aliases, :metadata, :namespace, :doc]

  SchemaMacros.cast_schema(data_fields: [:aliases, :name, :namespace, :size, :qualified_names, :doc])

  @spec changeset(AvroEx.Schema.Fixed.t(), %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}) ::
          Ecto.Changeset.t()
  def changeset(%__MODULE__{} = fixed, %{"type" => "fixed"} = params) do
    fixed
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
  end

  @spec match?(t, Context.t(), term) :: boolean
  def match?(%__MODULE__{size: size}, %Context{}, data)
      when is_binary(data) and byte_size(data) == size do
    true
  end

  def match?(_fixed, _context, _data), do: false
end
