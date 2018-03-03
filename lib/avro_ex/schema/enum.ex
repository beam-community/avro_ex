defmodule AvroEx.Schema.Enum do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros
  alias AvroEx.Schema.Context
  import Ecto.Changeset

  @primary_key false
  @optional_fields [:aliases, :doc, :metadata, :namespace]
  @required_fields [:name, :symbols]

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :doc, :string
    field :metadata, :map, default: %{}
    field :name, :string
    field :namespace, :string
    field :qualified_names, {:array, :string}, default: []
    field :symbols, {:array, :string}
  end

  @type full_name :: String.t

  @type t :: %__MODULE__{
    aliases: [Schema.alias],
    doc: Schema.doc,
    metadata: %{String.t => String.t},
    name: Schema.name,
    namespace: Schema.namespace,
    symbols: [String.t]
  }

  SchemaMacros.cast_schema([data_fields: [:aliases, :doc, :name, :namespace, :qualified_names, :symbols]])

  def changeset(%__MODULE__{} = struct, params) do
    struct
    |> cast(params, @optional_fields ++ @required_fields)
    |> validate_required(@required_fields)
  end

  def match?(%__MODULE__{symbols: symbols}, %Context{}, data) when is_binary(data) do
    data in symbols
  end

  def match?(_, _,  _), do: false
end
