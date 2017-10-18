defmodule AvroEx.Schema.Enum do
  use Ecto.Schema
  alias AvroEx.Error
  alias AvroEx.Schema.Context
  import Ecto.Changeset

  @primary_key false
  @optional_fields [:aliases, :doc, :namespace]
  @required_fields [:name, :symbols]

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :doc, :string
    field :name, :string
    field :namespace, :string
    field :qualified_names, {:array, :string}, default: []
    field :symbols, {:array, :string}
  end

  @type full_name :: String.t

  @type t :: %__MODULE__{
    aliases: [Schema.alias],
    doc: Schema.doc,
    name: Schema.name,
    namespace: Schema.namespace,
    symbols: [String.t]
  }

  def cast(params) do
    cs = changeset(%__MODULE__{}, params)

    if cs.valid? do
      {:ok, apply_changes(cs)}
    else
      {:error, Error.errors(cs)}
    end
  end

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
