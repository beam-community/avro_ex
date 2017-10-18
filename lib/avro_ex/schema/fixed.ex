defmodule AvroEx.Schema.Fixed do
  use Ecto.Schema

  import Ecto.Changeset
  alias AvroEx.Error
  alias AvroEx.Schema.Context
  alias AvroEx.Schema

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :name, :string
    field :namespace, :string
    field :size, :integer
    field :qualified_names, {:array, :string}, default: []
  end

  @type t :: %__MODULE__{
    aliases: [Schema.alias],
    name: Schema.name,
    namespace: Schema.namespace,
    size: Schema.integer
  }

  @required_fields [:name, :size]
  @optional_fields [:aliases, :namespace]

  def cast(params) do
    cs = changeset(%__MODULE__{}, params)
    
    if cs.valid? do
      {:ok, apply_changes(cs)}
    else
      {:error, Error.errors(cs)}
    end
  end

  def changeset(%__MODULE__{} = fixed, %{"type" => "fixed"} = params) do
    fixed
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
  end

  @spec match?(t, Context.t, term) :: boolean
  def match?(%__MODULE__{size: size}, %Context{}, data) when is_binary(data) and byte_size(data) == size do
    true
  end

  def match?(_, _,  _), do: false
end
