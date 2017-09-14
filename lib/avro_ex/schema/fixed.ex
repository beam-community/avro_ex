defmodule AvroEx.Schema.Fixed do
  use Ecto.Schema

  import Ecto.Changeset
  alias AvroEx.Error
  alias AvroEx.Schema.Context

  @type alias :: name
  @type full_name :: String.t
  @type name :: String.t
  @type namespace :: nil | String.t

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :name, :string
    field :namespace, :string
    field :size, :integer
  end

  @type t :: %__MODULE__{
    aliases: [alias],
    name: name,
    namespace: namespace,
    size: integer
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

  @spec full_name(t) :: full_name
  def full_name(%__MODULE__{namespace: namespace, name: name}) do
    full_name(namespace, name)
  end

  @spec full_names(t) :: [full_name]
  def full_names(%__MODULE__{aliases: aliases, namespace: namespace} = fixed) when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn(name) ->
        full_name(namespace, name)
      end)

    [full_name(fixed) | full_aliases]
  end

  @spec full_name(namespace, name) :: full_name
  def full_name(nil, name) when is_binary(name) do
    name
  end

  def full_name(namespace, name) when is_binary(namespace) and is_binary(name) do
    "#{namespace}.#{name}"
  end

  @spec match?(t, Context.t, term) :: boolean
  def match?(%__MODULE__{size: size}, %Context{}, data) when is_binary(data) and byte_size(data) == size do
    true
  end

  def match?(_, _,  _), do: false
end
