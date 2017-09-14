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
    field :symbols, {:array, :string}
  end

  @type full_name :: String.t

  @type t :: %__MODULE__{
    aliases: [Record.alias],
    doc: Record.doc,
    name: Record.name,
    namespace: Record.namespace,
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

  @spec full_name(t) :: full_name
  def full_name(%__MODULE__{namespace: namespace, name: name}) do
    full_name(namespace, name)
  end

  @spec full_name(Record.namespace, Record.name) :: Record.full_name
  def full_name(nil, name) when is_binary(name) do
    name
  end

  def full_name(namespace, name) when is_binary(namespace) and is_binary(name) do
    "#{namespace}.#{name}"
  end

  def full_names(%__MODULE__{aliases: aliases, namespace: namespace} = enum) when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn(name) ->
        full_name(namespace, name)
      end)

    [full_name(enum) | full_aliases]
  end

  def match?(%__MODULE__{symbols: symbols}, %Context{}, data) when is_binary(data) do
    data in symbols
  end

  def match?(_, _,  _), do: false
end
