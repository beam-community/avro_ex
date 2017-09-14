defmodule AvroEx.Schema.Record.Field do
  use Ecto.Schema
  import Ecto.Changeset

  alias Ecto.Changeset
  alias AvroEx.{Schema, Term}
  alias AvroEx.Schema.Context

  embedded_schema do
    field :name, :string
    field :doc, :string
    field :type, Term
    field :default, Term
    field :aliases, {:array, :string}, default: []
  end

  @required_fields [:name, :type]
  @optional_fields [:doc, :default, :aliases]

  def changeset(%__MODULE__{} = field, params) do
    field
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> encode_type
  end

  def match?(%__MODULE__{type: type}, %Context{} = context, data) do
    Schema.encodable?(type, context, data)
  end

  defp encode_type(%Changeset{} = cs) do
    type =
      cs
      |> Changeset.get_field(:type)
      |> Schema.cast

    case type do
      {:ok, value} -> Changeset.put_change(cs, :type, value)
      {:error, reason} -> Changeset.add_error(cs, :type, reason)
    end
  end
end

defmodule AvroEx.Schema.Record do
  use Ecto.Schema

  import Ecto.Changeset
  alias __MODULE__.Field
  alias AvroEx.{Error, Schema}
  alias AvroEx.Schema.Context

  @type name :: String.t
  @type namespace :: nil | String.t
  @type full_name :: String.t
  @type doc :: nil | String.t
  @type alias :: name

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :doc, :string
    field :name, :string
    field :namespace, :string

    embeds_many :fields, Field
  end

  @type t :: %__MODULE__{
    name: name,
    namespace: namespace,
    doc: doc,
    aliases: [alias]
  }

  @required_fields [:name]
  @optional_fields [:namespace, :doc, :aliases]

  def cast(params) do
    cs = changeset(%__MODULE__{}, params)
    
    if cs.valid? do
      {:ok, apply_changes(cs)}
    else
      {:error, Error.errors(cs)}
    end
  end

  def changeset(%__MODULE__{} = record, %{"type" => "record"} = params) do
    record
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> cast_embed(:fields)
  end

  @spec full_name(t) :: full_name
  def full_name(%__MODULE__{namespace: namespace, name: name}) do
    full_name(namespace, name)
  end

  @spec full_names(t) :: [full_name]
  def full_names(%__MODULE__{aliases: aliases, namespace: namespace} = record) when is_list(aliases) do
    full_aliases =
      Enum.map(aliases, fn(name) ->
        full_name(namespace, name)
      end)

    [full_name(record) | full_aliases]
  end

  @spec full_name(namespace, name) :: full_name
  def full_name(nil, name) when is_binary(name) do
    name
  end

  def full_name(namespace, name) when is_binary(namespace) and is_binary(name) do
    "#{namespace}.#{name}"
  end

  @spec match?(t, Context.t, term) :: boolean
  def match?(%__MODULE__{fields: fields}, %Context{} = context, data) when is_map(data) and map_size(data) == length(fields) do
    Enum.all?(fields, fn(%Field{name: name} = field) ->
      Map.has_key?(data, name) and Schema.encodable?(field, context, data[name])
    end)
  end

  def match?(_, _,  _), do: false
end
