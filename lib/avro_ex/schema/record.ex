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
  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset
  alias __MODULE__.Field
  alias AvroEx.{Schema}
  alias AvroEx.Schema.Context

  embedded_schema do
    field :aliases, {:array, :string}, default: []
    field :doc, :string
    field :name, :string
    field :namespace, :string
    field :qualified_names, {:array, :string}, default: []
    field :metadata, :map, default: %{}

    embeds_many :fields, Field
  end

  @type t :: %__MODULE__{
    aliases: [Schema.alias],
    doc: Schema.doc,
    name: Schema.name,
    namespace: Schema.namespace,
    metadata: %{String.t => String.t}
  }

  @required_fields [:name]
  @optional_fields [:namespace, :doc, :aliases, :metadata]

  SchemaMacros.cast_schema([data_fields: [:aliases, :doc, :fields, :name, :namespace, :qualified_names, :symbols]])

  def changeset(%__MODULE__{} = record, %{"type" => "record"} = params) do
    record
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> cast_embed(:fields)
  end

  @spec match?(t, Context.t, term) :: boolean
  def match?(%__MODULE__{fields: fields}, %Context{} = context, data) when is_map(data) and map_size(data) == length(fields) do
    Enum.all?(fields, fn(%Field{name: name} = field) ->
      Map.has_key?(data, name) and Schema.encodable?(field, context, data[name])
    end)
  end

  def match?(_, _,  _), do: false
end
