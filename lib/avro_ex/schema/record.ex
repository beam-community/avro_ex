defmodule AvroEx.Schema.Record do
  use Ecto.Schema
  require AvroEx.Schema.Macros, as: SchemaMacros

  import Ecto.Changeset
  alias __MODULE__.Field
  alias AvroEx.{Schema}
  alias AvroEx.Schema.Context

  embedded_schema do
    field(:aliases, {:array, :string}, default: [])
    field(:doc, :string)
    field(:name, :string)
    field(:namespace, :string)
    # TODO remove all of these
    field(:qualified_names, {:array, :string}, default: [])
    # TODO remove
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

  @spec match?(t, Context.t(), term) :: boolean
  def match?(%__MODULE__{fields: fields}, %Context{} = context, data)
      when is_map(data) and map_size(data) == length(fields) do
    Enum.all?(fields, fn %Field{name: name} = field ->
      data =
        Map.new(data, fn
          {k, v} when is_binary(k) -> {k, v}
          {k, v} when is_atom(k) -> {to_string(k), v}
        end)

      Map.has_key?(data, name) and Schema.encodable?(field, context, data[name])
    end)
  end

  def match?(_record, _context, _data), do: false
end
