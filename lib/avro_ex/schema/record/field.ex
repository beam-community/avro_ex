defmodule AvroEx.Schema.Record.Field do
  use Ecto.Schema
  import Ecto.Changeset

  alias Ecto.Changeset
  alias AvroEx.{Schema, Term}
  alias AvroEx.Schema.Context

  @type t() :: %__MODULE__{}

  embedded_schema do
    field(:name, :string)
    field(:doc, :string)
    field(:type, Term)
    field(:default, Term)
    field(:aliases, {:array, :string}, default: [])
  end

  @required_fields [:name, :type]
  @optional_fields [:doc, :default, :aliases]

  @spec changeset(
          AvroEx.Schema.Record.Field.t(),
          :invalid | %{optional(:__struct__) => none(), optional(atom() | binary()) => any()}
        ) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = field, params) do
    field
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> encode_type
  end

  @spec match?(AvroEx.Schema.Record.Field.t(), AvroEx.Schema.Context.t(), any()) :: boolean()
  def match?(%__MODULE__{type: type}, %Context{} = context, data) do
    Schema.encodable?(type, context, data)
  end

  defp encode_type(%Changeset{} = cs) do
    type =
      cs
      |> Changeset.get_field(:type)
      |> Schema.cast()

    case type do
      {:ok, value} -> Changeset.put_change(cs, :type, value)
      {:error, reason} -> Changeset.add_error(cs, :type, reason)
    end
  end
end
