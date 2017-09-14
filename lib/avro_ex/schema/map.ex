defmodule AvroEx.Schema.Map do
  use Ecto.Schema
  alias AvroEx.{Error, Schema, Term}
  alias AvroEx.Schema.{Context, Primitive}
  alias Ecto.Changeset
  import Ecto.Changeset

  @primary_key false
  @required_fields [:values]

  embedded_schema do
    field :values, Term
  end

  @type t :: %__MODULE__{
    values: Schema.schema_types,
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
    |> cast(params, @required_fields)
    |> validate_required(@required_fields)
    |> encode_values
  end

  defp encode_values(%Changeset{} = cs) do
    values =
      cs
      |> get_field(:values)
      |> Schema.cast

    case values do
      {:ok, value} -> put_change(cs, :values, value)
      {:error, reason} -> add_error(cs, :values, reason)
    end
  end

  def match?(%__MODULE__{values: value_type}, %Context{} = context, data) when is_map(data) do
    Enum.all?(data, fn({key, value}) ->
      Schema.encodable?(%Primitive{type: :string}, context, key) and Schema.encodable?(value_type, context, value)
    end)
  end

  def match?(_, _,  _), do: false
end
