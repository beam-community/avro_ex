defmodule AvroEx.Schema.Union do
  use Ecto.Schema
  import Ecto.Changeset

  alias AvroEx.{Schema, Term}

  @primary_key false
  @required_fields [:possibilities]
  @optional_fields []

  embedded_schema do
    field(:possibilities, {:array, Term})
  end

  @type t :: %__MODULE__{
          possibilities: [Schema.schema_types()]
        }

  @spec cast(maybe_improper_list()) :: {:ok, t()} | {:error, any()}
  def cast(union) when is_list(union) do
    cs = changeset(%__MODULE__{possibilities: []}, %{possibilities: union})

    if cs.valid? do
      {:ok,
       %__MODULE__{
         possibilities: get_field(cs, :possibilities)
       }}
    else
      {:error, Schema.errors(cs)}
    end
  end

  @spec changeset(t(), term()) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = union, params) do
    union
    |> cast(params, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> cast_possibilities()
  end

  @spec cast_possibilities(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  def cast_possibilities(%Ecto.Changeset{} = cs) do
    possibilities =
      cs
      |> get_field(:possibilities)
      |> Enum.map(&Schema.cast/1)

    valid =
      Enum.all?(possibilities, fn
        {:ok, _schema} -> true
        {:error, _} -> false
      end)

    if valid do
      possibilities = Enum.map(possibilities, fn {:ok, schema} -> schema end)
      put_change(cs, :possibilities, possibilities)
    else
      errors =
        Enum.filter(possibilities, fn
          {:error, reason} -> reason
          {:ok, _} -> false
        end)

      Enum.reduce(errors, cs, fn error, cs ->
        add_error(cs, :possibilities, error)
      end)
    end
  end
end
