defmodule AvroEx.Schema.Primitive do
  @moduledoc """
  Functions for handling primitive types in Avro schemas
  """

  use Ecto.{Schema, Type}

  @primary_key false

  @type primitive ::
          nil
          | :boolean
          | :integer
          | :long
          | :float
          | :double
          | :bytes
          | :string

  embedded_schema do
    field(:metadata, :map, default: %{})
    # Actually a primitive - placeholder until I create a custom ecto type
    field(:type, :string)
  end

  @type t :: %__MODULE__{
          metadata: %{String.t() => String.t()},
          type: primitive
        }

  @spec cast(any()) :: :error | {:ok, AvroEx.Schema.Primitive.t()}
  def cast(%{"type" => type} = data) do
    {:ok, %__MODULE__{type: type(type), metadata: Map.delete(data, "type")}}
  end

  def cast(nil) do
    {:ok, %__MODULE__{type: type(nil)}}
  end

  def cast(type) when is_binary(type) do
    {:ok, %__MODULE__{type: type(type)}}
  end

  def cast(_), do: :error

  @spec load(any()) :: {:ok, any()}
  def load(data), do: {:ok, data}

  @spec dump(any()) :: {:ok, any()}
  def dump(data), do: {:ok, data}

  @spec type() :: :primitive
  def type, do: :primitive

  @spec type(nil | <<_::24, _::_*8>>) :: :boolean | :bytes | :double | :float | :integer | :long | nil | :string
  def type("null"), do: nil
  def type(nil), do: nil
  def type("boolean"), do: :boolean
  def type("int"), do: :integer
  def type("long"), do: :long
  def type("float"), do: :float
  def type("double"), do: :double
  def type("bytes"), do: :bytes
  def type("string"), do: :string

  defimpl String.Chars do
    def to_string(%{metadata: %{"logicalType" => type}}) do
      type
    end

    def to_string(%{type: nil}), do: "null"
    def to_string(%{type: type}), do: Atom.to_string(type)
  end
end
