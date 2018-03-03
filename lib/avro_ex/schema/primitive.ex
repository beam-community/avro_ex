defmodule AvroEx.Schema.Primitive do
  use Ecto.Schema
  @behaviour Ecto.Type
  @primary_key false

  @type primitive :: nil
  | :boolean
  | :integer
  | :long
  | :float
  | :double
  | :bytes
  | :string

  embedded_schema do
    field :metadata, :map, default: %{}
    field :type, :string # Actually a primitive - placeholder until I create a custom ecto type
  end

  @type t :: %__MODULE__{
    metadata: %{String.t => String.t},
    type: primitive,
  }

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

  def load(data), do: {:ok, data}

  def dump(data), do: {:ok, data}

  def type(), do: :primitive

  def type("null"), do: nil
  def type(nil), do: nil
  def type("boolean"), do: :boolean
  def type("int"), do: :integer
  def type("long"), do: :long
  def type("float"), do: :float
  def type("double"), do: :double
  def type("bytes"), do: :bytes
  def type("string"), do: :string
end
