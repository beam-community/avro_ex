defmodule AvroEx.Schema.Primitive do
  @moduledoc """
  Functions for handling primitive types in Avro schemas
  """

  use TypedStruct

  alias AvroEx.{Schema}

  @type primitive ::
          :null
          | :boolean
          | :int
          | :long
          | :float
          | :double
          | :bytes
          | :string

  typedstruct do
    field :metadata, Schema.metadata(), default: %{}
    field :type, primitive(), enforce: true
  end
end
