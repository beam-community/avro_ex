defmodule AvroEx.Schema.Context do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Array, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  defstruct [names: %{}]

  @type t :: %__MODULE__{
    names: %{Schema.full_name => Record.t}
  }

  def add_schema(%__MODULE__{} = context, %Primitive{}), do: context
  def add_schema(%__MODULE__{} = context, %AvroEx.Schema.Map{values: values}), do: add_schema(context, values)
  def add_schema(%__MODULE__{} = context, %Array{items: items}), do: add_schema(context, items)
  def add_schema(%__MODULE__{} = context, %Union{possibilities: possibilities}) do
    Enum.reduce(possibilities, context, fn(schema, %__MODULE__{} = context) ->
      add_schema(context, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, %Fixed{} = schema) do
    Enum.reduce(schema.qualified_names, context, fn(name, %__MODULE__{} = context) ->
      add_name(context, name, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, %Record{} = schema) do
    context =
      Enum.reduce(schema.qualified_names, context, fn(name, %__MODULE__{} = context) ->
        add_name(context, name, schema)
      end)

    Enum.reduce(schema.fields, context, fn
      (%Field{type: type}, %__MODULE__{} = context) ->
        add_schema(context, type)
    end)
  end

  def add_schema(%__MODULE__{} = context, %AvroEnum{} = schema) do
    Enum.reduce(schema.qualified_names, context, fn(name, %__MODULE__{} = context) ->
      add_name(context, name, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, name) when is_binary(name) do
    context
  end

  def add_name(%__MODULE__{} = context, name, value) when is_binary(name) do
    %__MODULE__{names: Map.put_new(context.names, name, value)}
  end

  @spec lookup(t, String.t) :: nil | Schema.schema_types
  def lookup(%__MODULE__{} = context, name) do
    context.names[name]
  end
end
