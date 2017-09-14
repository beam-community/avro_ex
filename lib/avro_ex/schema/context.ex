defmodule AvroEx.Schema.Context do
  alias AvroEx.Schema.{Array, Fixed, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Record.Field

  defstruct [names: %{}]

  @type t :: %__MODULE__{
    names: %{Record.full_name => Record.t}
  }

  def add_schema(%__MODULE__{} = context, %Primitive{}), do: context
  def add_schema(%__MODULE__{} = context, %AvroEx.Schema.Map{values: values}), do: add_schema(context, values)
  def add_schema(%__MODULE__{} = context, %Array{items: items}), do: add_schema(context, items)
  def add_schema(%__MODULE__{} = context, %Union{possibilities: possibilities}) do
    Enum.reduce(possibilities, context, fn(schema, %__MODULE__{} = context) ->
      add_schema(context, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, %Fixed{} = record) do
    record
    |> Fixed.full_names
    |> Enum.reduce(context, fn(name, %__MODULE__{} = context) ->
         names = Map.put_new(context.names, name, record)
         %__MODULE__{context | names: names}
       end)
  end

  def add_schema(%__MODULE__{} = context, %Record{} = record) do
    context =
      record
      |> Record.full_names
      |> Enum.reduce(context, fn(name, %__MODULE__{} = context) ->
           names = Map.put_new(context.names, name, record)
           %__MODULE__{context | names: names}
         end)

    Enum.reduce(record.fields, context, fn
      (%Field{type: type}, %__MODULE__{} = context) ->
        add_schema(context, type)
    end)
  end

  def add_schema(%__MODULE__{} = context, %AvroEnum{} = enum) do
    enum
    |> AvroEnum.full_names
    |> Enum.reduce(context, fn(name, %__MODULE__{} = context) ->
         names = Map.put_new(context.names, name, enum)
         %__MODULE__{context | names: names}
       end)
  end

  @spec lookup(t, String.t) :: nil | Record.t
  def lookup(%__MODULE__{} = context, name) do
    context[name]
  end
end
