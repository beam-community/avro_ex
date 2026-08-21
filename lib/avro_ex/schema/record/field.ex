defmodule AvroEx.Schema.Record.Field do
  use TypedStruct

  alias AvroEx.Schema
  alias AvroEx.Schema.Context

  # A field's `default` is a legitimate Avro value, and Avro's null literal
  # decodes to Elixir's `nil` -- so a field with no default at all is
  # indistinguishable from one whose default is explicitly `null` if `nil`
  # is also used as the "absent" marker. This sentinel breaks that tie: it
  # is the struct's actual default value for `:default`, so `nil` stays free
  # to mean "the default is the Avro null literal".
  @typedoc false
  @type no_default :: :__avro_ex_no_default__

  @no_default :__avro_ex_no_default__

  typedstruct do
    field(:name, String.t(), enforce: true)
    field(:doc, String.t())
    field(:type, Schema.schema_types(), enforce: true)
    field(:default, Schema.schema_types() | no_default(), default: @no_default)
    field(:aliases, [Schema.alias()], default: [])
    field(:metadata, Schema.metadata(), default: %{})
  end

  @doc false
  @spec no_default() :: no_default()
  def no_default, do: @no_default

  @doc """
  Returns `true` if `field` declares a default value (including an explicit
  `null` default), `false` if it declares none.
  """
  @spec has_default?(t()) :: boolean()
  def has_default?(%__MODULE__{default: @no_default}), do: false
  def has_default?(%__MODULE__{}), do: true

  @spec match?(AvroEx.Schema.Record.Field.t(), AvroEx.Schema.Context.t(), any()) :: boolean()
  def match?(%__MODULE__{type: type}, %Context{} = context, data) do
    Schema.encodable?(type, context, data)
  end
end
