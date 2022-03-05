defmodule AvroEx.Schema.Record do
  use TypedStruct

  alias AvroEx.{Schema, Schema.Context, Schema.Record}

  typedstruct do
    field :aliases, [Schema.alias()], default: []
    field :doc, String.t()
    field :name, String.t(), enforce: true
    field :namespace, String.t()
    field :metadata, Schema.metadata(), default: %{}
    field :fields, [Record.Field.t()], default: []
  end

  @spec match?(t, Context.t(), term) :: boolean
  def match?(%__MODULE__{fields: fields}, %Context{} = context, data)
      when is_map(data) and map_size(data) == length(fields) do
    Enum.all?(fields, fn %Record.Field{name: name} = field ->
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
