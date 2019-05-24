defmodule AvroEx.Schema.Macros do
  @moduledoc false

  defmacro cast_schema(data_fields: fields) do
    quote do
      @spec cast(map()) :: {:error, any()} | {:ok, map()}
      def cast(data) do
        AvroEx.Schema.cast_schema(__MODULE__, data, unquote(fields))
      end
    end
  end
end
