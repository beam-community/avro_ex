defmodule AvroEx.Schema.DecodeError do
  defexception [:message]

  def new(values) do
    message = "Failed to decode schema for #{inspect(values[:reason])} data=#{inspect(values[:data])}"
    %__MODULE__{message: message}
  end
end
