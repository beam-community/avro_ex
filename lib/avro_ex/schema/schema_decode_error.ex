defmodule AvroEx.Schema.DecodeError do
  defexception [:message]

  def new(values) do
    message = "Failed to decode schema reason=#{inspect(values[:reason])}"
    %__MODULE__{message: message}
  end
end
