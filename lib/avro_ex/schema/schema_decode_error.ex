defmodule AvroEx.Schema.DecodeError do
  defexception [:message]

  def new({:unrecognized_fields, keys, type, data}) do
    qualifier =
      case keys do
        [_] -> "key"
        _ -> "keys"
      end

    message =
      "Unrecognized schema #{qualifier} #{Enum.map_join(keys, ", ", &surround(&1, "`"))} for #{inspect(type)} in #{inspect(data)}"

    %__MODULE__{message: message}
  end

  def new({:missing_required, key, data}) do
    message = "Schema missing required key #{surround(to_string(key), "`")} in #{inspect(data)}"
    %__MODULE__{message: message}
  end

  def new({:invalid_format, data}) do
    message = "Invalid schema format #{inspect(data)}"
    %__MODULE__{message: message}
  end

  defp surround(string, value) do
    value <> string <> value
  end
end
