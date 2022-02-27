defmodule AvroEx.Schema.DecodeError do
  defexception [:message]

  def new({:unrecognized_fields, keys, type, data}) do
    qualifier =
      case keys do
        [_] -> "key"
        _ -> "keys"
      end

    message =
      "Unrecognized schema #{qualifier} #{Enum.map_join(keys, ", ", &surround(&1))} for #{inspect(type)} in #{inspect(data)}"

    %__MODULE__{message: message}
  end

  def new({:missing_required, key, data}) do
    message = "Schema missing required key #{surround(key)} in #{inspect(data)}"
    %__MODULE__{message: message}
  end

  def new({:nested_union, nested, union}) do
    message = "Union contains nested union #{inspect(nested)} as immediate child in #{inspect(union)}"
    %__MODULE__{message: message}
  end

  def new({:duplicate_union_type, type, union}) do
    message = "Union conains duplicated #{inspect(type)} in #{inspect(union)}"
    %__MODULE__{message: message}
  end

  def new({:duplicate_symbol, symbol, enum}) do
    message = "Enum conains duplicated symbol #{surround(symbol)} in #{inspect(enum)}"
    %__MODULE__{message: message}
  end

  def new({:invalid_name, {field, name}, context}) do
    message = "Invalid name #{surround(name)} for #{surround(field)} in #{inspect(context)}"
    %__MODULE__{message: message}
  end

  def new({:invalid_format, data}) do
    message = "Invalid schema format #{inspect(data)}"
    %__MODULE__{message: message}
  end

  defp surround(string, value \\ "`") do
    value <> to_string(string) <> value
  end
end
