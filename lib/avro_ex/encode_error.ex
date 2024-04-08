defmodule AvroEx.EncodeError do
  @moduledoc """
  Exceptions in encoding Avro data
  """
  defexception [:message]

  @type t :: %__MODULE__{}

  @spec new(tuple()) :: t()
  def new({:schema_mismatch, schema, value, _context}) do
    type = AvroEx.Schema.type_name(schema)

    %__MODULE__{message: "Schema Mismatch: Expected value of #{type}, got #{inspect(value)}"}
  end

  def new({:invalid_string, str, _context}) do
    %__MODULE__{message: "Invalid string \"#{inspect(str)}\""}
  end

  def new({:invalid_symbol, enum, value, _context}) do
    type = AvroEx.Schema.type_name(enum)

    %__MODULE__{
      message: "Invalid symbol for #{type}. Expected value in #{inspect(enum.symbols)}, got #{inspect(value)}"
    }
  end

  def new({:incorrect_fixed_size, fixed, binary, _context}) do
    type = AvroEx.Schema.type_name(fixed)

    %__MODULE__{
      message: "Invalid size for #{type}. Size of #{byte_size(binary)} for #{inspect(binary)}"
    }
  end

  def new({:incompatible_decimal, expected_scale, actual_scale}) do
    %__MODULE__{
      message: "Incompatible decimal: expected scale #{expected_scale}, got #{actual_scale}"
    }
  end
end
