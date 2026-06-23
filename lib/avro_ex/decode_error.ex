defmodule AvroEx.DecodeError do
  @moduledoc """
  Exceptions raised during Avro data decoding.

  Each `{tag, ...}` tuple produces a descriptive error message to help
  diagnose the root cause — truncated messages, schema mismatches,
  invalid UTF‑8, out-of-range indices, etc.
  """

  defexception [:message]

  @type t :: %__MODULE__{}

  @spec new(tuple()) :: t()

  ## ── Data integrity ──────────────────────────────────────────────

  def new({:truncated_data, expected, available}) do
    %__MODULE__{
      message: "Truncated data: expected at least #{expected} byte(s) but only #{available} available"
    }
  end

  def new({:invalid_utf8, str}) do
    %__MODULE__{message: "Invalid UTF-8 string: #{inspect(str)}"}
  end

  ## ── Index out of range ──────────────────────────────────────────

  def new({:union_index_out_of_range, index, count}) do
    %__MODULE__{
      message: "Union index #{index} out of range (#{count} possibilities)"
    }
  end

  def new({:symbol_index_out_of_range, index, symbols}) do
    %__MODULE__{
      message: "Enum symbol index #{index} out of range (#{length(symbols)} symbols: #{inspect(symbols)})"
    }
  end

  def new({:fixed_size_mismatch, expected, actual}) do
    %__MODULE__{
      message: "Fixed size mismatch: expected #{expected} bytes, got #{actual}"
    }
  end

  ## ── Logical type ────────────────────────────────────────────────

  def new({:invalid_logical_type, logical_type, base_type}) do
    %__MODULE__{
      message: "Invalid logical type #{inspect(logical_type)} on base type #{inspect(base_type)}"
    }
  end

  ## ── Legacy / fallback ───────────────────────────────────────────

  def new({:invalid_string, str}) do
    %__MODULE__{message: "Invalid UTF-8 string found: #{inspect(str)}."}
  end

  def new({:unknown, reason}) do
    %__MODULE__{message: "Decode error: #{Exception.message(reason)}"}
  end

  def new(other) do
    %__MODULE__{message: "Decode error: #{inspect(other)}"}
  end
end
