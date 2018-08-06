defmodule Helper do
  def to_bits(bitstring, bits \\ [])

  def to_bits(<<bit::1, rest::bitstring>>, bits) do
    to_bits(rest, [bit | bits])
  end

  def to_bits(<<>>, bits) do
    Enum.reverse(bits)
  end
end
