defmodule AvroEx.ObjectContainer.Codec.Snappy do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def name(), do: :snappy
  @impl AvroEx.ObjectContainer.Codec
  def encode!(data) do
    {:ok, compressed} = :snappyer.compress(data)
    <<compressed, :erlang.crc32(data)::32>>
  end

  @impl AvroEx.ObjectContainer.Codec
  def decode!(data) do
    len = byte_size(data) - 4
    <<compressed::binary-size(len), crc::32>> = data
    {:ok, decompressed} = :snappyer.decompress(compressed)

    if crc == :erlang.crc32(decompressed) do
      decompressed
    else
      raise %AvroEx.DecodeError{message: "CRC mismatch during decompression"}
    end
  end
end
