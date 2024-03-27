defmodule AvroEx.ObjectContainer.Codec.Snappy do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def name(), do: :snappy

  if Code.ensure_loaded?(:snappyer) do
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
  else
    @impl AvroEx.ObjectContainer.Codec
    def encode!(_data) do
      raise """
      Cannot encode data using the Snappy codec because snappyer has not been loaded.
      If you require Snappy compression, you must add snappyer as a dependency in your mix.exs file.
      """
    end

    @impl AvroEx.ObjectContainer.Codec
    def decode!(_data) do
      raise """
      Cannot encode data using the Snappy codec because snappyer has not been loaded.
      If you require Snappy compression, you must add snappyer as a dependency in your mix.exs file.
      """
    end
  end
end
