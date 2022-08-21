defmodule AvroEx.ObjectContainer.Codec do
  @callback encode!(binary()) :: binary()
  @callback decode!(binary()) :: binary()

  defp get_codec_config(codec, dflt), do: Application.get_env(:avro_ex, codec, dflt)
  @spec get_codec!(atom) :: __MODULE__.Null
  def get_codec!(:null), do: get_codec_config(:null, AvroEx.ObjectContainer.Codec.Null)
  def get_codec!(:deflate), do: get_codec_config(:deflate, AvroEx.ObjectContainer.Codec.Deflate)
  def get_codec!(:snappy), do: get_codec_config(:snappy, AvroEx.ObjectContainer.Codec.Snappy)
  def get_codec!(codec), do: Application.fetch_env!(:avro_ex, codec)
end

defmodule AvroEx.ObjectContainer.Codec.Null do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def encode!(data), do: data
  @impl AvroEx.ObjectContainer.Codec
  def decode!(data), do: data
end

defmodule AvroEx.ObjectContainer.Codec.Deflate do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def encode!(data), do: :zlib.zip(data)
  @impl AvroEx.ObjectContainer.Codec
  def decode!(data), do: :zlib.unzip(data)
end

defmodule AvroEx.ObjectContainer.Codec.Snappy do
  @behaviour AvroEx.ObjectContainer.Codec
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
