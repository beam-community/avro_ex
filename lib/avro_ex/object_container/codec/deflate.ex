defmodule AvroEx.ObjectContainer.Codec.Deflate do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def encode!(data), do: :zlib.zip(data)
  @impl AvroEx.ObjectContainer.Codec
  def decode!(data), do: :zlib.unzip(data)
end
