defmodule AvroEx.ObjectContainer.Codec.Null do
  @behaviour AvroEx.ObjectContainer.Codec
  @impl AvroEx.ObjectContainer.Codec
  def encode!(data), do: data
  @impl AvroEx.ObjectContainer.Codec
  def decode!(data), do: data
end
