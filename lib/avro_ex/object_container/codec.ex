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
