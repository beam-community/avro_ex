defmodule AvroEx.ObjectContainer.Codec do
  @callback encode!(binary()) :: binary()
  @callback decode!(binary()) :: binary()
  @callback name() :: atom()

  def mandatory_codecs do
    [null: __MODULE__.Null, deflate: __MODULE__.Deflate]
  end

  def get_codec_implementation(codec, user_codecs \\ [])
  def get_codec_implementation(codec, user_codecs) when is_binary(codec),
    do: get_codec_implementation(String.to_atom(codec), user_codecs)

  def get_codec_implementation(codec, user_codecs) when is_atom(codec) do
    impl =
      mandatory_codecs()
      |> Keyword.merge(user_codecs)
      |> Keyword.get(codec)

    if impl do
      {:ok, impl}
    else
      {:error, %AvroEx.DecodeError{message: "Codec implimentation not found"}}
    end
  end
end
