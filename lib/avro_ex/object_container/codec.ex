defmodule AvroEx.ObjectContainer.Codec do
  @moduledoc """
  Behaviour for Avro Object Container compression codecs.

  This module defines the contract that compression codecs must implement
  in order to be used by the Avro object container file encoder/decoder.

  ## Built-in Codecs

  The following codecs are mandatory according to the Avro specification
  and are provided out of the box:

    * `:null` — No compression
    * `:deflate` — DEFLATE compression

  These can be retrieved via `mandatory_codecs/0`.

  ## Custom Codecs

  Users may provide additional codecs by passing a keyword list of
  `{codec_name, module}` pairs to the `codecs` option in encode/decode
  file functions.

  The provided module **must implement this behaviour**.

  Example:

      defmodule MyApp.SnappyCodec do
        @behaviour AvroEx.ObjectContainer.Codec

        @impl AvroEx.ObjectContainer.Codec
        def name, do: :snappy

        @impl AvroEx.ObjectContainer.Codec
        def encode!(data) do
          # compression logic
        end

        @impl AvroEx.ObjectContainer.Codec
        def decode!(data) do
          # decompression logic
        end
      end

  The snappy codec bundled in AvroEx can be used as is if snappyer
  is included in your dependencies.
  """

  @type codec_name :: atom()
  @type codec_impl :: module()
  @type codec_error :: {:error, AvroEx.DecodeError.t()}

  @doc """
  Encodes (compresses) the given binary.

  Implementations must return a compressed binary.
  Raises on failure.
  """
  @callback encode!(binary()) :: binary()

  @doc """
  Decodes (decompresses) the given binary.

  Implementations must return the original uncompressed binary.
  Raises on failure.
  """
  @callback decode!(binary()) :: binary()

  @doc """
  Returns the codec's registered name.

  This name is used when resolving codec implementations.
  """
  @callback name() :: codec_name()

  @doc """
  Returns mandatory Avro codecs.

  These codecs are always available and compliant with the Avro
  object container specification.

      iex> AvroEx.ObjectContainer.Codec.mandatory_codecs()
      [null: AvroEx.ObjectContainer.Codec.Null,
       deflate: AvroEx.ObjectContainer.Codec.Deflate]
  """
  @spec mandatory_codecs() :: Keyword.t(codec_impl())
  def mandatory_codecs do
    [null: __MODULE__.Null, deflate: __MODULE__.Deflate]
  end

  @doc """
  Retrieves the implementation for a given codec.

  The codec may be provided as an atom or a string.
  User-provided codecs may override or extend the mandatory codecs.

  ## Parameters

    * `codec` — The codec name (`atom` or `binary`)
    * `user_codecs` — Optional keyword list of custom codecs

  ## Returns

    * `{:ok, module}` if the codec implementation is found
    * `{:error, AvroEx.DecodeError.t()}` if not found

  ## Examples

      iex> AvroEx.ObjectContainer.Codec.get_codec_implementation(:null)
      {:ok, AvroEx.ObjectContainer.Codec.Null}

      iex> AvroEx.ObjectContainer.Codec.get_codec_implementation(:unknown)
      {:error, %AvroEx.DecodeError{}}
  """
  @spec get_codec_implementation(codec_name() | binary(), Keyword.t(codec_impl())) ::
          {:ok, codec_impl()} | codec_error()
  def get_codec_implementation(codec, user_codecs \\ [])

  def get_codec_implementation(codec, user_codecs) when is_binary(codec) do
    case safe_to_existing_atom(codec) do
      {:ok, atom} -> get_codec_implementation(atom, user_codecs)
      :error -> codec_not_found_error()
    end
  end

  def get_codec_implementation(codec, user_codecs) when is_atom(codec) do
    codecs =
      mandatory_codecs()
      |> Keyword.merge(user_codecs)

    case Keyword.fetch(codecs, codec) do
      {:ok, impl} -> {:ok, impl}
      :error -> codec_not_found_error()
    end
  end

  # Used to prevent atom exhaustion when user supplied data is bad
  defp safe_to_existing_atom(binary) do
    try do
      {:ok, String.to_existing_atom(binary)}
    rescue
      ArgumentError -> :error
    end
  end

  defp codec_not_found_error do
    {:error, %AvroEx.DecodeError{message: "Codec implementation not found"}}
  end
end
