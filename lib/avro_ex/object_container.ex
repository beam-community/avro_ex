defmodule AvroEx.ObjectContainer do
  @moduledoc """
  Implementation of the Avro Object Container File (OCF) format.

  This module is responsible for:

    * Building Avro object container files
    * Encoding file headers and data blocks
    * Decoding existing container files
    * Managing compression codecs
    * Validating sync markers

  ## Overview

  An Avro Object Container File consists of:

    1. A **file header**
       * Magic bytes (`"Obj" <> <<1>>`)
       * Metadata (including schema and codec)
       * 16-byte sync marker

    2. One or more **data blocks**
       * Block header (object count + byte size)
       * Compressed object data
       * Sync marker

  ## Creating a File

      schema = AvroEx.decode_schema!(schema_map)

      ocf =
        AvroEx.ObjectContainer.new(schema,
          codec: AvroEx.ObjectContainer.Codec.Deflate
        )

      binary = AvroEx.ObjectContainer.encode_file!(ocf, objects)

  ## Decoding a File

      {:ok, header, objects} =
        AvroEx.ObjectContainer.decode_file(binary)

  Custom codecs can be provided via the `:codecs` option when decoding.
  """

  use TypedStruct

  alias AvroEx.Schema
  alias AvroEx.ObjectContainer.Codec

  @type codec_types :: :null | :deflate | :bzip2 | :snappy | :xz | :zstandard

  typedstruct do
    field :schema, Schema.t()
    field :codec, AvroEx.ObjectContainer.Codec, default: AvroEx.ObjectContainer.Codec.Null
    field :meta, map(), default: %{}
    field :sync, <<_::128>>
  end

  @magic <<"Obj", 1>>
  @bh_schema AvroEx.decode_schema!(%{
               "type" => "record",
               "name" => "block_header",
               "fields" => [
                 %{"name" => "num_objects", "type" => "long"},
                 %{"name" => "num_bytes", "type" => "long"}
               ]
             })

  @fh_schema AvroEx.decode_schema!(%{
               "type" => "record",
               "name" => "org.apache.avro.file.Header",
               "fields" => [
                 %{"name" => "magic", "type" => %{"type" => "fixed", "name" => "Magic", "size" => 4}},
                 %{"name" => "meta", "type" => %{"type" => "map", "values" => "bytes"}},
                 %{"name" => "sync", "type" => %{"type" => "fixed", "name" => "Sync", "size" => 16}}
               ]
             })

  @doc "Avro schema used for block headers."
  @spec block_header_schema() :: Schema.t()
  def block_header_schema, do: @bh_schema

  @doc "Avro schema used for file headers."
  @spec file_header_schema() :: Schema.t()
  def file_header_schema, do: @fh_schema

  @doc """
  Creates a new object container struct.

  ## Options

    * `:codec` – Compression codec module (defaults to `Codec.Null`)
    * `:meta`  – Additional metadata map
  """
  def new(schema, opts \\ []) do
    %__MODULE__{
      schema: schema,
      codec: Keyword.get(opts, :codec, AvroEx.ObjectContainer.Codec.Null),
      meta: Keyword.get(opts, :meta, %{}),
      sync: :rand.bytes(16)
    }
  end

  @doc """
  Encodes the Avro file header.
  """
  @spec encode_file_header!(t()) :: binary()
  def encode_file_header!(%__MODULE__{} = ocf) do
    metadata =
      %{
        "avro.schema" => AvroEx.encode_schema(ocf.schema),
        "avro.codec" => to_string(ocf.codec.name())
      }
      |> Map.merge(ocf.meta)

    AvroEx.encode!(@fh_schema, %{
      magic: @magic,
      meta: metadata,
      sync: ocf.sync
    })
  end

  @doc """
  Encodes a block header.
  """
  @spec encode_block_header!(pos_integer(), pos_integer()) :: binary()
  def encode_block_header!(num_objects, encoded_data_size) do
    header = %{"num_objects" => num_objects, "num_bytes" => encoded_data_size}
    AvroEx.encode!(@bh_schema, header)
  end

  @doc """
  Sync marker for a block footer.
  """
  @spec encode_block_footer!(t()) :: binary()
  def encode_block_footer!(%__MODULE__{sync: sync}), do: sync

  @doc """
  Encodes and compresses a list of objects.
  """
  @spec encode_block_objects!(t(), list()) :: binary()
  def encode_block_objects!(%__MODULE__{} = ocf, objects) do
    for obj <- objects, reduce: <<>> do
      acc -> acc <> AvroEx.encode!(ocf.schema, obj)
    end
    |> ocf.codec.encode!()
  end

  @doc """
  Encodes a full block (header + compressed data + sync).
  """
  @spec encode_block!(t(), list()) :: binary()
  def encode_block!(%__MODULE__{} = ocf, objects) do
    data = encode_block_objects!(ocf, objects)
    encode_block_header!(length(objects), byte_size(data)) <> data <> encode_block_footer!(ocf)
  end

  @doc """
  Encodes a complete Avro Object Container file.
  """
  @spec encode_file!(t(), list()) :: binary()
  def encode_file!(%__MODULE__{} = ocf, objects) do
    encode_file_header!(ocf) <> encode_block!(ocf, objects)
  end

  # ------------ Decoding ------------ #
  # ---------------------------------- #

  @doc """
  Decodes the file header

  ## Options

    * `:codecs` – Custom codecs available for decoding.

  ## Returns

      {:ok, header_struct, remaining_binary}
  """
  @spec decode_file_header(binary(), keyword()) :: {:ok, t(), binary()} | {:error, AvroEx.DecodeError.t()}
  def decode_file_header(file_header, opts \\ []) do
    user_codecs = Keyword.get(opts, :codecs, [])

    with :ok <- check_magic(file_header),
         {:ok, decoded_header, rest} <- decode_with_rest(@fh_schema, file_header),
         {:ok, schema} <- get_schema(decoded_header["meta"]),
         {:ok, codec} <- get_codec(decoded_header["meta"]),
         {:ok, decoded_schema} <- AvroEx.decode_schema(schema),
         {:ok, codec_impl} <- __MODULE__.Codec.get_codec_implementation(codec, user_codecs) do
      meta = Map.drop(decoded_header["meta"], ["avro.schema", "avro.codec"])

      {:ok,
       %__MODULE__{
         schema: decoded_schema,
         codec: codec_impl,
         meta: meta,
         sync: decoded_header["sync"]
       }, rest}
    end
  end

  @doc """
  Decodes a block header from binary data.

  ## Returns

    * `{:ok, %{num_objects: n, num_bytes: b}, rest}`
    * `{:error, reason}`
  """
  @spec decode_block_header(binary()) ::
      {:ok, %{num_objects: non_neg_integer(), num_bytes: non_neg_integer()}, binary()}
      | {:error, AvroEx.DecodeError.t()}
  def decode_block_header(data) do
    with {:ok, header, rest} <- decode_with_rest(@bh_schema, data),
         {:ok, checked_header} <- check_block_header(header) do
      {:ok, checked_header, rest}
    end
  end

  @doc """
  Decodes and decompresses block objects.

  This function:

    1. Extracts compressed/encoded block data
    2. Decompresses/decodes block data using the codec specified in the header
    3. Decodes individual Avro objects
    4. Validates object count
  """
  @spec decode_block_objects(t(), map(), binary()) ::
        {:ok, list(), binary()} | {:error, AvroEx.DecodeError.t()}
  def decode_block_objects(file_header, block_header, data) do
    with {:ok, %{num_objects: num_objects, num_bytes: num_bytes}} <- check_block_header(block_header),
         {:ok, compressed_data, rest} <- get_object_data(num_bytes, data),
         {:ok, object_data} <- safe_decompress(file_header.codec, compressed_data),
         {:ok, objects} <- do_decode_block_objects(file_header, object_data),
         :ok <- check_num_objects(objects, num_objects) do
      {:ok, objects, rest}
    end
  end

  @doc """
  Decodes a complete block (header + objects + sync marker).
  """
  @spec decode_block(t(), binary()) ::
        {:ok, list(), binary()} | {:error, AvroEx.DecodeError.t()}
  def decode_block(file_header, data) do
    with {:ok, block_header, rest} <- decode_block_header(data),
         {:ok, objects, rest} <- decode_block_objects(file_header, block_header, rest),
         {:ok, rest} <- check_block_footer(file_header, rest) do
      {:ok, objects, rest}
    end
  end

  @doc """
  Decodes all blocks following a file header.
  """
  @spec decode_blocks(t(), binary()) ::
        {:ok, list()} | {:error, AvroEx.DecodeError.t()}
  def decode_blocks(file_header, data) do
    do_decode_blocks(file_header, data)
  end

  @doc """
  Decodes a complete Avro Object Container File.

  ## Options

    * `:codecs` – Custom codecs available for decoding.

  ## Returns

    * `{:ok, file_header_struct, objects}`
    * `{:error, reason}`
  """
  @spec decode_file(binary(), keyword()) ::
        {:ok, t(), list()} | {:error, AvroEx.DecodeError.t()}
  def decode_file(data, opts \\ []) do
    with {:ok, %__MODULE__{} = fileheader, rest} <- decode_file_header(data, opts),
         {:ok, objects} <- decode_blocks(fileheader, rest) do
      {:ok, fileheader, objects}
    end
  end

  defp check_magic(<<"Obj", 1, _::binary>>), do: :ok
  defp check_magic(_), do: {:error, %AvroEx.DecodeError{message: "Invalid file header"}}

  defp decode_with_rest(schema, message, opts \\ []) do
    try do
      AvroEx.Decode.decode(schema, message, opts)
    rescue
      e in MatchError -> {:error, e}
    end
  end

  defp safe_decompress(codec, data) do
    try do
      {:ok, codec.decode!(data)}
    rescue
      _ -> {:error, %AvroEx.DecodeError{message: "Failed to decompress block data"}}
    end
  end

  defp get_schema(%{"avro.schema" => schema}), do: {:ok, schema}
  defp get_schema(_), do: {:error, %AvroEx.DecodeError{message: "Invalid or missing schema in file header"}}
  defp get_codec(%{"avro.codec" => codec}), do: {:ok, codec}
  defp get_codec(_), do: {:ok, :null}

  defp do_decode_block_objects(file_header, data, objects \\ [])
  defp do_decode_block_objects(_file_header, <<>>, objects), do: {:ok, Enum.reverse(objects)}

  defp do_decode_block_objects(%__MODULE__{} = file_header, data, objects) do
    with {:ok, object, rest} <- decode_with_rest(file_header.schema, data) do
      do_decode_block_objects(file_header, rest, [object | objects])
    end
  end

  defp get_object_data(num_bytes, data) do
    with <<object_data::binary-size(num_bytes), rest::binary>> <- data do
      {:ok, object_data, rest}
    else
      _ -> {:error, %AvroEx.DecodeError{message: "Not enough bytes for block objects"}}
    end
  end

  defp check_num_objects(objects, num_objects) when length(objects) == num_objects, do: :ok
  defp check_num_objects(_, _), do: {:error, %AvroEx.DecodeError{message: "Invalid number of objects"}}

  defp do_decode_blocks(file_header, data, objects \\ [])
  defp do_decode_blocks(_file_header, <<>>, objects), do: {:ok, objects |> Enum.reverse() |> List.flatten()}

  defp do_decode_blocks(file_header, data, objects) do
    with {:ok, new_objects, rest} <- decode_block(file_header, data) do
      do_decode_blocks(file_header, rest, [new_objects | objects])
    end
  end

  defp check_block_header(%{"num_objects" => num_objects, "num_bytes" => num_bytes})
       when is_integer(num_objects) and num_objects >= 0 and is_integer(num_bytes) and num_bytes >= 0,
       do: {:ok, %{num_objects: num_objects, num_bytes: num_bytes}}

  defp check_block_header(%{num_objects: num_objects, num_bytes: num_bytes})
       when is_integer(num_objects) and num_objects >= 0 and is_integer(num_bytes) and num_bytes >= 0,
       do: {:ok, %{num_objects: num_objects, num_bytes: num_bytes}}

  defp check_block_header(_), do: {:error, %AvroEx.DecodeError{message: "Invalid block header"}}

  defp check_block_footer(%__MODULE__{sync: sync}, <<read_sync::128, rest::binary>>) when sync == <<read_sync::128>>,
    do: {:ok, rest}

  defp check_block_footer(%__MODULE__{sync: sync}, <<read_sync::128, _::binary>>),
    do: {:error, %AvroEx.DecodeError{message: "Invalid sync bytes: #{inspect(sync)} != #{inspect(read_sync)}"}}
end
