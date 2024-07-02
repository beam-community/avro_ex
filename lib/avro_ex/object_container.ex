defmodule AvroEx.ObjectContainer do
  use TypedStruct

  alias AvroEx.{Schema}

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

  def block_header_schema(), do: @bh_schema
  def file_header_schema(), do: @fh_schema

  def new(schema, opts \\ []) do
    %__MODULE__{
      schema: schema,
      codec: Keyword.get(opts, :codec, AvroEx.ObjectContainer.Codec.Null),
      meta: Keyword.get(opts, :meta, %{}),
      sync: :rand.bytes(16)
    }
  end

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

  @spec encode_block_header!(pos_integer(), pos_integer()) :: binary()
  def encode_block_header!(num_objects, encoded_data_size) do
    header = %{"num_objects" => num_objects, "num_bytes" => encoded_data_size}
    AvroEx.encode!(@bh_schema, header)
  end

  def encode_block_footer!(%__MODULE__{sync: sync}), do: sync

  def encode_block_objects!(%__MODULE__{} = ocf, objects) do
    for obj <- objects, reduce: <<>> do
      acc -> acc <> AvroEx.encode!(ocf.schema, obj)
    end
    |> ocf.codec.encode!()
  end

  def encode_block!(%__MODULE__{} = ocf, objects) do
    data = encode_block_objects!(ocf, objects)
    encode_block_header!(length(objects), byte_size(data)) <> data <> encode_block_footer!(ocf)
  end

  def encode_file!(%__MODULE__{} = ocf, objects) do
    encode_file_header!(ocf) <> encode_block!(ocf, objects)
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

  defp get_schema(%{"avro.schema" => schema}), do: {:ok, schema}
  defp get_schema(_), do: {:error, %AvroEx.DecodeError{message: "Invalid or missing schema in file header"}}
  defp get_codec(%{"avro.codec" => codec}), do: {:ok, codec}
  defp get_codec(_), do: {:ok, :null}

  @spec decode_file_header(binary(), keyword()) ::
          {:ok, AvroEx.ObjectContainer.t(), binary()} | {:error, AvroEx.DecodeError.t()}
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

  defp check_block_header(%{"num_objects" => num_objects, "num_bytes" => num_bytes})
       when is_integer(num_objects) and num_objects >= 0 and is_integer(num_bytes) and num_bytes >= 0,
       do: {:ok, %{num_objects: num_objects, num_bytes: num_bytes}}

  defp check_block_header(%{num_objects: num_objects, num_bytes: num_bytes})
       when is_integer(num_objects) and num_objects >= 0 and is_integer(num_bytes) and num_bytes >= 0,
       do: {:ok, %{num_objects: num_objects, num_bytes: num_bytes}}

  defp check_block_header(_), do: {:error, %AvroEx.DecodeError{message: "Invalid block header"}}

  def decode_block_header(data) do
    with {:ok, header, rest} <- decode_with_rest(@bh_schema, data),
         {:ok, checked_header} <- check_block_header(header) do
      {:ok, checked_header, rest}
    end
  end

  def check_block_footer(%__MODULE__{sync: sync}, <<read_sync::128, rest::binary>>) when sync == <<read_sync::128>>,
    do: {:ok, rest}

  def check_block_footer(%__MODULE__{sync: sync}, <<read_sync::128, _::binary>>),
    do: {:error, %AvroEx.DecodeError{message: "Invalid sync bytes: #{inspect(sync)} != #{inspect(read_sync)}"}}

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

  def decode_block_objects(file_header, block_header, data) do
    with {:ok, %{num_objects: num_objects, num_bytes: num_bytes}} <- check_block_header(block_header),
         {:ok, object_data, rest} <- get_object_data(num_bytes, data),
         {:ok, objects} <- do_decode_block_objects(file_header, object_data),
         :ok <- check_num_objects(objects, num_objects) do
      {:ok, objects, rest}
    end
  end

  def decode_block(file_header, data) do
    with {:ok, block_header, rest} <- decode_block_header(data),
         {:ok, objects, rest} <- decode_block_objects(file_header, block_header, rest),
         {:ok, rest} <- check_block_footer(file_header, rest) do
      {:ok, objects, rest}
    end
  end

  defp do_decode_blocks(file_header, data, objects \\ [])
  defp do_decode_blocks(_file_header, <<>>, objects), do: {:ok, objects |> Enum.reverse() |> List.flatten()}

  defp do_decode_blocks(file_header, data, objects) do
    with {:ok, new_objects, rest} <- decode_block(file_header, data) do
      do_decode_blocks(file_header, rest, [new_objects | objects])
    end
  end

  def decode_blocks(file_header, data) do
    do_decode_blocks(file_header, data)
  end

  def decode_file(data, opts \\ []) do
    with {:ok, %__MODULE__{} = fileheader, rest} <- decode_file_header(data, opts),
         {:ok, objects} <- decode_blocks(fileheader, rest) do
      {:ok, fileheader, objects}
    end
  end
end
