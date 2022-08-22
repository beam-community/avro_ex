defmodule AvroEx.ObjectContainer do
  use TypedStruct

  alias AvroEx.{Schema}

  @type codec_types :: :null | :deflate | :bzip2 | :snappy | :xz | :zstandard

  typedstruct do
    field :schema, Schema.t()
    field :codec, codec_types(), default: :null
    field :meta, map(), default: %{}
    field :sync, <<_::128>>
  end

  @magic <<"Obj", 1>>
  @bh_schema AvroEx.decode_schema!(~S({
    "type":"record","name":"block_header",
    "fields":[
      {"name":"num_objects","type":"long"},
      {"name":"num_bytes","type":"long"}
    ]
  }))
  @fh_schema AvroEx.decode_schema!(~S({
    "type": "record", "name": "org.apache.avro.file.Header",
    "fields" : [
      {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
      {"name": "meta", "type": {"type": "map", "values": "bytes"}},
      {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
    ]
  }))

  def magic(), do: @magic
  def block_header_schema(), do: @bh_schema
  def file_header_schema(), do: @fh_schema

  def new(schema, opts \\ []) do
    %__MODULE__{
      schema: schema,
      codec: Keyword.get(opts, :codec, :null),
      meta: Keyword.get(opts, :meta, %{}),
      sync: :rand.bytes(16)
    }
  end

  def encode_file_header!(%__MODULE__{} = ocf) do
    metadata =
      %{
        "avro.schema" => AvroEx.encode_schema(ocf.schema),
        "avro.codec" => to_string(ocf.codec)
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
    codec = AvroEx.ObjectContainer.Codec.get_codec!(ocf.codec)

    for obj <- objects, reduce: <<>> do
      acc -> acc <> AvroEx.encode!(ocf.schema, obj)
    end
    |> codec.encode!()
  end

  def encode_block!(%__MODULE__{} = ocf, objects) do
    data = encode_block_objects!(ocf, objects)
    encode_block_header!(length(objects), byte_size(data)) <> data <> encode_block_footer!(ocf)
  end

  def encode_file!(%__MODULE__{} = ocf, objects) do
    encode_file_header!(ocf) <> encode_block!(ocf, objects)
  end
end
