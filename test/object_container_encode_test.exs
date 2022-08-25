defmodule AvroEx.ObjectContainer.Encode.Test do
  use ExUnit.Case, async: true

  alias AvroEx.ObjectContainer
  alias AvroEx.ObjectContainer.Codec

  describe "encode file header" do
    test "new containers have different sync bytes" do
      containers =
        for _ <- 1..10 do
          ObjectContainer.new(nil)
        end

      for container <- containers do
        others = containers |> List.delete(container)

        for other <- others do
          refute container.sync == other.sync
        end
      end
    end

    # TODO: use multiple schemas instead of just "null"
    test "codec embedded in header" do
      codecs = [Codec.Null, Codec.Deflate, Codec.Snappy]

      containers =
        for codec <- codecs do
          ObjectContainer.new(AvroEx.decode_schema!(~S("null")), codec: codec)
        end

      headers =
        for container <- containers do
          headerdata = ObjectContainer.encode_file_header!(container)
          AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
        end

      for {header, codec} <- Enum.zip(headers, codecs) do
        assert header["meta"]["avro.codec"] == to_string(codec.name())
      end
    end

    test "default codec is null" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
      assert header["meta"]["avro.codec"] == "null"
    end

    test "schema is stored in the file header metadata" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "user metadata is stored in the file header metadata" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
      assert header["meta"]["first_time"] == "12345678"
    end

    test "user metadata does not prevent schema and codec from being written preoperly" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
      assert header["meta"]["avro.codec"] == "null"
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "magic matches standard" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema, headerdata)
      assert header["magic"] == <<"Obj", 1>>
    end
  end

  test "encode block header" do
    # TODO: property based test makes more sense
    encoded_header = ObjectContainer.encode_block_header!(100, 5000)
    header = AvroEx.decode!(ObjectContainer.block_header_schema, encoded_header)
    assert header["num_objects"] == 100
    assert header["num_bytes"] == 5000
  end

  describe "encode block objects" do
  end

  describe "encode file" do
  end
end
