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
          AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
        end

      for {header, codec} <- Enum.zip(headers, codecs) do
        assert header["meta"]["avro.codec"] == to_string(codec.name())
      end
    end

    test "default codec is null" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
      assert header["meta"]["avro.codec"] == "null"
    end

    test "schema is stored in the file header metadata" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "user metadata is stored in the file header metadata" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
      assert header["meta"]["first_time"] == "12345678"
    end

    test "user metadata does not prevent schema and codec from being written preoperly" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
      assert header["meta"]["avro.codec"] == "null"
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "magic matches standard" do
      container = ObjectContainer.new(AvroEx.decode_schema!(~S("null")))
      headerdata = ObjectContainer.encode_file_header!(container)
      header = AvroEx.decode!(ObjectContainer.file_header_schema(), headerdata)
      assert header["magic"] == <<"Obj", 1>>
    end
  end

  describe "block header" do
    test "encode and then decode block header" do
      # TODO: property based test makes more sense
      encoded_header = ObjectContainer.encode_block_header!(100, 5000)
      header = AvroEx.decode!(ObjectContainer.block_header_schema(), encoded_header)
      assert header["num_objects"] == 100
      assert header["num_bytes"] == 5000
    end
  end

  describe "decode file header" do
    test "full valid file header with optional metas" do
      {:ok, header, <<>>} =
        ObjectContainer.decode_file_header(
          AvroEx.encode!(ObjectContainer.file_header_schema(), %{
            "magic" => <<"Obj", 1>>,
            "meta" => %{
              "avro.schema" => "{\"type\":\"null\"}",
              "avro.codec" => "null",
              "custom_meta" => "custom_value"
            },
            "sync" => <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
          })
        )

      assert header.schema == AvroEx.decode_schema!(nil)
      assert header.codec == ObjectContainer.Codec.Null
      assert header.meta == %{"custom_meta" => "custom_value"}
      assert header.sync == <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
    end

    test "invalid magic is detected" do
      assert {:error, %AvroEx.DecodeError{}} =
               ObjectContainer.decode_file_header("some random data stream that doesn't start with right magic")
    end

    test "missing schema detected" do
      assert {:error, %AvroEx.DecodeError{}} =
               ObjectContainer.decode_file_header(
                 AvroEx.encode!(ObjectContainer.file_header_schema(), %{
                   "magic" => <<"Obj", 1>>,
                   "meta" => %{"avro.codec" => "null"},
                   "sync" => <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
                 })
               )
    end

    test "missing codec defaults to null" do
      assert {:ok, header, <<>>} =
               ObjectContainer.decode_file_header(
                 AvroEx.encode!(ObjectContainer.file_header_schema(), %{
                   "magic" => <<"Obj", 1>>,
                   "meta" => %{"avro.schema" => "{\"type\":\"null\"}"},
                   "sync" => <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
                 })
               )

      assert header.codec == ObjectContainer.Codec.Null
    end

    test "missing sync detected" do
      data =
        AvroEx.encode!(ObjectContainer.file_header_schema(), %{
          "magic" => <<"Obj", 1>>,
          "meta" => %{
            "avro.schema" => "{\"type\":\"null\"}",
            "avro.codec" => "null",
            "custom_meta" => "custom_value"
          },
          "sync" => <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
        })

      slice = byte_size(data) - Enum.random(1..16)
      <<corrupt_data::binary-size(slice), _::binary>> = data
      assert {:error, _} = ObjectContainer.decode_file_header(corrupt_data)
    end
  end

  describe "encode and decode block objects" do
    setup testinfo do
      data_scema =
        AvroEx.decode_schema!(%{
          "type" => "record",
          "name" => "block_test_data",
          "fields" => [%{"name" => "testdata1", "type" => "bytes"}, %{"name" => "testdata2", "type" => "int"}]
        })
      ocf = ObjectContainer.new(data_scema)
      testinfo |> Map.merge(%{data_scema: data_scema, ocf: ocf})
    end

    test "encode and then decode objects", %{ocf: ocf} do
      data_input = for v <- 1..10, do: %{"testdata1" => "test#{v}", "testdata2" => v}
      encoded = ObjectContainer.encode_block_objects!(ocf, data_input)
      block_header = %{num_objects: 10, num_bytes: byte_size(encoded)}
      {:ok, data_output, _rest} = ObjectContainer.decode_block_objects(ocf, block_header, encoded)
      assert data_output == data_input
    end
  end

  describe "full object container" do
    setup testinfo do
      data_scema =
        AvroEx.decode_schema!(%{
          "type" => "record",
          "name" => "block_test_data",
          "fields" => [%{"name" => "testdata1", "type" => "bytes"}, %{"name" => "testdata2", "type" => "int"}]
        })
      ocf = ObjectContainer.new(data_scema)
      testinfo |> Map.merge(%{data_scema: data_scema, ocf: ocf})
    end

    test "encode and then decode a file with a single block", %{data_scema: data_scema, ocf: ocf} do
      data_input = for v <- 1..10, do: %{"testdata1" => "test#{v}", "testdata2" => v}
      file_data = ObjectContainer.encode_file!(ocf, data_input)
      {:ok, ocf_output, data_output} = ObjectContainer.decode_file(file_data)
      assert AvroEx.encode_schema(ocf_output.schema) == AvroEx.encode_schema(data_scema)
      assert data_output == data_input
    end

    test "encode and then decode a file with a multiple blocks", %{data_scema: data_scema, ocf: ocf} do
      data_input = for v <- 1..30, do: %{"testdata1" => "test#{v}", "testdata2" => v}
      data_chunks = Enum.chunk_every(data_input, 10)
      file_data = ObjectContainer.encode_file!(ocf, hd(data_chunks))
      file_data = for block <- tl(data_chunks), reduce: file_data do
        acc -> acc <> ObjectContainer.encode_block!(ocf, block)
      end
      {:ok, ocf_output, data_output} = ObjectContainer.decode_file(file_data)
      assert AvroEx.encode_schema(ocf_output.schema) == AvroEx.encode_schema(data_scema)
      assert data_output == data_input
    end
  end
end
