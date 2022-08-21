defmodule AvroEx.ObjectContainer.Encode.Test do
  use ExUnit.Case, async: true

  @test_module AvroEx.ObjectContainer

  describe "encode file header" do
    test "new containers have different sync bytes" do
      containers =
        for _ <- 1..10 do
          @test_module.new(nil)
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
      codecs = [:null, :deflate, :bzip2, :snappy, :xz, :zstandard]

      containers =
        for codec <- codecs do
          @test_module.new(AvroEx.decode_schema!(~S("null")), codec: codec)
        end

      headers =
        for container <- containers do
          headerdata = @test_module.encode_file_header!(container)
          AvroEx.decode!(@test_module.file_header_schema, headerdata)
        end

      for {header, codec} <- Enum.zip(headers, codecs) do
        assert header["meta"]["avro.codec"] == to_string(codec)
      end
    end

    test "default codec is null" do
      container = @test_module.new(AvroEx.decode_schema!(~S("null")))
      headerdata = @test_module.encode_file_header!(container)
      header = AvroEx.decode!(@test_module.file_header_schema, headerdata)
      assert header["meta"]["avro.codec"] == "null"
    end

    test "schema is stored in the file header metadata" do
      container = @test_module.new(AvroEx.decode_schema!(~S("null")))
      headerdata = @test_module.encode_file_header!(container)
      header = AvroEx.decode!(@test_module.file_header_schema, headerdata)
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "user metadata is stored in the file header metadata" do
      container = @test_module.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = @test_module.encode_file_header!(container)
      header = AvroEx.decode!(@test_module.file_header_schema, headerdata)
      assert header["meta"]["first_time"] == "12345678"
    end

    test "user metadata does not prevent schema and codec from being written preoperly" do
      container = @test_module.new(AvroEx.decode_schema!(~S("null")), meta: %{first_time: "12345678"})
      headerdata = @test_module.encode_file_header!(container)
      header = AvroEx.decode!(@test_module.file_header_schema, headerdata)
      assert header["meta"]["avro.codec"] == "null"
      assert header["meta"]["avro.schema"] == "{\"type\":\"null\"}"
    end

    test "magic matches standard" do
      container = @test_module.new(AvroEx.decode_schema!(~S("null")))
      headerdata = @test_module.encode_file_header!(container)
      header = AvroEx.decode!(@test_module.file_header_schema, headerdata)
      assert header["magic"] == <<"Obj", 1>>
    end
  end

  test "encode block header" do
    # TODO: property based test makes more sense
    encoded_header = @test_module.encode_block_header!(100, 5000)
    header = AvroEx.decode!(@test_module.block_header_schema, encoded_header)
    assert header["num_objects"] == 100
    assert header["num_bytes"] == 5000
  end

  describe "encode block objects" do
  end

  describe "encode file" do
  end
end
