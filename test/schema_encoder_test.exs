defmodule AvroEx.Schema.EncoderTest do
  use ExUnit.Case, async: true

  describe "encode/2" do
    test "primitive" do
      input = "int"

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"type":"int"})
    end

    test "logical types" do
      input = %{"type" => "int", "logicalType" => "date"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"type":"int","logicalType":"date"})
    end

    test "enum" do
      input = %{"type" => "enum", "symbols" => ["a"], "name" => "cool"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"name":"cool","symbols":["a"],"type":"enum"})

      # TODO all fields
    end

    test "map" do
      # primitive map
      input = %{"type" => "map", "values" => "int"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"type":"map","values":{"type":"int"}})

      # complex map
      input = %{"type" => "map", "values" => ["null", "int"]}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"type":"map","values":[{"type":"null"},{"type":"int"}]})

      # TODO all fields
    end

    test "record" do
      # primitive record
      input = %{"type" => "record", "name" => "test", "fields" => [%{"name" => "a", "type" => "string"}]}

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"string\"}}],\"name\":\"test\",\"type\":\"record\"}"

      # Complex record
      input = %{"type" => "record", "name" => "test", "fields" => [%{"name" => "a", "type" => ["int", "string"]}]}

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\"},{\"type\":\"string\"}]}],\"name\":\"test\",\"type\":\"record\"}"

      # TODO all fields
    end

    test "array" do
      # primitive array
      input = %{"type" => "array", "items" => "int"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"items":{"type":"int"},"type":"array"})

      # TODO all fields
    end

    test "fixed" do
      # primitive fixed
      input = %{"type" => "fixed", "name" => "double", "size" => 2}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"name":"double","size":2,"type":"fixed"})
    end

    test "reference" do
    end

    test "union" do
    end

    test "complex" do
    end
  end

  describe "canonical encoding" do
  end
end
