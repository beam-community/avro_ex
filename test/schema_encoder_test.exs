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

      all = %{
        "type" => "enum",
        "symbols" => ["a"],
        "name" => "cool",
        "aliases" => ["alias"],
        "doc" => "docs",
        "extra" => "val",
        "namespace" => "namespace"
      }

      assert schema = AvroEx.decode_schema!(all)

      assert AvroEx.encode_schema(schema) ==
               ~S({"aliases":["alias"],"doc":"docs","name":"cool","namespace":"namespace","symbols":["a"],"type":"enum","extra":"val"})
    end

    test "map" do
      # primitive map
      input = %{"type" => "map", "values" => "int"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"type":"map","values":{"type":"int"}})

      # complex map
      complex = %{"type" => "map", "values" => ["null", "int"]}

      assert schema = AvroEx.decode_schema!(complex)
      assert AvroEx.encode_schema(schema) == ~S({"type":"map","values":[{"type":"null"},{"type":"int"}]})

      all = %{"type" => "map", "values" => "int", "default" => %{"a" => 1}, "extra" => "val"}

      assert schema = AvroEx.decode_schema!(all)
      assert AvroEx.encode_schema(schema) == ~S({"default":{"a":1},"type":"map","values":{"type":"int"},"extra":"val"})
    end

    test "record" do
      # primitive record
      input = %{"type" => "record", "name" => "test", "fields" => [%{"name" => "a", "type" => "string"}]}

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"string\"}}],\"name\":\"test\",\"type\":\"record\"}"

      # Complex record
      complex = %{"type" => "record", "name" => "test", "fields" => [%{"name" => "a", "type" => ["int", "string"]}]}

      assert schema = AvroEx.decode_schema!(complex)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\"},{\"type\":\"string\"}]}],\"name\":\"test\",\"type\":\"record\"}"

      all = %{
        "type" => "record",
        "name" => "all",
        "namespace" => "beam.community",
        "doc" => "docs!",
        "aliases" => ["a_map"],
        "extra" => "val",
        "fields" => [
          %{
            "name" => "one",
            "type" => "int",
            "doc" => "field",
            "default" => 1,
            "aliases" => ["first"],
            "meta" => "meta"
          }
        ]
      }

      assert schema = AvroEx.decode_schema!(all)

      assert AvroEx.encode_schema(schema) ==
               "{\"aliases\":[\"a_map\"],\"doc\":\"docs!\",\"fields\":[{\"aliases\":[\"first\"],\"default\":1,\"doc\":\"field\",\"name\":\"one\",\"type\":{\"type\":\"int\"},\"meta\":\"meta\"}],\"name\":\"all\",\"namespace\":\"beam.community\",\"type\":\"record\",\"extra\":\"val\"}"
    end

    test "array" do
      # primitive array
      input = %{"type" => "array", "items" => "int"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"items":{"type":"int"},"type":"array"})

      all = %{"type" => "array", "items" => "int", "default" => [1, 2, 3]}

      assert schema = AvroEx.decode_schema!(all)
      assert AvroEx.encode_schema(schema) == ~S({"default":[1,2,3],"items":{"type":"int"},"type":"array"})
    end

    test "fixed" do
      # primitive fixed
      input = %{"type" => "fixed", "name" => "double", "size" => 2}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S({"name":"double","size":2,"type":"fixed"})

      all = %{
        "type" => "fixed",
        "name" => "double",
        "namespace" => "beam.community",
        "aliases" => ["two"],
        "doc" => "docs",
        "size" => 2,
        "extra" => "val"
      }

      assert schema = AvroEx.decode_schema!(all)

      assert AvroEx.encode_schema(schema) ==
               "{\"aliases\":[\"two\"],\"doc\":\"docs\",\"name\":\"double\",\"namespace\":\"beam.community\",\"size\":2,\"type\":\"fixed\",\"extra\":\"val\"}"
    end

    test "reference" do
      input = %{
        "type" => "record",
        "name" => "LinkedList",
        "fields" => [
          %{"name" => "value", "type" => "int"},
          %{"name" => "next", "type" => ["null", "LinkedList"]}
        ]
      }

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"int\"}},{\"name\":\"next\",\"type\":[{\"type\":\"null\"},\"LinkedList\"]}],\"name\":\"LinkedList\",\"type\":\"record\"}"
    end

    test "union" do
      input = ["null", "int"]
      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema) == ~S([{"type":"null"},{"type":"int"}])
    end

    test "complex" do
      input = %{
        "type" => "record",
        "name" => "complex",
        "fields" => [
          %{"name" => "a", "type" => ["null", %{"type" => "fixed", "name" => "double", "size" => 2}]},
          %{"name" => "b", "type" => %{"type" => "map", "values" => "string"}}
        ]
      }

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema) ==
               "{\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"null\"},{\"name\":\"double\",\"size\":2,\"type\":\"fixed\"}]},{\"name\":\"b\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\"}}}],\"name\":\"complex\",\"type\":\"record\"}"
    end
  end

  describe "canonical encoding" do
    test "it collapses primitives" do
      input = %{"type" => "int", "logicalType" => "date"}

      assert schema = AvroEx.decode_schema!(input)
      assert AvroEx.encode_schema(schema, canonical: true) == ~S("int")
    end

    test "it replaces names with full names, drops namespace" do
      input = %{
        "type" => "record",
        "name" => "MyRecord",
        "namespace" => "beam.community",
        "fields" => [
          %{"name" => "a", "type" => %{"name" => "MyFixed", "type" => "fixed", "size" => 10}},
          %{
            "name" => "b",
            "type" => %{"name" => "MyEnum", "type" => "enum", "namespace" => "java.community", "symbols" => ["one"]}
          }
        ]
      }

      assert schema = AvroEx.decode_schema!(input)

      assert AvroEx.encode_schema(schema, canonical: true) ==
               ~S({"name":"beam.community.MyRecord","type":"record","fields":[{"name":"a","type":{"name":"beam.community.MyFixed","type":"fixed","size":10}},{"name":"b","type":{"name":"java.community.MyEnum","type":"enum","symbols":["one"]}}]})
    end

    test "the order fields is name, type, fields, symbols, items, values, size" do
      input = %{
        "type" => "record",
        "name" => "MyRecord",
        "namespace" => "beam.community",
        "fields" => [
          %{"name" => "a", "type" => %{"name" => "MyFixed", "type" => "fixed", "size" => 10}},
          %{
            "name" => "b",
            "type" => %{"name" => "MyEnum", "type" => "enum", "namespace" => "java.community", "symbols" => ["one"]}
          },
          %{"name" => "c", "type" => %{"type" => "map", "values" => "int"}},
          %{"name" => "d", "type" => %{"type" => "array", "items" => "int"}},
          %{"name" => "e", "type" => "int"}
        ]
      }

      assert schema = AvroEx.decode_schema!(input, strict: true)

      assert AvroEx.encode_schema(schema, canonical: true) ==
               "{\"name\":\"beam.community.MyRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"a\",\"type\":{\"name\":\"beam.community.MyFixed\",\"type\":\"fixed\",\"size\":10}},{\"name\":\"b\",\"type\":{\"name\":\"java.community.MyEnum\",\"type\":\"enum\",\"symbols\":[\"one\"]}},{\"name\":\"c\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"d\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"e\",\"type\":\"int\"}]}"
    end
  end
end
