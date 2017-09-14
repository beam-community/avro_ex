defmodule AvroEx.Decode.Test do
  use ExUnit.Case

  @test_module AvroEx.Decode

  describe "decode (primitive)" do
    test "null" do
      {:ok, schema} = AvroEx.parse_schema(~S("null"))
      {:ok, avro_message} = AvroEx.encode(schema, nil)
      assert {:ok, nil} = @test_module.decode(schema, avro_message)
    end

    test "boolean" do
      {:ok, schema} = AvroEx.parse_schema(~S("boolean"))
      {:ok, true_message} = AvroEx.encode(schema, true)
      {:ok, false_message} = AvroEx.encode(schema, false)

      assert {:ok, true} = @test_module.decode(schema, true_message)
      assert {:ok, false} = @test_module.decode(schema, false_message)
    end

    test "integer" do
      {:ok, schema} = AvroEx.parse_schema(~S("int"))
      {:ok, zero} = AvroEx.encode(schema, 0)
      {:ok, neg_ten} = AvroEx.encode(schema, -10)
      {:ok, ten} = AvroEx.encode(schema, 10)
      {:ok, big} = AvroEx.encode(schema, 5_000_000)
      {:ok, small} = AvroEx.encode(schema, -5_000_000)

      assert {:ok, 0} = @test_module.decode(schema, zero)
      assert {:ok, -10} = @test_module.decode(schema, neg_ten)
      assert {:ok, 10} = @test_module.decode(schema, ten)
      assert {:ok, 5_000_000} = @test_module.decode(schema, big)
      assert {:ok, -5_000_000} = @test_module.decode(schema, small)
    end

    test "long" do
      {:ok, schema} = AvroEx.parse_schema(~S("long"))
      {:ok, zero} = AvroEx.encode(schema, 0)
      {:ok, neg_ten} = AvroEx.encode(schema, -10)
      {:ok, ten} = AvroEx.encode(schema, 10)
      {:ok, big} = AvroEx.encode(schema, 2147483647)
      {:ok, small} = AvroEx.encode(schema, -2147483647)

      assert {:ok, 0} = @test_module.decode(schema, zero)
      assert {:ok, -10} = @test_module.decode(schema, neg_ten)
      assert {:ok, 10} = @test_module.decode(schema, ten)
      assert {:ok, 2147483647} = @test_module.decode(schema, big)
      assert {:ok, -2147483647} = @test_module.decode(schema, small)
    end

    test "float" do
      {:ok, schema} = AvroEx.parse_schema(~S("float"))
      {:ok, zero} = AvroEx.encode(schema, 0.0)
      {:ok, big} = AvroEx.encode(schema, 256.25)

      assert {:ok, 0.0} = @test_module.decode(schema, zero)
      assert {:ok, 256.25} = @test_module.decode(schema, big)
    end

    test "double" do
      {:ok, schema} = AvroEx.parse_schema(~S("double"))
      {:ok, zero} = AvroEx.encode(schema, 0.0)
      {:ok, big} = AvroEx.encode(schema, 256.25)

      assert {:ok, 0.0} = @test_module.decode(schema, zero)
      assert {:ok, 256.25} = @test_module.decode(schema, big)
    end

    test "bytes" do
      {:ok, schema} = AvroEx.parse_schema(~S("bytes"))
      {:ok, bytes} = AvroEx.encode(schema, <<222, 213, 194, 34, 58, 92, 95, 62>>)

      assert {:ok, <<222, 213, 194, 34, 58, 92, 95, 62>>} = @test_module.decode(schema, bytes)
    end

    test "string" do
      {:ok, schema} = AvroEx.parse_schema(~S("string"))
      {:ok, bytes} = AvroEx.encode(schema, "Hello there 🕶")

      assert {:ok, "Hello there 🕶"} = @test_module.decode(schema, bytes)
    end
  end

  describe "complex types" do
    test "record" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "MyRecord", "fields": [
        {"type": "int", "name": "a"},
        {"type": "int", "name": "b", "aliases": ["c", "d"]},
        {"type": "string", "name": "e"}
      ]}))

      {:ok, encoded_message} = AvroEx.encode(schema, %{"a" => 1, "b" => 2, "e" => "Hello world!"})

      assert {:ok, %{"a" => 1, "b" => 2, "e" => "Hello world!"}} =
        @test_module.decode(schema, encoded_message)
    end

    test "union" do
      {:ok, schema} = AvroEx.parse_schema(~S(["null", "int"]))

      {:ok, encoded_null} = AvroEx.encode(schema, nil)
      {:ok, encoded_int} = AvroEx.encode(schema, 25)

      assert {:ok, nil} = @test_module.decode(schema, encoded_null)
      assert {:ok, 25} = @test_module.decode(schema, encoded_int)
    end

    test "array" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": ["null", "int"]}))

      {:ok, encoded_array} = AvroEx.encode(schema, [1, 2, 3, nil, 4, 5, nil])

      assert {:ok, [1, 2, 3, nil, 4, 5, nil]} = @test_module.decode(schema, encoded_array)
    end

    test "map" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": ["null", "int"]}))

      {:ok, encoded_array} = AvroEx.encode(schema, %{"a" => 1, "b" => nil, "c" => 3})

      assert {:ok, %{"a" => 1, "b" => nil, "c" => 3}} =
        @test_module.decode(schema, encoded_array)
    end

    test "enum" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      {:ok, club} = AvroEx.encode(schema, "club")
      {:ok, heart} = AvroEx.encode(schema, "heart")
      {:ok, diamond} = AvroEx.encode(schema, "diamond")
      {:ok, spade} = AvroEx.encode(schema, "spade")

      assert {:ok, "club"} = @test_module.decode(schema, club)
      assert {:ok, "heart"} = @test_module.decode(schema, heart)
      assert {:ok, "diamond"} = @test_module.decode(schema, diamond)
      assert {:ok, "spade"} = @test_module.decode(schema, spade)
    end

    test "fixed" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "fixed", "name": "SHA", "size": "40"}))
      sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      {:ok, encoded_sha} = AvroEx.encode(schema, sha)
      assert {:ok, ^sha} = @test_module.decode(schema, encoded_sha)
    end
  end
end
