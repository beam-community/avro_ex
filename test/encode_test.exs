defmodule AvroEx.Encode.Test do
  require __MODULE__.Macros
  alias __MODULE__.Macros
  use ExUnit.Case

  @test_module AvroEx.Encode

  describe "encode (primitive)" do
    test "null" do
      {:ok, schema} = AvroEx.parse_schema(~S("null"))

      assert {:ok, ""} = @test_module.encode(schema, nil)
    end

    test "boolean" do
      {:ok, schema} = AvroEx.parse_schema(~S("boolean"))

      assert {:ok, <<1::8>>} = @test_module.encode(schema, true)
      assert {:ok, <<0::8>>} = @test_module.encode(schema, false)
    end

    test "integer" do
      {:ok, schema} = AvroEx.parse_schema(~S("int"))

      assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
    end

    test "long" do
      {:ok, schema} = AvroEx.parse_schema(~S("long"))

      assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
    end

    test "float" do
      {:ok, schema} = AvroEx.parse_schema(~S("float"))

      assert {:ok, <<205, 204, 140, 63>>} = @test_module.encode(schema, 1.1)
    end

    test "double" do
      {:ok, schema} = AvroEx.parse_schema(~S("double"))

      assert {:ok, <<154, 153, 153, 153, 153, 153, 241, 63>>} = @test_module.encode(schema, 1.1)
    end

    test "bytes" do
      {:ok, schema} = AvroEx.parse_schema(~S("bytes"))

      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} = @test_module.encode(schema, "abcdefg")
    end

    test "string" do
      {:ok, schema} = AvroEx.parse_schema(~S("string"))

      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} = @test_module.encode(schema, "abcdefg")
    end
  end

  describe "variable_integer_encode" do
    Macros.assert_result(@test_module, :variable_integer_encode, [0], <<0>>)

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [1],
      <<1::size(8)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [128],
      <<32_769::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [16_383],
      <<65_407::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [16_384],
      <<8_421_377::size(24)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [4_294_967_041],
      <<129, 254, 255, 255, 15>>
    )
  end

  describe "encode (record)" do
    test "works as expected with primitive fields" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": "null", "name": "null"},
        {"type": "boolean", "name": "bool"},
        {"type": "int", "name": "integer"},
        {"type": "long", "name": "long"},
        {"type": "float", "name": "float"},
        {"type": "double", "name": "double"},
        {"type": "string", "name": "string"},
        {"type": "bytes", "name": "bytes"}
      ]}))

      record = %{
        "null" => nil,
        "bool" => true,
        "integer" => 25,
        "long" => 25,
        "float" => 2.5,
        "double" => 2.5,
        "string" => "abcdefg",
        "bytes" => "abcdefg"
      }

      {:ok, null_schema} = AvroEx.parse_schema(~S("null"))
      {:ok, boolean_schema} = AvroEx.parse_schema(~S("boolean"))
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))
      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
      {:ok, float_schema} = AvroEx.parse_schema(~S("float"))
      {:ok, double_schema} = AvroEx.parse_schema(~S("double"))
      {:ok, string_schema} = AvroEx.parse_schema(~S("string"))
      {:ok, bytes_schema} = AvroEx.parse_schema(~S("bytes"))

      assert {:ok,
              Enum.join([
                elem(@test_module.encode(null_schema, record["null"]), 1),
                elem(@test_module.encode(boolean_schema, record["bool"]), 1),
                elem(@test_module.encode(int_schema, record["integer"]), 1),
                elem(@test_module.encode(long_schema, record["long"]), 1),
                elem(@test_module.encode(float_schema, record["float"]), 1),
                elem(@test_module.encode(double_schema, record["double"]), 1),
                elem(@test_module.encode(string_schema, record["string"]), 1),
                elem(@test_module.encode(bytes_schema, record["bytes"]), 1)
              ])} == @test_module.encode(schema, record)
    end

    test "works as expected with default values" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": "null", "name": "null", "default": null},
        {"type": "boolean", "name": "bool", "default": false},
        {"type": "int", "name": "integer", "default": 0},
        {"type": "long", "name": "long", "default": 0},
        {"type": "float", "name": "float", "default": 0.0},
        {"type": "double", "name": "double", "default": 0.0},
        {"type": "string", "name": "string", "default": "ok"},
        {"type": "bytes", "name": "bytes", "default": "ok"}
      ]}))

      assert {:ok, _encoded} = @test_module.encode(schema, %{})
    end

    test "works as expected with default of null on union type" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": ["null", "string"], "name": "maybe_null", "default": null}
        ]}))

      assert {:ok, <<0>>} = @test_module.encode(schema, %{})
      assert {:ok, <<2, 2, 49>>} = @test_module.encode(schema, %{"maybe_null" => "1"})
    end
  end

  describe "encode (union)" do
    test "works as expected with nulls" do
      {:ok, schema} = AvroEx.parse_schema(~S(["null", "int"]))
      {:ok, null_schema} = AvroEx.parse_schema(~S("null"))
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))

      {:ok, index} = @test_module.encode(int_schema, 0)
      {:ok, encoded_null} = @test_module.encode(null_schema, nil)
      {:ok, encoded_union} = @test_module.encode(schema, nil)

      assert encoded_union == index <> encoded_null
    end

    test "works as expected with ints" do
      {:ok, schema} = AvroEx.parse_schema(~S(["null", "int"]))
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_int} = @test_module.encode(int_schema, 2086)
      {:ok, encoded_union} = @test_module.encode(schema, 2086)

      assert encoded_union == index <> encoded_int
    end

    test "works as expected with logical types" do
      datetime_json = ~S({"type": "long", "logicalType":"timestamp-millis"})
      datetime_value = ~U[2020-09-17 12:56:50.438Z]

      {:ok, schema} = AvroEx.parse_schema(~s(["null", #{datetime_json}]))
      {:ok, datetime_schema} = AvroEx.parse_schema(datetime_json)

      {:ok, index} = @test_module.encode(datetime_schema, 1)
      {:ok, encoded_datetime} = @test_module.encode(datetime_schema, datetime_value)
      {:ok, encoded_union} = @test_module.encode(schema, datetime_value)

      assert encoded_union == index <> encoded_datetime
    end

    test "works as expected with records" do
      record_json = ~S"""
        {
          "type": "record",
          "name": "MyRecord",
          "fields": [
            {"type": "int", "name": "a"},
            {"type": "string", "name": "b"}
          ]
        }
      """

      json_schema = ~s(["null", #{record_json}])

      {:ok, schema} = AvroEx.parse_schema(json_schema)
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))
      {:ok, record_schema} = AvroEx.parse_schema(record_json)

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_record} = @test_module.encode(record_schema, %{"a" => 25, "b" => "hello"})
      {:ok, encoded_union} = @test_module.encode(schema, %{"a" => 25, "b" => "hello"})

      assert encoded_union == index <> encoded_record
    end

    test "errors if the data doesn't match the schema" do
      {:ok, schema} = AvroEx.parse_schema(~S(["null", "int"]))

      assert {:error, :data_does_not_match_schema, "wat", _schema} = @test_module.encode(schema, "wat")
    end
  end

  describe "encode (map)" do
    test "properly encodes the length, key-value pairs, and terminal byte" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
      assert {:ok, <<2, 12, 118, 97, 108, 117, 101, 49, 2, 0>>} = @test_module.encode(schema, %{"value1" => 1})
    end

    test "encodes an empty map" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
      assert {:ok, <<0>>} = @test_module.encode(schema, %{})
    end
  end

  describe "encode (array)" do
    test "properly encodes an array with length, items, and terminal byte" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
      assert {:ok, <<6, 2, 4, 6, 0>>} = @test_module.encode(schema, [1, 2, 3])
    end

    test "encodes an empty array" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
      assert {:ok, <<0>>} = @test_module.encode(schema, [])
    end
  end

  describe "encode (fixed)" do
    test "encodes the given value" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "fixed", "name": "sha", "size": 40}))
      sha = binary_of_size(40)
      assert {:ok, encoded} = @test_module.encode(schema, sha)
      assert encoded == sha
    end

    test "fails if the value is too small" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "fixed", "name": "sha", "size": 40}))
      bad_sha = binary_of_size(39)

      assert {:error, :incorrect_fixed_size, [expected: 40, got: 39, name: "sha"]} =
               @test_module.encode(schema, bad_sha)
    end

    test "fails if the value is too large" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "fixed", "name": "sha", "size": 40}))
      bad_sha = binary_of_size(41)

      assert {:error, :incorrect_fixed_size, [expected: 40, got: 41, name: "sha"]} =
               @test_module.encode(schema, bad_sha)
    end
  end

  describe "encode (enum)" do
    test "encodes the index of the symbol" do
      {:ok, enum_schema} =
        AvroEx.parse_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))

      {:ok, heart_index} = @test_module.encode(long_schema, 0)
      {:ok, spade_index} = @test_module.encode(long_schema, 1)
      {:ok, diamond_index} = @test_module.encode(long_schema, 2)
      {:ok, club_index} = @test_module.encode(long_schema, 3)

      {:ok, heart} = @test_module.encode(enum_schema, "heart")
      {:ok, spade} = @test_module.encode(enum_schema, "spade")
      {:ok, diamond} = @test_module.encode(enum_schema, "diamond")
      {:ok, club} = @test_module.encode(enum_schema, "club")

      assert heart_index == heart
      assert spade_index == spade
      assert diamond_index == diamond
      assert club_index == club
    end
  end

  describe "Doesn't match schema" do
    test "returns the expected error tuple" do
      {:ok, schema} = AvroEx.parse_schema(~S("null"))

      assert {:error, :data_does_not_match_schema, :wat, _schema} = @test_module.encode(schema, :wat)
    end
  end

  @spec binary_of_size(integer, binary) :: binary
  def binary_of_size(size, bin \\ "")
  def binary_of_size(0, bin), do: bin
  def binary_of_size(size, bin), do: binary_of_size(size - 1, bin <> "a")
end
