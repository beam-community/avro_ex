defmodule AvroEx.Encode.Test do
  require __MODULE__.Macros
  alias __MODULE__.Macros
  use ExUnit.Case, async: true

  @test_module AvroEx.Encode

  describe "encode (primitive)" do
    test "null" do
      {:ok, schema} = AvroEx.decode_schema(~S("null"))

      assert {:ok, ""} = @test_module.encode(schema, nil)
    end

    test "boolean" do
      {:ok, schema} = AvroEx.decode_schema(~S("boolean"))

      assert {:ok, <<1::8>>} = @test_module.encode(schema, true)
      assert {:ok, <<0::8>>} = @test_module.encode(schema, false)
    end

    test "integer" do
      {:ok, schema} = AvroEx.decode_schema(~S("int"))

      assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
    end

    test "long" do
      {:ok, schema} = AvroEx.decode_schema(~S("long"))

      assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
    end

    test "float" do
      {:ok, schema} = AvroEx.decode_schema(~S("float"))

      assert {:ok, <<205, 204, 140, 63>>} = @test_module.encode(schema, 1.1)
    end

    test "double" do
      {:ok, schema} = AvroEx.decode_schema(~S("double"))

      assert {:ok, <<154, 153, 153, 153, 153, 153, 241, 63>>} = @test_module.encode(schema, 1.1)
    end

    test "bytes" do
      {:ok, schema} = AvroEx.decode_schema(~S("bytes"))

      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} = @test_module.encode(schema, "abcdefg")
    end

    test "string" do
      {:ok, schema} = AvroEx.decode_schema(~S("string"))

      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} = @test_module.encode(schema, "abcdefg")
      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} = @test_module.encode(schema, :abcdefg)

      assert {:error, %AvroEx.EncodeError{message: message}} = @test_module.encode(schema, nil)
      assert message == "Schema Mismatch: Expected value of string, got nil"

      assert {:error, %AvroEx.EncodeError{message: message}} = @test_module.encode(schema, true)
      assert message == "Schema Mismatch: Expected value of string, got true"

      assert {:error, %AvroEx.EncodeError{message: message}} = @test_module.encode(schema, false)
      assert message == "Schema Mismatch: Expected value of string, got false"
    end
  end

  describe "encode (logical types)" do
    test "date" do
      assert %AvroEx.Schema{} = schema = AvroEx.decode_schema!(%{"type" => "int", "logicalType" => "date"})
      date1 = ~D[1970-01-01]
      assert {:ok, <<0>>} = AvroEx.encode(schema, date1)

      date2 = ~D[1970-03-01]
      assert {:ok, "v"} = AvroEx.encode(schema, date2)
    end

    test "decimal" do
      schema = "test/fixtures/decimal.avsc" |> File.read!() |> AvroEx.decode_schema!()

      encoded =
        AvroEx.encode!(schema, %{
          "decimalField1" => Decimal.new("1.23456789E-7"),
          "decimalField2" => Decimal.new("4.54545454545E-35"),
          "decimalField3" => Decimal.new("-111111111.1"),
          "decimalField4" => Decimal.new("5.3E-11")
        })

      assert AvroEx.decode!(schema, encoded, decimals: :exact) == %{
               "decimalField1" => Decimal.new("1.23456789E-7"),
               "decimalField2" => Decimal.new("4.54545454545E-35"),
               "decimalField3" => Decimal.new("-111111111.1"),
               "decimalField4" => Decimal.new("5.3E-11")
             }

      # This reference file was encoded using avro's reference implementation:
      #
      # ```java
      # Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
      # BigDecimal bigDecimal = new BigDecimal(valueInString);
      # return conversion.toBytes(bigDecimal, schema, logicalType);
      # ```
      assert encoded == File.read!("test/fixtures/decimal.avro")
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
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "record", "name": "Record", "fields": [
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

      {:ok, null_schema} = AvroEx.decode_schema(~S("null"))
      {:ok, boolean_schema} = AvroEx.decode_schema(~S("boolean"))
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))
      {:ok, long_schema} = AvroEx.decode_schema(~S("long"))
      {:ok, float_schema} = AvroEx.decode_schema(~S("float"))
      {:ok, double_schema} = AvroEx.decode_schema(~S("double"))
      {:ok, string_schema} = AvroEx.decode_schema(~S("string"))
      {:ok, bytes_schema} = AvroEx.decode_schema(~S("bytes"))

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
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "record", "name": "Record", "fields": [
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

    test "can encode records with atom keys and string values" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": "string", "name": "first"},
        {"type": "string", "name": "last"},
        {"name": "meta", "type": {
          "name": "MetaRecord",
          "type": "record",
          "fields": [
            {"type": "int", "name": "age"}
          ]
       }}]}))

      assert {:ok, "\bDave\nLucia@"} =
               @test_module.encode(schema, %{"first" => "Dave", "last" => "Lucia", "meta" => %{"age" => 32}})

      assert {:ok, "\bDave\nLucia@"} = @test_module.encode(schema, %{first: "Dave", last: "Lucia", meta: %{age: 32}})

      assert {:ok, "\bdave\nlucia@"} = @test_module.encode(schema, %{first: :dave, last: :lucia, meta: %{age: 32}})
    end

    test "works as expected with default of null on union type" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": ["null", "string"], "name": "maybe_null", "default": null}
        ]}))

      assert {:ok, <<0>>} = @test_module.encode(schema, %{})
      assert {:ok, <<2, 2, 49>>} = @test_module.encode(schema, %{"maybe_null" => "1"})
    end

    test "works with logicalType field values" do
      schema =
        AvroEx.decode_schema!(%{
          "type" => "record",
          "name" => "Record",
          "fields" => [
            %{"name" => "timestamp", "type" => %{"type" => "long", "logicalType" => "timestamp-millis"}}
          ]
        })

      timestamp = ~U[2022-02-23 20:28:13.498428Z]

      assert {:ok, <<244, 132, 169, 132, 229, 95>>} = @test_module.encode(schema, %{timestamp: timestamp})
    end
  end

  describe "encode (union)" do
    test "works as expected with nulls" do
      {:ok, schema} = AvroEx.decode_schema(~S(["null", "int"]))
      {:ok, null_schema} = AvroEx.decode_schema(~S("null"))
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))

      {:ok, index} = @test_module.encode(int_schema, 0)
      {:ok, encoded_null} = @test_module.encode(null_schema, nil)
      {:ok, encoded_union} = @test_module.encode(schema, nil)

      assert encoded_union == index <> encoded_null
    end

    test "works as expected with ints" do
      {:ok, schema} = AvroEx.decode_schema(~S(["null", "int"]))
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_int} = @test_module.encode(int_schema, 2086)
      {:ok, encoded_union} = @test_module.encode(schema, 2086)

      assert encoded_union == index <> encoded_int
    end

    test "works as expected with int and long" do
      {:ok, schema} = AvroEx.decode_schema(~S(["int", "long"]))
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))
      {:ok, long_schema} = AvroEx.decode_schema(~S("long"))

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_long} = @test_module.encode(long_schema, -3_376_656_585_598_455_353)
      {:ok, encoded_union} = @test_module.encode(schema, -3_376_656_585_598_455_353)

      assert encoded_union == index <> encoded_long
    end

    test "works as expected with float and double" do
      {:ok, schema} = AvroEx.decode_schema(~S(["float", "double"]))
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))
      {:ok, double_schema} = AvroEx.decode_schema(~S("double"))

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_long} = @test_module.encode(double_schema, 0.0000000001)
      {:ok, encoded_union} = @test_module.encode(schema, 0.0000000001)

      assert encoded_union == index <> encoded_long
    end

    test "works as expected with logical types" do
      datetime_json = ~S({"type": "long", "logicalType":"timestamp-millis"})
      datetime_value = ~U[2020-09-17 12:56:50.438Z]

      {:ok, schema} = AvroEx.decode_schema(~s(["null", #{datetime_json}]))
      {:ok, datetime_schema} = AvroEx.decode_schema(datetime_json)

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

      {:ok, schema} = AvroEx.decode_schema(json_schema)
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))
      {:ok, record_schema} = AvroEx.decode_schema(record_json)

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_record} = @test_module.encode(record_schema, %{"a" => 25, "b" => "hello"})
      {:ok, encoded_union} = @test_module.encode(schema, %{"a" => 25, "b" => "hello"})

      assert encoded_union == index <> encoded_record
    end

    test "works as expected with union values tagged for a named possibility" do
      record_json_factory = fn name ->
        ~s"""
          {
            "type": "record",
            "name": "#{name}",
            "fields": [
              {"type": "string", "name": "value"}
            ]
          }
        """
      end

      json_schema = ~s([#{record_json_factory.("a")}, #{record_json_factory.("b")}])

      {:ok, schema} = AvroEx.decode_schema(json_schema)
      {:ok, int_schema} = AvroEx.decode_schema(~S("int"))
      {:ok, record_schema} = AvroEx.decode_schema(record_json_factory.("b"))

      {:ok, index} = @test_module.encode(int_schema, 1)
      {:ok, encoded_record} = @test_module.encode(record_schema, %{"value" => "hello"})
      {:ok, encoded_union} = @test_module.encode(schema, {"b", %{"value" => "hello"}})

      assert encoded_union == index <> encoded_record
    end

    test "errors with a clear error for tagged unions" do
      record_json_factory = fn name ->
        ~s"""
          {
            "type": "record",
            "name": "#{name}",
            "fields": [
              {"type": "string", "name": "value"}
            ]
          }
        """
      end

      json_schema = ~s([#{record_json_factory.("a")}, #{record_json_factory.("b")}])

      {:ok, schema} = AvroEx.decode_schema(json_schema)

      assert {:error,
              %AvroEx.EncodeError{
                message:
                  "Schema Mismatch: Expected value of Union<possibilities=Record<name=a>|Record<name=b>>" <>
                    ", got {\"c\", %{\"value\" => \"hello\"}}"
              }} = @test_module.encode(schema, {"c", %{"value" => "hello"}})
    end

    test "errors if the data doesn't match the schema" do
      {:ok, schema} = AvroEx.decode_schema(~S(["null", "int"]))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Union<possibilities=null|int>, got \"wat\""
              }} = @test_module.encode(schema, "wat")
    end
  end

  describe "encode (map)" do
    test "properly encodes the length, key-value pairs, and terminal byte" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert {:ok, <<2, 12, 118, 97, 108, 117, 101, 49, 2, 0>>} = @test_module.encode(schema, %{"value1" => 1})
    end

    test "can encode atom keys" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert {:ok, <<2, 12, 118, 97, 108, 117, 101, 49, 2, 0>>} = @test_module.encode(schema, %{value1: 1})
    end

    test "encodes an empty map" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert {:ok, <<0>>} = @test_module.encode(schema, %{})
    end
  end

  describe "encode (array)" do
    test "properly encodes an array with length, items, and terminal byte" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      assert {:ok, <<6, 2, 4, 6, 0>>} = @test_module.encode(schema, [1, 2, 3])
    end

    test "properly encodes an array with length, byte_size, items, and terminal byte" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      assert {:ok, <<5, 6, 2, 4, 6, 0>>} = @test_module.encode(schema, [1, 2, 3], include_block_byte_size: true)
    end

    test "encodes an empty array" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      assert {:ok, <<0>>} = @test_module.encode(schema, [])
    end
  end

  describe "encode (fixed)" do
    test "encodes the given value" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "fixed", "name": "sha", "size": 40}))
      sha = binary_of_size(40)
      assert {:ok, encoded} = @test_module.encode(schema, sha)
      assert encoded == sha
    end

    test "fails if the value is too large" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "fixed", "name": "sha", "size": 40}))
      bad_sha = binary_of_size(41)

      assert {:error,
              %AvroEx.EncodeError{
                message:
                  "Invalid size for Fixed<name=sha, size=40>. Size of 41 for \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""
              }} = @test_module.encode(schema, bad_sha)
    end
  end

  describe "encode (enum)" do
    test "encodes the index of the symbol" do
      {:ok, enum_schema} =
        AvroEx.decode_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      {:ok, long_schema} = AvroEx.decode_schema(~S("long"))

      {:ok, heart_index} = @test_module.encode(long_schema, 0)
      {:ok, spade_index} = @test_module.encode(long_schema, 1)
      {:ok, diamond_index} = @test_module.encode(long_schema, 2)
      {:ok, club_index} = @test_module.encode(long_schema, 3)

      {:ok, heart} = @test_module.encode(enum_schema, "heart")
      {:ok, spade} = @test_module.encode(enum_schema, "spade")

      # Can handle atoms
      {:ok, diamond} = @test_module.encode(enum_schema, :diamond)
      {:ok, club} = @test_module.encode(enum_schema, :club)

      assert heart_index == heart
      assert spade_index == spade
      assert diamond_index == diamond
      assert club_index == club
    end
  end

  describe "EncodingError - schema mismatch" do
    test "(null)" do
      {:ok, schema} = AvroEx.decode_schema(~S("null"))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of null, got :wat"
              }} = @test_module.encode(schema, :wat)
    end

    test "(int)" do
      {:ok, schema} = AvroEx.decode_schema(~S("int"))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of int, got :wat"
              }} = @test_module.encode(schema, :wat)
    end

    test "(array)" do
      schema = AvroEx.decode_schema!(~S({"type": "array", "items": "int"}))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Array<items=int>, got :wat"
              }} = @test_module.encode(schema, :wat)
    end

    test "(enum)" do
      schema =
        AvroEx.decode_schema!(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Enum<name=Suit>, got 12345"
              }} = @test_module.encode(schema, 12_345)
    end

    test "(fixed)" do
      schema = AvroEx.decode_schema!(~S({"type": "fixed", "name": "sha", "size": 40}))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Fixed<name=sha, size=40>, got 12345"
              }} = @test_module.encode(schema, 12_345)
    end

    test "(map)" do
      schema = AvroEx.decode_schema!(~S({"type": "map", "values": "int"}))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Map<values=int>, got 12345"
              }} = @test_module.encode(schema, 12_345)
    end

    test "(record)" do
      assert schema =
               AvroEx.decode_schema!(~S({"type": "record", "namespace": "beam.community", "name": "Name", "fields": [
        {"type": "string", "name": "first"},
        {"type": "string", "name": "last"}
      ]}))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Record<name=beam.community.Name>, got :wat"
              }} = @test_module.encode(schema, :wat)

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of string, got nil"
              }} = @test_module.encode(schema, %{})
    end

    test "(reference)" do
      assert schema =
               AvroEx.decode_schema!(%{
                 "type" => "record",
                 "namespace" => "beam.community",
                 "name" => "Name",
                 "fields" => [
                   %{
                     "name" => "first_name",
                     "type" => %{
                       "type" => "record",
                       "name" => "DefinedRecord",
                       "fields" => [
                         %{
                           "type" => "string",
                           "name" => "full"
                         }
                       ]
                     }
                   },
                   %{
                     "type" => "beam.community.DefinedRecord",
                     "name" => "last_name"
                   }
                 ]
               })

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Record<name=DefinedRecord>, got :wat"
              }} = @test_module.encode(schema, %{first_name: %{full: "foo"}, last_name: :wat})

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Record<name=DefinedRecord>, got nil"
              }} = @test_module.encode(schema, %{})
    end

    test "(reference with union)" do
      assert schema =
               AvroEx.decode_schema!(%{
                 "type" => "record",
                 "namespace" => "beam.community",
                 "name" => "Name",
                 "fields" => [
                   %{
                     "name" => "first_name",
                     "type" => %{
                       "type" => "record",
                       "name" => "DefinedRecord",
                       "fields" => [
                         %{
                           "type" => "string",
                           "name" => "full"
                         }
                       ]
                     }
                   },
                   %{
                     "type" => ["null", "beam.community.DefinedRecord"],
                     "name" => "last_name"
                   }
                 ]
               })

      assert {:error,
              %AvroEx.EncodeError{
                message:
                  "Schema Mismatch: Expected value of Union<possibilities=null|Reference<name=beam.community.DefinedRecord>>, got :wat"
              }} = @test_module.encode(schema, %{first_name: %{full: "foo"}, last_name: :wat})
    end

    test "(union)" do
      schema = AvroEx.decode_schema!(~S(["null", "string"]))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Schema Mismatch: Expected value of Union<possibilities=null|string>, got 12345"
              }} = @test_module.encode(schema, 12_345)
    end
  end

  describe "EncodingError - Invalid Fixed size" do
    test "(fixed)" do
      schema = AvroEx.decode_schema!(~S({"type": "fixed", "name": "sha", "size": 40}))
      bad_sha = binary_of_size(39)

      assert {:error,
              %AvroEx.EncodeError{
                message:
                  "Invalid size for Fixed<name=sha, size=40>. Size of 39 for \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""
              }} = @test_module.encode(schema, bad_sha)
    end
  end

  describe "EncodingError - Invalid Symbol" do
    test "(enum)" do
      schema =
        AvroEx.decode_schema!(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      assert {:error,
              %AvroEx.EncodeError{
                message:
                  "Invalid symbol for Enum<name=Suit>. Expected value in [\"heart\", \"spade\", \"diamond\", \"club\"], got \"joker\""
              }} = @test_module.encode(schema, "joker")
    end
  end

  describe "EncodingError - Invalid string" do
    test "(fixed)" do
      schema = AvroEx.decode_schema!(~S("string"))

      assert {:error,
              %AvroEx.EncodeError{
                message: "Invalid string \"<<255, 255>>\""
              }} = @test_module.encode(schema, <<0xFFFF::16>>)
    end
  end

  @spec binary_of_size(integer, binary) :: binary
  def binary_of_size(size, bin \\ "")
  def binary_of_size(0, bin), do: bin
  def binary_of_size(size, bin), do: binary_of_size(size - 1, bin <> "a")
end
