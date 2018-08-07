defmodule AvroEx.Decode.Test do
  use ExUnit.Case
  use ExUnitProperties

  @test_module AvroEx.Decode

  describe "decode (primitive)" do
    test "null" do
      {:ok, schema} = AvroEx.parse_schema(~S("null"))
      {:ok, avro_message} = AvroEx.encode(schema, nil)
      assert {:ok, nil} = @test_module.decode(schema, avro_message)
    end

    property "boolean" do
      check all bool <- StreamData.boolean() do
        {:ok, schema} = AvroEx.parse_schema(~S("boolean"))
        {:ok, encoded} = AvroEx.encode(schema, bool)

        assert {:ok, bool} = @test_module.decode(schema, encoded)
      end
    end

    property "integer" do
      check all integer <- StreamData.integer() do
        {:ok, schema} = AvroEx.parse_schema(~S("int"))
        {:ok, encoded} = AvroEx.encode(schema, integer)

        assert {:ok, ^integer} = @test_module.decode(schema, encoded)
      end
    end

    property "long" do
      check all long <- StreamData.integer(-2_147_483_647..2_147_483_647) do
        {:ok, schema} = AvroEx.parse_schema(~S("long"))
        {:ok, encoded} = AvroEx.encode(schema, long)

        assert {:ok, ^long} = @test_module.decode(schema, encoded)
      end
    end

    property "float" do
      check all float <- StreamData.float() do
        {:ok, schema} = AvroEx.parse_schema(~S("float"))
        {:ok, encoded} = AvroEx.encode(schema, float)

        assert {:ok, _} = @test_module.decode(schema, encoded)
      end
    end

    property "double" do
      check all double <- StreamData.float() do
        {:ok, schema} = AvroEx.parse_schema(~S("double"))
        {:ok, encoded} = AvroEx.encode(schema, double)

        assert {:ok, ^double} = @test_module.decode(schema, encoded)
      end
    end

    property "bytes" do
      check all bytes <- StreamData.binary() do
        {:ok, schema} = AvroEx.parse_schema(~S("bytes"))
        {:ok, encoded} = AvroEx.encode(schema, bytes)

        assert {:ok, ^bytes} = @test_module.decode(schema, encoded)
      end
    end

    property "string" do
      check all string <- StreamData.string(:printable) do
        {:ok, schema} = AvroEx.parse_schema(~S("string"))
        {:ok, encoded} = AvroEx.encode(schema, string)

        assert {:ok, ^string} = @test_module.decode(schema, encoded)
      end
    end
  end

  describe "complex types" do
    property "record" do
      check all record <-
                  fixed_map(%{
                    "a" => integer(),
                    "b" => integer(),
                    "e" => string(:printable)
                  }) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "MyRecord", "fields": [
        {"type": "int", "name": "a"},
        {"type": "int", "name": "b", "aliases": ["c", "d"]},
        {"type": "string", "name": "e"}
      ]}))

        # %{"a" => 1, "b" => 2, "e" => "Hello world!"})
        {:ok, encoded_message} = AvroEx.encode(schema, record)

        assert {:ok, ^record} = @test_module.decode(schema, encoded_message)
      end
    end

    property "union" do
      check all item <- one_of([nil, integer()]) do
        {:ok, schema} = AvroEx.parse_schema(~S(["null", "int"]))

        {:ok, encoded} = AvroEx.encode(schema, item)

        assert {:ok, ^item} = @test_module.decode(schema, encoded)
      end
    end

    property "array" do
      check all items <- list_of(one_of([nil, integer()])) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": ["null", "int"]}))

        {:ok, encoded_array} = AvroEx.encode(schema, items)

        assert {:ok, ^items} = @test_module.decode(schema, encoded_array)
      end
    end

    property "map" do
      check all map <- StreamData.map_of(StreamData.string(:ascii), StreamData.integer()) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": ["null", "int"]}))

        {:ok, encoded_array} = AvroEx.encode(schema, map)

        assert {:ok, ^map} = @test_module.decode(schema, encoded_array)
      end
    end

    property "enum" do
      check all option <- member_of(["heart", "spade", "diamond", "club"]) do
        {:ok, schema} =
          AvroEx.parse_schema(
            ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
          )

        {:ok, encoded} = AvroEx.encode(schema, option)

        assert {:ok, ^option} = @test_module.decode(schema, encoded)
      end
    end

    property "fixed" do
      check all sha <- string(:ascii, length: 40) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "fixed", "name": "SHA", "size": "40"}))
        {:ok, encoded_sha} = AvroEx.encode(schema, sha)
        assert {:ok, ^sha} = @test_module.decode(schema, encoded_sha)
      end
    end
  end

  describe "decode logical types" do
    test "datetime micros" do
      now = DateTime.utc_now()

      {:ok, micro_schema} =
        AvroEx.parse_schema(~S({"type": "long", "logicalType":"timestamp-micros"}))

      {:ok, micro_encode} = AvroEx.encode(micro_schema, now)
      assert {:ok, ^now} = @test_module.decode(micro_schema, micro_encode)
    end

    test "datetime millis" do
      now = DateTime.utc_now() |> DateTime.truncate(:millisecond)

      {:ok, milli_schema} =
        AvroEx.parse_schema(~S({"type": "long", "logicalType":"timestamp-millis"}))

      {:ok, milli_encode} = AvroEx.encode(milli_schema, now)
      assert {:ok, ^now} = @test_module.decode(milli_schema, milli_encode)
    end

    test "datetime nanos" do
      now = DateTime.utc_now()

      {:ok, nano_schema} =
        AvroEx.parse_schema(~S({"type": "long", "logicalType":"timestamp-nanos"}))

      {:ok, nano_encode} = AvroEx.encode(nano_schema, now)
      assert {:ok, ^now} = @test_module.decode(nano_schema, nano_encode)
    end

    test "time micros" do
      now = Time.utc_now() |> Time.truncate(:microsecond)

      {:ok, micro_schema} = AvroEx.parse_schema(~S({"type": "long", "logicalType":"time-micros"}))
      {:ok, micro_encode} = AvroEx.encode(micro_schema, now)
      assert {:ok, ^now} = @test_module.decode(micro_schema, micro_encode)
    end

    test "time millis" do
      now = Time.utc_now() |> Time.truncate(:millisecond)

      {:ok, milli_schema} = AvroEx.parse_schema(~S({"type": "int", "logicalType":"time-millis"}))
      {:ok, milli_encode} = AvroEx.encode(milli_schema, now)
      {:ok, time} = @test_module.decode(milli_schema, milli_encode)

      assert Time.truncate(time, :millisecond) == now
    end
  end
end
