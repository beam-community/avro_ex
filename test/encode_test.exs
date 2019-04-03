defmodule AvroEx.Encode.Test.Macros do
  defmacro assert_result(m, f, a, result) do
    quote do
      test "#{unquote(m)}.#{unquote(f)} - #{unquote(:erlang.unique_integer())}" do
        assert apply(unquote(m), unquote(f), unquote(a)) == unquote(result)
      end
    end
  end
end

defmodule AvroEx.Encode.Test do
  require __MODULE__.Macros
  alias __MODULE__.Macros
  use ExUnit.Case
  use ExUnitProperties

  @test_module AvroEx.Encode

  describe "encode (primitive)" do
    test "null" do
      {:ok, schema} = AvroEx.parse_schema(~S("null"))

      assert {:ok, ""} = @test_module.encode(schema, nil)
    end

    property "boolean" do
      check all bool <- StreamData.boolean() do
        {:ok, schema} = AvroEx.parse_schema(~S("boolean"))

        assert {:ok, <<_::8>>} = @test_module.encode(schema, bool)
      end
    end

    property "integer" do
      check all int <- StreamData.integer() do
        {:ok, schema} = AvroEx.parse_schema(~S("int"))

        assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
        assert {:ok, _} = @test_module.encode(schema, int)
      end
    end

    property "long" do
      check all long <- StreamData.integer() do
        {:ok, schema} = AvroEx.parse_schema(~S("long"))

        assert {:ok, <<2::8>>} = @test_module.encode(schema, 1)
        assert {:ok, _} = @test_module.encode(schema, long)
      end
    end

    test "float" do
      {:ok, schema} = AvroEx.parse_schema(~S("float"))
      assert {:ok, <<205, 204, 140, 63>>} = @test_module.encode(schema, 1.1)
    end

    property "float" do
      check all float <- StreamData.float() do
        {:ok, schema} = AvroEx.parse_schema(~S("float"))
        assert {:ok, _} = @test_module.encode(schema, float)
      end
    end

    property "double" do
      check all double <- StreamData.float() do
        {:ok, schema} = AvroEx.parse_schema(~S("double"))
        assert {:ok, _} = @test_module.encode(schema, double)
      end
    end

    test "bytes" do
      {:ok, schema} = AvroEx.parse_schema(~S("bytes"))

      assert {:ok, <<14, 97, 98, 99, 100, 101, 102, 103>>} =
               @test_module.encode(schema, "abcdefg")
    end

    property "bytes" do
      check all bytes <- StreamData.binary() do
        {:ok, schema} = AvroEx.parse_schema(~S("bytes"))

        assert {:ok, _} = @test_module.encode(schema, bytes)
      end
    end

    property "string" do
      check all string <- StreamData.string(:printable) do
        {:ok, schema} = AvroEx.parse_schema(~S("string"))

        assert {:ok, _} = @test_module.encode(schema, string)
      end
    end
  end

  describe "variable_integer_encode (int)" do
    Macros.assert_result(@test_module, :variable_integer_encode, [<<0::size(32)>>], <<0>>)

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<1::size(32)>>],
      <<1::size(8)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<128::size(32)>>],
      <<32769::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<16383::size(32)>>],
      <<65407::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<16384::size(32)>>],
      <<8_421_377::size(24)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<4_294_967_041::size(32)>>],
      <<129, 254, 255, 255, 15>>
    )
  end

  describe "variable_integer_encode (long)" do
    Macros.assert_result(@test_module, :variable_integer_encode, [<<0::size(64)>>], <<0>>)

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<1::size(64)>>],
      <<1::size(8)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<128::size(64)>>],
      <<32769::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<16383::size(64)>>],
      <<65407::size(16)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<16384::size(64)>>],
      <<8_421_377::size(24)>>
    )

    Macros.assert_result(
      @test_module,
      :variable_integer_encode,
      [<<4_294_967_041::size(64)>>],
      <<129, 254, 255, 255, 15>>
    )
  end

  describe "encode (record)" do
    property "primitive fields" do
      check all record <-
                  fixed_map(%{
                    "null" => nil,
                    "bool" => boolean(),
                    "integer" => integer(),
                    "long" => integer(),
                    "float" => float(),
                    "double" => float(),
                    "string" => string(:printable),
                    "bytes" => binary()
                  }) do
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

        {:ok, null_schema} = AvroEx.parse_schema(~S("null"))
        {:ok, boolean_schema} = AvroEx.parse_schema(~S("boolean"))
        {:ok, int_schema} = AvroEx.parse_schema(~S("int"))
        {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
        {:ok, float_schema} = AvroEx.parse_schema(~S("float"))
        {:ok, double_schema} = AvroEx.parse_schema(~S("double"))
        {:ok, string_schema} = AvroEx.parse_schema(~S("string"))
        {:ok, bytes_schema} = AvroEx.parse_schema(~S("bytes"))

        assert {:ok,
                [
                  @test_module.encode(null_schema, record["null"]) |> elem(1),
                  @test_module.encode(boolean_schema, record["bool"]) |> elem(1),
                  @test_module.encode(int_schema, record["integer"]) |> elem(1),
                  @test_module.encode(long_schema, record["long"]) |> elem(1),
                  @test_module.encode(float_schema, record["float"]) |> elem(1),
                  @test_module.encode(double_schema, record["double"]) |> elem(1),
                  @test_module.encode(string_schema, record["string"]) |> elem(1),
                  @test_module.encode(bytes_schema, record["bytes"]) |> elem(1)
                ]
                |> Enum.join()} == @test_module.encode(schema, record)
      end
    end

    property "partial" do
      check all record <-
                  optional_map(%{
                    "null" => nil,
                    "bool" => boolean(),
                    "integer" => integer(),
                    "long" => integer(),
                    "float" => float(),
                    "double" => float(),
                    "string" => string(:printable),
                    "bytes" => binary()
                  }) do
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

        assert {:ok, encoded} = @test_module.encode(schema, record)
        assert {:ok, decoded} = @test_module.encode(schema, record)
      end
    end

    property "nullable, potentially empty record" do
      check all record <-
                  optional_map(%{
                    "maybe_null" => one_of([nil, string(:printable)])
                  }) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "record", "name": "Record", "fields": [
        {"type": ["null", "string"], "name": "maybe_null", "default": null}
        ]}))

        assert {:ok, encoded} = @test_module.encode(schema, record)
        assert {:ok, %{"maybe_null" => _}} = AvroEx.decode(schema, encoded)
      end
    end

    property "nested potentially nullable" do
      check all record <-
                  optional_map(%{
                    "record" => one_of([nil, optional_map(%{"hello" => string(:printable)})])
                  }) do
        {:ok, schema} = AvroEx.parse_schema(~S(
        {
          "type": "record", "name": "Record", "fields": [
          {
            "name": "record", 
            "default": null,
            "type": 
              [
                "null", 
                { 
                  "name": "optional_record",
                  "type": "record",
                  "fields": [
                    {
                      "name": "hello",
                    "type": ["null", "string"],
                    "default": null
                    }
                  ]
                }
              ]
          }
        ]}
      ))

        assert {:ok, encoded} = @test_module.encode(schema, record)
        assert {:ok, %{"record" => _}} = AvroEx.decode(schema, encoded)
      end
    end

    test "encoding nullable union" do
      record = %{"record" => %{}}
      {:ok, schema} = AvroEx.parse_schema(~S(
        {
          "type": "record", "name": "Record", "fields": [
          {
            "name": "record", 
            "default": null,
            "type": 
              [
                "null", 
                { 
                  "name": "optional_record",
                  "type": "record",
                  "fields": [
                    {
                      "name": "hello",
                    "type": ["null", "string"],
                    "default": null
                    }
                  ]
                }
              ]
          }
        ]}
      ))

      assert {:ok, encoded} = @test_module.encode(schema, record)
      assert {:ok, res} = AvroEx.decode(schema, encoded)
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

      assert {:error, :data_does_not_match_schema, "wat", _schema} =
               @test_module.encode(schema, "wat")
    end
  end

  describe "encode (map)" do
    property "" do
      check all map <- StreamData.map_of(StreamData.string(:ascii), StreamData.integer()) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
        assert {:ok, encoded_array} = @test_module.encode(schema, map)
        assert {:ok, ^map} = AvroEx.decode(schema, encoded_array)
      end
    end

    test "encodes the count as a long" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
      {:ok, expected_count} = @test_module.encode(long_schema, 3)

      {:ok, <<actual_count::8, _rest::binary>>} =
        @test_module.encode(schema, %{"value1" => 1, "value2" => 2, "value3" => 3})

      assert expected_count == <<actual_count>>
    end

    test "encodes the key-value pair correctly" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
      {:ok, string_schema} = AvroEx.parse_schema(~S("string"))
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))
      {:ok, expected_key} = @test_module.encode(string_schema, "value1")
      {:ok, expected_value} = @test_module.encode(int_schema, 1)
      {:ok, <<_count::8, rest::binary>>} = @test_module.encode(schema, %{"value1" => 1})

      assert rest == expected_key <> expected_value
    end

    test "encodes an empty map" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "map", "values": "int"}))
      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
      {:ok, expected_count} = @test_module.encode(long_schema, 0)

      assert {:ok, <<actual_count::8>>} = @test_module.encode(schema, %{})
      assert expected_count == <<actual_count>>
    end
  end

  describe "encode (array)" do
    property "" do
      check all items <- list_of(integer()) do
        {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
        assert {:ok, encoded} = @test_module.encode(schema, items)
        assert {:ok, ^items} = AvroEx.decode(schema, encoded)
      end
    end

    test "encodes the count as a long" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
      {:ok, expected_count} = @test_module.encode(long_schema, 3)

      {:ok, <<actual_count::8, _rest::binary>>} = @test_module.encode(schema, [1, 2, 3])

      assert expected_count == <<actual_count>>
    end

    test "encodes the remaining values" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
      {:ok, int_schema} = AvroEx.parse_schema(~S("int"))
      items = [1, 2, 3]

      ints =
        items
        |> Enum.map(fn item ->
          {:ok, v} = @test_module.encode(int_schema, item)
          v
        end)
        |> Enum.join()

      assert {:ok, <<_count::8>> <> ^ints} = @test_module.encode(schema, items)
    end

    test "encodes an empty array" do
      {:ok, schema} = AvroEx.parse_schema(~S({"type": "array", "items": "int"}))
      {:ok, long_schema} = AvroEx.parse_schema(~S("long"))
      {:ok, expected_count} = @test_module.encode(long_schema, 0)

      assert {:ok, <<actual_count::8>>} = @test_module.encode(schema, [])
      assert expected_count == <<actual_count>>
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
        AvroEx.parse_schema(
          ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
        )

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

      assert {:error, :data_does_not_match_schema, :wat, _schema} =
               @test_module.encode(schema, :wat)
    end
  end

  def binary_of_size(size, bin \\ "")
  def binary_of_size(0, bin), do: bin
  def binary_of_size(size, bin), do: binary_of_size(size - 1, bin <> "a")
end
