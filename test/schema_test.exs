defmodule AvroEx.Schema.Test do
  use ExUnit.Case

  require __MODULE__.Macros
  import __MODULE__.Macros

  alias AvroEx.Schema
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Map, as: AvroMap
  alias AvroEx.Schema.Record.Field
  alias AvroEx.Schema.{Array, Context, Fixed, Primitive, Record, Union}

  doctest AvroEx.Schema, import: true

  @test_module AvroEx.Schema

  @spec json_add_property(binary | map, atom | binary, any) :: map | binary
  def json_add_property(str, property, value) when is_binary(str) do
    str
    |> Jason.decode!()
    |> json_add_property(property, value)
    |> Jason.encode!()
  end

  def json_add_property(json, property, value) when is_map(json) and is_atom(property) do
    json_add_property(json, Atom.to_string(property), value)
  end

  def json_add_property(json, property, value) when is_map(json) and is_binary(property) do
    Map.update(json, property, value, fn _ -> value end)
  end

  @json ~S"""
    {
      "type": "record",
      "name": "MyRecord",
      "namespace": "me.cjpoll.avro_ex",
      "aliases": ["OldRecord", "SomeRecord"],
      "doc": "A record for testing",
      "fields": [
        {
          "type": "long",
          "name": "field3",
          "doc": "some field",
          "aliases": ["field1", "field2"]
        },
        {
          "type": {
            "type": "record",
            "name": "ChildRecord",
            "aliases": ["InnerRecord"],
            "fields": []
          },
          "name": "field6",
          "doc": "some field",
          "aliases": ["field4", "field5"]
        }
      ]
    }
  """

  describe "parse record" do
    @schema AvroEx.Schema.Record

    test "works" do
      child_record = %@schema{
        name: "ChildRecord",
        aliases: ["InnerRecord"],
        fields: []
      }

      parent = %@schema{
        aliases: ["OldRecord", "SomeRecord"],
        doc: "A record for testing",
        name: "MyRecord",
        namespace: "me.cjpoll.avro_ex",
        fields: [
          %Field{
            type: %Primitive{
              type: :long,
              metadata: %{}
            },
            name: "field3",
            doc: "some field",
            aliases: ["field1", "field2"]
          },
          %Field{
            type: child_record,
            name: "field6",
            doc: "some field",
            aliases: ["field4", "field5"]
          }
        ]
      }

      context = %Context{
        names: %{
          "me.cjpoll.avro_ex.OldRecord" => parent,
          "me.cjpoll.avro_ex.SomeRecord" => parent,
          "me.cjpoll.avro_ex.MyRecord" => parent,
          "me.cjpoll.avro_ex.ChildRecord" => child_record,
          "me.cjpoll.avro_ex.InnerRecord" => child_record
        }
      }

      {:ok, %@test_module{} = schema} = AvroEx.decode_schema(@json)

      assert parent == schema.schema
      assert context == schema.context
    end

    handles_metadata()
  end

  describe "parse union" do
    test "primitives" do
      assert {:ok,
              %AvroEx.Schema{
                schema: %Union{
                  possibilities: [
                    %Primitive{type: :null},
                    %Primitive{type: :int}
                  ]
                }
              }} = AvroEx.decode_schema(~S(["null", "int"]))
    end

    test "record in union" do
      child_record = %Record{
        name: "ChildRecord",
        aliases: ["InnerRecord"]
      }

      parent = %Record{
        aliases: ["OldRecord", "SomeRecord"],
        doc: "A record for testing",
        name: "MyRecord",
        namespace: "me.cjpoll.avro_ex",
        fields: [
          %Field{
            type: %Primitive{
              type: :long,
              metadata: %{}
            },
            name: "field3",
            doc: "some field",
            aliases: ["field1", "field2"]
          },
          %Field{
            type: child_record,
            name: "field6",
            doc: "some field",
            aliases: ["field4", "field5"]
          }
        ]
      }

      context = %Context{
        names: %{
          "me.cjpoll.avro_ex.OldRecord" => parent,
          "me.cjpoll.avro_ex.SomeRecord" => parent,
          "me.cjpoll.avro_ex.MyRecord" => parent,
          "me.cjpoll.avro_ex.ChildRecord" => child_record,
          "me.cjpoll.avro_ex.InnerRecord" => child_record
        }
      }

      {:ok, %AvroEx.Schema{} = schema} = AvroEx.decode_schema(~s(["null", #{@json}]))

      assert ^context = schema.context

      assert %Union{
               possibilities: [
                 %Primitive{type: :null},
                 ^parent
               ]
             } = schema.schema
    end

    test "union in record" do
      schema = ~S"""
      {"type": "record", "name": "arecord", "fields": [
        {"type": ["null", "int"], "name": "a"}
      ]}
      """

      assert {:ok,
              %AvroEx.Schema{
                schema: %Record{
                  fields: [
                    %Field{
                      name: "a",
                      type: %Union{
                        possibilities: [
                          %Primitive{type: :null},
                          %Primitive{type: :int}
                        ]
                      }
                    }
                  ]
                }
              }} = AvroEx.decode_schema(schema)
    end
  end

  describe "parse map" do
    @json ~S({"type": "map", "values": "int"})
    @schema AvroMap

    handles_metadata()

    test "doesn't blow up" do
      assert {:ok,
              %@test_module{
                schema: %@schema{}
              }} = AvroEx.decode_schema(@json)
    end

    test "matches the given type" do
      assert {:ok, %@test_module{schema: %@schema{values: %Primitive{type: :int}}}} = AvroEx.decode_schema(@json)
    end

    test "works with a union" do
      assert {:ok,
              %@test_module{
                schema: %@schema{
                  values: %Union{
                    possibilities: [
                      %Primitive{type: :null},
                      %Primitive{type: :int}
                    ]
                  }
                }
              }} =
               @json
               |> json_add_property(:values, ["null", "int"])
               |> AvroEx.decode_schema()
    end
  end

  describe "parse array" do
    @json ~S({"type": "array", "items": "int"})
    @schema Array
    test "doesn't blow up" do
      assert {:ok, %@test_module{schema: %@schema{}}} = AvroEx.decode_schema(@json)
    end

    handles_metadata()
  end

  describe "encodable? (primitive)" do
    @values %{
      "null" => nil,
      "boolean" => false,
      "int" => 1,
      "long" => 1,
      "float" => 1.0,
      "double" => 1.0,
      "bytes" => "12345",
      "string" => "12345"
    }

    for a <- @values,
        b <- @values do
      test "#{inspect(a)} vs #{inspect(b)}" do
        {ka, va} = unquote(a)
        {_kb, vb} = unquote(b)
        {:ok, schema} = AvroEx.decode_schema(~s(#{inspect(ka)}))

        assert @test_module.encodable?(schema, va)
        assert @test_module.encodable?(schema, vb) == (va === vb)
      end
    end

    test "accepts atoms as strings" do
      {:ok, schema} = AvroEx.decode_schema(~S("string"))
      assert @test_module.encodable?(schema, :dave)
      refute @test_module.encodable?(schema, nil)
    end

    test "does not accept non-utf8 strings as string" do
      {:ok, schema} = AvroEx.decode_schema(~S("string"))
      refute @test_module.encodable?(schema, <<128>>)
    end

    test "does accept non-utf8 binaries as bytes" do
      {:ok, schema} = AvroEx.decode_schema(~S("bytes"))
      assert @test_module.encodable?(schema, <<128>>)
    end

    test "does not accept non-binary bitstrings as string" do
      {:ok, schema} = AvroEx.decode_schema(~S("string"))
      refute @test_module.encodable?(schema, <<0::7>>)
    end

    test "does not accept non-binary bitstrings as bytes" do
      {:ok, schema} = AvroEx.decode_schema(~S("bytes"))
      refute @test_module.encodable?(schema, <<0::7>>)
    end
  end

  describe "parse (enum)" do
    test "doesn't blow up" do
      assert {:ok, _enum_schema} =
               AvroEx.decode_schema(
                 ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
               )
    end

    test "returns an Enum struct" do
      assert {:ok, %Schema{schema: %AvroEnum{}}} =
               AvroEx.decode_schema(
                 ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
               )
    end

    test "fails if the symbols aren't all strings" do
      assert {:ok, %Schema{schema: %AvroEnum{}}} =
               AvroEx.decode_schema(
                 ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
               )
    end
  end

  describe "encodable? (record: non-nested)" do
    setup do
      schema = """
      {
        "type": "record",
        "name": "Person",
        "fields": [
          {"name": "first_name", "type": "string"},
          {"name": "age", "type": "int"}
        ]
      }
      """

      {:ok, parsed_schema} = AvroEx.decode_schema(schema)
      {:ok, %{schema: parsed_schema}}
    end

    test "can be encoded with a map", %{schema: schema} do
      assert @test_module.encodable?(schema, %{"first_name" => "Cody", "age" => 30})
    end

    test "can not be encoded with a proplist", %{schema: schema} do
      refute @test_module.encodable?(schema, [{"first_name", "Cody"}, {"age", 30}])
    end

    test "checks that the value typings match", %{schema: schema} do
      refute @test_module.encodable?(schema, %{"first_name" => "Cody", "age" => "Cody"})
      refute @test_module.encodable?(schema, %{"first_name" => 30, "age" => 30})
      refute @test_module.encodable?(schema, %{"first_name" => "Cody", "ages" => 30})
    end

    test "records can have keys as atoms", %{schema: schema} do
      assert @test_module.encodable?(schema, %{first_name: "Dave", age: 32})
    end
  end

  describe "encodable? (record: nested)" do
    setup do
      schema = """
      {
        "type": "record",
        "name": "Person",
        "fields": [
          {"name": "first_name", "type": "string"},
          {"name": "age", "type": "int"},
          {
            "name": "thing",
            "type":{
              "type": "record",
              "name": "Thing",
              "fields": [
                {"name": "some_field", "type": "null"}
              ]
            }
          }
        ]
      }
      """

      {:ok, parsed_schema} = AvroEx.decode_schema(schema)
      {:ok, %{schema: parsed_schema}}
    end

    test "works as expected", %{schema: schema} do
      data = %{"first_name" => "Cody", "age" => 30, "thing" => %{"some_field" => nil}}

      assert @test_module.encodable?(schema, data)
      refute @test_module.encodable?(schema, %{"first_name" => "Cody", "age" => 30})
    end

    test "checks typing on child records", %{schema: schema} do
      data = %{"first_name" => "Cody", "age" => 30, "thing" => %{"some_field" => 1}}
      refute @test_module.encodable?(schema, data)
    end
  end

  describe "encodable? (record: named)" do
    setup do
      schema = """
      {
        "type": "record",
        "name": "LinkedList",
        "fields": [
          {"name": "value", "type": "int"},
          {"name": "next", "type": ["null", "LinkedList"]}
        ]
      }
      """

      {:ok, parsed_schema} = AvroEx.decode_schema(schema)
      {:ok, %{schema: parsed_schema}}
    end

    test "works with a named type", %{schema: schema} do
      assert @test_module.encodable?(schema, %{
               "value" => 1,
               "next" => %{"value" => 2, "next" => nil}
             })
    end
  end

  describe "encodable? (union)" do
    test "works as expected" do
      {:ok, schema} = AvroEx.decode_schema(~S(["null", "string", "int"]))

      assert @test_module.encodable?(schema, nil)
      assert @test_module.encodable?(schema, "hello")
      assert @test_module.encodable?(schema, 25)

      refute @test_module.encodable?(schema, 25.1)
      refute @test_module.encodable?(schema, true)
      refute @test_module.encodable?(schema, %{"Hello" => "world"})
    end

    test "works with logical types" do
      {:ok, schema} = AvroEx.decode_schema(~S(["null", {"type": "long", "logicalType":"timestamp-millis"}]))

      assert @test_module.encodable?(schema, nil)
      assert @test_module.encodable?(schema, DateTime.utc_now())
      assert @test_module.encodable?(schema, 1_525_658_987)

      refute @test_module.encodable?(schema, 1.5)
      refute @test_module.encodable?(schema, "AvroEx")
      refute @test_module.encodable?(schema, Time.utc_now())
    end
  end

  describe "encodable? (map)" do
    test "works as expected" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert @test_module.encodable?(schema, %{"value" => 1, "value2" => 2, "value3" => 3})
    end

    test "fails if key is not a string" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      refute @test_module.encodable?(schema, %{1 => 1})
    end

    test "fails if value does not match type" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      refute @test_module.encodable?(schema, %{"value" => 1.1})
    end

    test "fails if one value does not match type" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      refute @test_module.encodable?(schema, %{"value" => 11, "value2" => 12, "value3" => 1.1})
    end

    test "works with a union" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": ["null", "int"]}))
      assert @test_module.encodable?(schema, %{"value" => 1, "value2" => 2, "value3" => nil})
      refute @test_module.encodable?(schema, %{"value" => 1, "value2" => 2.1, "value3" => nil})
    end

    test "works with an empty map" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert @test_module.encodable?(schema, %{})
    end

    test "maps can have atom keys" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "map", "values": "int"}))
      assert @test_module.encodable?(schema, %{a: 1, b: 2})
    end
  end

  describe "encodable? (array)" do
    test "works as expected" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      assert @test_module.encodable?(schema, [1, 2, 3, 4, 5])
    end

    test "fails if item does not match type" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      refute @test_module.encodable?(schema, [1.1])
    end

    test "fails if one item does not match type" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      refute @test_module.encodable?(schema, [1, 2, 3, 4.5, 6])
    end

    test "works with a union" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": ["null", "int"]}))
      assert @test_module.encodable?(schema, [1, 2, nil, 3, 4, nil, 5])
      assert @test_module.encodable?(schema, [nil, 2, nil, 3, 4, nil, 5])
      refute @test_module.encodable?(schema, [1, 2.1, nil])
    end

    test "works with an empty array" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "array", "items": "int"}))
      assert @test_module.encodable?(schema, [])
    end
  end

  describe "encodable? (fixed)" do
    test "works as expected" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "fixed", "name": "SHA", "size": 40}))
      assert @test_module.encodable?(schema, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    end

    test "fails if size is too small" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "fixed", "size": 40, "name": "SHA"}))
      refute @test_module.encodable?(schema, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    end

    test "fails if size is too large" do
      {:ok, schema} = AvroEx.decode_schema(~S({"type": "fixed", "size": 40, "name": "SHA"}))
      refute @test_module.encodable?(schema, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    end
  end

  describe "encodable? (enum)" do
    test "works as expected" do
      {:ok, schema} =
        AvroEx.decode_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      assert @test_module.encodable?(schema, "heart")
    end

    test "fails if string is not in symbols" do
      {:ok, schema} =
        AvroEx.decode_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      refute @test_module.encodable?(schema, "kkjasdfkasdfj")
    end

    test "enums can have atoms" do
      {:ok, schema} =
        AvroEx.decode_schema(~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]}))

      assert @test_module.encodable?(schema, :heart)
    end
  end
end
