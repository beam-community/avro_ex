defmodule AvroEx.Schema.Primitive.Test do
  use ExUnit.Case

  @test_module AvroEx.Schema.Primitive

  describe "string" do
    test "simple string" do
      assert {:ok, %@test_module{type: :string}} = @test_module.cast("string")
    end

    test "complex string" do
      assert {:ok, %@test_module{type: :string, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "string", "some" => "metadata"})
    end
  end

  describe "null" do
    test "simple null" do
      assert {:ok, %@test_module{type: :null}} = @test_module.cast("null")
    end

    test "complex null" do
      assert {:ok, %@test_module{type: :null, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "null", "some" => "metadata"})
    end
  end

  describe "boolean" do
    test "simple boolean" do
      assert {:ok, %@test_module{type: :boolean}} = @test_module.cast("boolean")
    end

    test "complex boolean" do
      assert {:ok, %@test_module{type: :boolean, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "boolean", "some" => "metadata"})
    end
  end

  describe "int" do
    test "simple integer" do
      assert {:ok, %@test_module{type: :int}} = @test_module.cast("int")
    end

    test "complex integer" do
      assert {:ok, %@test_module{type: :int, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "int", "some" => "metadata"})
    end
  end

  describe "long" do
    test "simple long" do
      assert {:ok, %@test_module{type: :long}} = @test_module.cast("long")
    end

    test "complex long" do
      assert {:ok, %@test_module{type: :long, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "long", "some" => "metadata"})
    end
  end

  describe "float" do
    test "simple float" do
      assert {:ok, %@test_module{type: :float}} = @test_module.cast("float")
    end

    test "complex float" do
      assert {:ok, %@test_module{type: :float, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "float", "some" => "metadata"})
    end
  end

  describe "double" do
    test "simple double" do
      assert {:ok, %@test_module{type: :double}} = @test_module.cast("double")
    end

    test "complex double" do
      assert {:ok, %@test_module{type: :double, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "double", "some" => "metadata"})
    end
  end

  describe "bytes" do
    test "simple bytes" do
      assert {:ok, %@test_module{type: :bytes}} = @test_module.cast("bytes")
    end

    test "complex bytes" do
      assert {:ok, %@test_module{type: :bytes, metadata: %{"some" => "metadata"}}} =
               @test_module.cast(%{"type" => "bytes", "some" => "metadata"})
    end
  end

  alias AvroEx.Schema
  alias AvroEx.Schema.{Primitive, Record, Record.Field, Union}

  describe "lookup" do
    test "looks up a named type" do
      schema_json = ~S(["null", {"type": "record", "namespace": "me.cjpoll", "name": "LinkedList", "fields": [
          {"type": "int", "name": "value"},
          {"type": ["null", "me.cjpoll.LinkedList"], "name": "next"}
        ]}])

      assert {:ok,
              %Schema{
                schema: %Union{
                  possibilities: [
                    %Primitive{type: :null},
                    %Record{
                      name: "LinkedList",
                      fields: [
                        %Field{name: "value", type: %Primitive{type: :int}},
                        %Field{
                          name: "next",
                          type: %Union{
                            possibilities: [
                              %Primitive{type: :null},
                              "me.cjpoll.LinkedList" = type
                            ]
                          }
                        }
                      ]
                    } = record
                  ]
                }
              } = schema} = AvroEx.decode_schema(schema_json)

      assert AvroEx.named_type(type, schema) == record
    end
  end

  describe "encode recursive" do
    test "can encode and decode a recursive type" do
      schema_json = ~S(["null", {"type": "record", "namespace": "me.cjpoll", "name": "LinkedList", "fields": [
          {"type": "int", "name": "value"},
          {"type": ["null", "me.cjpoll.LinkedList"], "name": "next"}
        ]}])

      assert {:ok,
              %Schema{
                schema: %Union{
                  possibilities: [
                    %Primitive{type: :null},
                    %Record{
                      name: "LinkedList",
                      fields: [
                        %Field{name: "value", type: %Primitive{type: :int}},
                        %Field{
                          name: "next",
                          type: %Union{
                            possibilities: [
                              %Primitive{type: :null},
                              "me.cjpoll.LinkedList"
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
              } = schema} = AvroEx.decode_schema(schema_json)

      data = %{
        "value" => 25,
        "next" => %{"value" => 23, "next" => %{"value" => 20, "next" => nil}}
      }

      assert {:ok, avro} = AvroEx.encode(schema, data)
      assert {:ok, ^data} = AvroEx.decode(schema, avro)
    end
  end
end
