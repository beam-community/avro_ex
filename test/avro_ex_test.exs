defmodule AvroEx.Schema.Primitive.Test do
  use ExUnit.Case

  alias AvroEx.Schema
  alias AvroEx.Schema.{Primitive, Record, Record.Field, Reference, Union}

  doctest AvroEx

  describe "lookup" do
    test "looks up a named type" do
      schema_json = ~S(["null", {"type": "record", "namespace": "me.cjpoll", "name": "LinkedList", "fields": [
          {"type": "int", "name": "value"},
          {"type": ["null", "me.cjpoll.LinkedList"], "name": "next"}
        ]}])

      assert %Schema{
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
                             %Reference{type: "me.cjpoll.LinkedList" = type}
                           ]
                         }
                       }
                     ]
                   } = record
                 ]
               },
               context: context
             } = AvroEx.decode_schema!(schema_json)

      assert AvroEx.Schema.Context.lookup(context, type) == record
    end
  end

  describe "encode recursive" do
    test "can encode and decode a recursive type" do
      schema_json = ~S(["null", {"type": "record", "namespace": "me.cjpoll", "name": "LinkedList", "fields": [
          {"type": "int", "name": "value"},
          {"type": ["null", "me.cjpoll.LinkedList"], "name": "next"}
        ]}])

      assert %Schema{
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
                             %Reference{type: "me.cjpoll.LinkedList"}
                           ]
                         }
                       }
                     ]
                   }
                 ]
               },
               context: context
             } = schema = AvroEx.decode_schema!(schema_json)

      data = %{
        "value" => 25,
        "next" => %{"value" => 23, "next" => %{"value" => 20, "next" => nil}}
      }

      assert context ==
               %AvroEx.Schema.Context{
                 names: %{
                   "me.cjpoll.LinkedList" => %AvroEx.Schema.Record{
                     fields: [
                       %AvroEx.Schema.Record.Field{
                         name: "value",
                         type: %AvroEx.Schema.Primitive{type: :int}
                       },
                       %AvroEx.Schema.Record.Field{
                         name: "next",
                         type: %AvroEx.Schema.Union{
                           possibilities: [
                             %AvroEx.Schema.Primitive{type: :null},
                             %AvroEx.Schema.Reference{type: "me.cjpoll.LinkedList"}
                           ]
                         }
                       }
                     ],
                     name: "LinkedList",
                     namespace: "me.cjpoll"
                   }
                 }
               }

      assert {:ok, avro} = AvroEx.encode(schema, data)
      assert {:ok, ^data} = AvroEx.decode(schema, avro)
    end
  end
end
