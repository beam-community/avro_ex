defmodule AvroEx.Schema.Context.Test do
  use ExUnit.Case

  alias AvroEx.Schema.Record
  alias AvroEx.Schema.Record.Field

  @test_module AvroEx.Schema.Context

  describe "add_schema" do
    test "adds the fully qualified record name to the index" do
      record = %Record{name: "MyRecord", namespace: "me.cjpoll.avro_ex"}

      assert %@test_module{names: %{"me.cjpoll.avro_ex.MyRecord" => ^record}} =
        @test_module.add_schema(%@test_module{}, record)
    end

    test "adds the fully qualified aliases to the index" do
      record = %Record{name: "MyRecord", namespace: "me.cjpoll.avro_ex", aliases: ["TestRecord"]}

      assert %@test_module{names: %{
        "me.cjpoll.avro_ex.MyRecord" => ^record,
        "me.cjpoll.avro_ex.TestRecord" => ^record
      }} =
        @test_module.add_schema(%@test_module{}, record)
    end

    test "adds the fully qualified names of child records to the index" do
      record = %Record{
        name: "MyRecord",
        namespace: "me.cjpoll.avro_ex",
        aliases: ["TestRecord"],
        fields: [
          %Field{type: child = %Record{
            name: "ChildRecord",
            namespace: "me.cjpoll.avro_ex"
          }}
        ]
      }

      assert %@test_module{names: %{
        "me.cjpoll.avro_ex.MyRecord" => ^record,
        "me.cjpoll.avro_ex.TestRecord" => ^record,
        "me.cjpoll.avro_ex.ChildRecord" => ^child,
      }} =
        @test_module.add_schema(%@test_module{}, record)
    end
  end
end
