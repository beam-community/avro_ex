defmodule AvroEx.Schema.Record.Test do
  use ExUnit.Case
  import AvroEx.Error
  alias AvroEx.Schema.Primitive
  alias AvroEx.Schema.Record.Field

  @test_module AvroEx.Schema.Record

  @valid_record %{
    "type" => "record",
    "name" => "MyRecord",
    "namespace" => "me.cjpoll.avro_ex",
    "doc" => "An example Record for testing",
    "aliases" => ["TestRecord"],
    "fields" => [
      %{
        "name" => "FullName",
        "doc" => "A User's full name",
        "type" => "string",
        "default" => nil,
      },
      %{"name" => "field2", "type"  => "string"},
      %{"name" => "field3", "type"  => %{"type" => "string"}},
      %{"name" => "field4", "type"  => %{"type" => "record", "name" => "ChildRecord"}},
      %{"name" => "field5", "type"  => "MyRecord"}
    ]
  }

  describe "changeset" do
    test "params are valid" do
      assert {:ok, %@test_module{
        fields: [
          %Field{name: "FullName", default: nil},
          %Field{name: "field2", type: %Primitive{type: :string}},
          %Field{name: "field3", type: %Primitive{type: :string}},
          %Field{name: "field4", type: %@test_module{name: "ChildRecord"}},
          %Field{name: "field5", type: "MyRecord"}
        ]}} =
          @test_module.cast(@valid_record)
    end

    test "Errors if a name field is not specified" do
      schema = @valid_record |> Map.delete("name")

      assert {:error, errors} = @test_module.cast(schema)
      assert error("can't be blank") in errors.name
    end

    test "returns a Record if successful" do
      {:ok, %@test_module{}} = @test_module.cast(@valid_record)
    end

    test "Looks at the namespace if given" do
      {:ok, %@test_module{namespace: "me.cjpoll.avro_ex"}}
    end

    test "Does not require a namespace" do
      schema = @valid_record |> Map.delete("namespace")

      assert {:ok, %@test_module{} = record} = @test_module.cast(schema)
      assert record.namespace == nil
    end

    test "Looks at doc if given" do
      {:ok, %@test_module{doc: "An example Record for testing"}} = @test_module.cast(@valid_record)
    end

    test "Does not require doc" do
      schema = @valid_record |> Map.delete("doc")

      {:ok, %@test_module{doc: nil}} = @test_module.cast(schema)
    end

    test "Allows alternate names (aliases) for the record" do
      {:ok, %@test_module{aliases: ["TestRecord"]}} = @test_module.cast(@valid_record)
    end

    test "Does not require aliases" do
      schema = @valid_record |> Map.delete("aliases")

      {:ok, %@test_module{aliases: []}} = @test_module.cast(schema)
    end
  end

  describe "fully qualified name" do
    setup do
      both = %@test_module{namespace: "me.cjpoll.avro_ex", name: "SomeRecord", aliases: ["MyRecord"]}
      no_namespace = %@test_module{both | namespace: nil}

      state = %{
        both: both,
        no_namespace: no_namespace
      }

      {:ok, state}
    end

    test "is correct if both are provided", %{both: record} do
      "me.cjpoll.avro_ex.SomeRecord" = @test_module.full_name(record)
    end

    test "is correct if no namespace provided", %{no_namespace: record} do
      "SomeRecord" = @test_module.full_name(record)
    end

    test "can get all qualified names for record", %{both: record} do
      ["me.cjpoll.avro_ex.SomeRecord", "me.cjpoll.avro_ex.MyRecord"] = @test_module.full_names(record)
    end
  end
end
