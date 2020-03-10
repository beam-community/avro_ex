defmodule AvroEx.Schema.Record.Test do
  use ExUnit.Case

  import AvroEx.Error

  alias AvroEx.Schema
  alias AvroEx.Schema.Primitive
  alias AvroEx.Schema.Record.Field

  @test_module AvroEx.Schema.Record

  @valid_record %{
    "type" => "record",
    "name" => "MyRecord",
    "namespace" => "me.cjpoll.avro_ex",
    "doc" => "An example Record for testing",
    "aliases" => ["TestRecord", "qrs.tuv.QualifiedAlias"],
    "fields" => [
      %{
        "name" => "FullName",
        "doc" => "A User's full name",
        "type" => "string",
        "default" => nil
      },
      %{"name" => "field2", "type" => "string"},
      %{"name" => "field3", "type" => %{"type" => "string"}},
      %{"name" => "field4", "type" => %{"type" => "record", "name" => "ChildRecord"}},
      %{"name" => "field5", "type" => "MyRecord"}
    ]
  }

  describe "changeset" do
    test "params are valid" do
      assert {:ok,
              %@test_module{
                fields: [
                  %Field{name: "FullName", default: nil},
                  %Field{name: "field2", type: %Primitive{type: :string}},
                  %Field{name: "field3", type: %Primitive{type: :string}},
                  %Field{name: "field4", type: %@test_module{name: "ChildRecord"}},
                  %Field{name: "field5", type: "MyRecord"}
                ]
              }} = @test_module.cast(@valid_record)
    end

    test "Errors if a name field is not specified" do
      schema = Map.delete(@valid_record, "name")

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
      schema = Map.delete(@valid_record, "namespace")

      assert {:ok, %@test_module{} = record} = @test_module.cast(schema)
      assert record.namespace == nil
    end

    test "Looks at doc if given" do
      {:ok, %@test_module{doc: "An example Record for testing"}} = @test_module.cast(@valid_record)
    end

    test "Does not require doc" do
      schema = Map.delete(@valid_record, "doc")

      {:ok, %@test_module{doc: nil}} = @test_module.cast(schema)
    end

    test "Allows alternate names (aliases) for the record" do
      {:ok, %@test_module{} = record} = @test_module.cast(@valid_record)
      assert "TestRecord" in record.aliases
      assert "qrs.tuv.QualifiedAlias" in record.aliases
    end

    test "Does not require aliases" do
      schema = Map.delete(@valid_record, "aliases")

      {:ok, %@test_module{aliases: []}} = @test_module.cast(schema)
    end
  end

  describe "fully qualified name" do
    setup do
      both = %{@valid_record | "name" => "SomeRecord", "namespace" => "me.cjpoll.avro_ex"}
      no_namespace = %{both | "namespace" => nil}

      prov_nprov = %{
        no_namespace
        | "namespace" => "abc.def",
          "fields" => [
            %{
              "name" => "field1",
              "type" => %{
                "type" => "record",
                "name" => "InnerRecord",
                "fields" => [%{"name" => "name", "type" => "string"}],
                "aliases" => ["xyz.QualifiedAlias"]
              }
            }
          ]
      }

      prov_prov = %{
        both
        | "namespace" => "abc.def",
          "fields" => [
            %{
              "name" => "field1",
              "type" => %{
                "type" => "record",
                "namespace" => "def.abc",
                "name" => "InnerRecord",
                "fields" => [%{"name" => "name", "type" => "string"}],
                "aliases" => ["xyz.QualifiedAlias"]
              }
            }
          ]
      }

      nprov_nprov = %{
        no_namespace
        | "fields" => [
            %{
              "name" => "field1",
              "type" => %{
                "type" => "record",
                "name" => "InnerRecord",
                "fields" => [%{"name" => "name", "type" => "string"}],
                "aliases" => ["xyz.QualifiedAlias"]
              }
            }
          ]
      }

      nprov_prov = %{
        no_namespace
        | "fields" => [
            %{
              "name" => "field1",
              "type" => %{
                "type" => "record",
                "namespace" => "def.abc",
                "name" => "InnerRecord",
                "fields" => [%{"name" => "name", "type" => "string"}],
                "aliases" => ["xyz.QualifiedAlias"]
              }
            }
          ]
      }

      state = %{
        both: Jason.encode!(both),
        no_namespace: Jason.encode!(no_namespace),
        prov_nprov: Jason.encode!(prov_nprov),
        nprov_nprov: Jason.encode!(nprov_nprov),
        nprov_prov: Jason.encode!(nprov_prov),
        prov_prov: Jason.encode!(prov_prov)
      }

      {:ok, state}
    end

    test "is correct if both are provided", %{both: record} do
      qualified_names =
        record
        |> Schema.parse!()
        |> Map.get(:schema)
        |> Map.get(:qualified_names)

      assert "me.cjpoll.avro_ex.SomeRecord" in qualified_names
    end

    @tag :now
    test "is correct if no namespace provided", %{no_namespace: record} do
      full_names =
        record
        |> Schema.parse!()
        |> Map.get(:schema)
        |> Map.get(:qualified_names)

      assert "SomeRecord" in full_names
      assert "TestRecord" in full_names
    end

    test "nested record, outer provided, inner not provided", %{prov_nprov: record} do
      record =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "abc.def.SomeRecord" in record.qualified_names
      assert "abc.def.TestRecord" in record.qualified_names

      [field | _] = record.fields

      assert "abc.def.InnerRecord" in field.type.qualified_names
    end

    test "nested record, outer not provided, inner provided", %{nprov_prov: record} do
      record =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "SomeRecord" in record.qualified_names

      [field | _] = record.fields

      assert "def.abc.InnerRecord" in field.type.qualified_names
    end

    test "nested record, outer provided, inner provided", %{prov_prov: record} do
      record =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "abc.def.SomeRecord" in record.qualified_names
      assert "abc.def.TestRecord" in record.qualified_names

      [field | _] = record.fields

      assert "def.abc.InnerRecord" in field.type.qualified_names
    end

    test "nested record, outer not provided, inner not provided", %{nprov_nprov: record} do
      record =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "SomeRecord" in record.qualified_names
      assert "TestRecord" in record.qualified_names

      [field | _] = record.fields

      assert "InnerRecord" in field.type.qualified_names
    end

    test "includes all qualified names in record", %{both: record} do
      record =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "me.cjpoll.avro_ex.SomeRecord" in record.qualified_names
      assert "me.cjpoll.avro_ex.TestRecord" in record.qualified_names
    end

    test "correctly gets a qualified alias from outer (namespace not provided)", %{
      nprov_nprov: record
    } do
      outer =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "qrs.tuv.QualifiedAlias" in outer.qualified_names
    end

    test "correctly gets a qualified alias from inner (namespace not provided)", %{
      nprov_nprov: record
    } do
      inner =
        record
        |> Schema.parse!()
        |> Map.get(:schema)
        |> Map.get(:fields)
        |> List.first()
        |> Map.get(:type)

      assert "xyz.QualifiedAlias" in inner.qualified_names
    end

    test "correctly gets a qualified alias from outer (namespace provided)", %{nprov_prov: record} do
      outer =
        record
        |> Schema.parse!()
        |> Map.get(:schema)

      assert "qrs.tuv.QualifiedAlias" in outer.qualified_names
    end

    test "correctly gets a qualified alias from inner (namespace provided)", %{nprov_prov: record} do
      inner =
        record
        |> Schema.parse!()
        |> Map.get(:schema)
        |> Map.get(:fields)
        |> List.first()
        |> Map.get(:type)

      assert "xyz.QualifiedAlias" in inner.qualified_names
    end
  end
end
