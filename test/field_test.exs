defmodule AvroEx.Schema.Record.Field.Test do
  use ExUnit.Case
  import AvroEx.Schema, only: [errors: 2, error: 1]
  alias Ecto.Changeset

  @test_module AvroEx.Schema.Record.Field

  describe "changeset" do
    @schema %{
      "name" => "name",
      "doc" => "a test field",
      "type" => "string",
      "default" => nil
    }

    test "Errors if a type is not specified" do
      schema = Map.delete(@schema, "type")
      cs = @test_module.changeset(%@test_module{}, schema)
      refute cs.valid?
      assert error("can't be blank") in errors(cs, :type)
    end

    test "Errors if a name is not specified" do
      schema = Map.delete(@schema, "name")
      cs = @test_module.changeset(%@test_module{}, schema)
      refute cs.valid?
      assert error("can't be blank") in errors(cs, :name)
    end

    test "Looks at doc if given" do
      cs = @test_module.changeset(%@test_module{}, @schema)
      assert cs.valid?
      assert Changeset.get_field(cs, :doc) == @schema["doc"]
    end

    test "Does not require doc" do
      schema = Map.delete(@schema, "doc")
      cs = @test_module.changeset(%@test_module{}, schema)
      assert cs.valid?
      assert Changeset.get_field(cs, :doc) == nil
    end

    test "Looks at default if given" do
      cs = @test_module.changeset(%@test_module{}, @schema)
      assert cs.valid?
      assert Changeset.get_field(cs, :default) == @schema["default"]
    end

    test "Does not require default" do
      schema = Map.delete(@schema, "default")
      cs = @test_module.changeset(%@test_module{}, schema)
      assert cs.valid?
      assert Changeset.get_field(cs, :default) == nil
    end
  end
end
