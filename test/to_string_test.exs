defmodule AvroEx.ToStringTest do
  use ExUnit.Case

  alias AvroEx.Schema
  alias AvroEx.Schema.Array
  alias AvroEx.Schema.Enum, as: AvroEnum
  alias AvroEx.Schema.Fixed
  alias AvroEx.Schema.Primitive

  describe "primitive" do
    @primitives [
      {"null", "null"},
      "boolean",
      "int",
      "long",
      "float",
      "double",
      "bytes",
      "string"
    ]
    test "can be to_string" do
      for t <- @primitives do
        {:ok, type} = Primitive.cast(t)
        assert to_string(type) == t
      end
    end
  end

  describe "enum" do
    test "uses the name" do
      assert %Schema{schema: %AvroEnum{} = enum} =
               AvroEx.parse_schema!(~S({"type": "enum", "name": "Cool", "symbols": ["a", "b"]}))

      assert to_string(enum) == "Enum(Cool)"
    end

    test "includes the namespace if one is available" do
      assert %Schema{schema: %AvroEnum{} = enum} =
               AvroEx.parse_schema!(
                 ~S({"type": "enum", "namespace": "beam.community", "name": "Cool", "symbols": ["a", "b"]})
               )

      assert to_string(enum) == "Enum(beam.community.Cool)"
    end
  end

  describe "fixed" do
    test "shows the size" do
      assert %Schema{schema: %Fixed{} = fixed} =
               AvroEx.parse_schema!(~S({"type": "fixed", "name": "toofer", "size": "2"}))

      assert to_string(fixed) == "Fixed2(toofer)"
    end

    test "includes the namespace if one is available" do
      assert %Schema{schema: %Fixed{} = fixed} =
               AvroEx.parse_schema!(~S({"type": "fixed", "namespace": "beam.community", "name": "quad", "size": "4"}))

      assert to_string(fixed) == "Fixed4(beam.community.quad)"
    end
  end
end
