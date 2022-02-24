defmodule AvroEx.Schema.ParserTest do
  use ExUnit.Case

  alias AvroEx.{Schema}
  alias AvroEx.Schema.{Array, Context, Fixed, Parser, Primitive, Record, Union}

  describe "primitives" do
    test "it can parse primitives" do
      for p <- Parser.primitives() do
        p_atom = String.to_atom(p)
        assert %Schema{schema: schema, context: context} = Parser.parse!(p)

        assert %Primitive{type: ^p_atom} = schema
        assert context == %Context{names: %{}}
      end
    end

    test "it can parse complex primitives" do
      for p <- Parser.primitives() do
        p_atom = String.to_atom(p)
        assert %Schema{schema: schema, context: context} = Parser.parse!(%{"type" => p})

        assert %Primitive{type: ^p_atom} = schema
        assert context == %Context{names: %{}}
      end
    end

    test "it can parse complex primitives with additional fields" do
      for p <- Parser.primitives() do
        p_atom = String.to_atom(p)

        assert %Schema{schema: schema, context: context} =
                 Parser.parse!(%{"type" => p, "a" => 1, "logicalType" => "timestamp-millis", "name" => "complex"})

        assert %Primitive{
                 type: ^p_atom,
                 metadata: %{"a" => 1, "logicalType" => "timestamp-millis", "name" => "complex"}
               } = schema

        assert context == %Context{names: %{}}
      end
    end

    test "invalid primitives raise a DecodeError" do
      assert_raise AvroEx.Schema.DecodeError, "Failed to decode schema for :invalid_format data=\"nope\"", fn ->
        Parser.parse!("nope")
      end

      message = "Failed to decode schema for :invalid_format data=%{\"type\" => \"nada\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{"type" => "nada"})
      end
    end
  end

  describe "records" do
    test "it can decode simple records" do
      assert %Schema{schema: schema, context: context} =
               Parser.parse!(%{
                 "type" => "record",
                 "name" => "kyc",
                 "fields" => [
                   %{"name" => "first", "type" => "string"},
                   %{"name" => "last", "type" => "string"}
                 ]
               })

      assert schema == %Record{
               name: "kyc",
               fields: [
                 %Record.Field{name: "first", type: %Primitive{type: :string}},
                 %Record.Field{name: "last", type: %Primitive{type: :string}}
               ]
             }
    end
  end
end
