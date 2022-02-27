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
                 Parser.parse!(%{
                   "type" => p,
                   "a" => 1,
                   "logicalType" => "timestamp-millis",
                   "name" => "complex"
                 })

        assert %Primitive{
                 type: ^p_atom,
                 metadata: %{"a" => 1, "logicalType" => "timestamp-millis", "name" => "complex"}
               } = schema

        assert context == %Context{names: %{}}
      end
    end

    test "invalid primitives raise a DecodeError" do
      assert_raise AvroEx.Schema.DecodeError,
                   "Invalid schema format \"nope\"",
                   fn ->
                     Parser.parse!("nope")
                   end

      message = "Invalid schema format %{\"type\" => \"nada\"}"

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

      # TODO figure out context
      # assert context == %Context{}
    end

    test "records can have fields that are logicalTypes" do
      assert %Schema{schema: schema, context: context} =
               Parser.parse!(%{
                 "type" => "record",
                 "name" => "analytics",
                 "fields" => [
                   %{
                     "name" => "timestamp",
                     "type" => %{"type" => "string", "logicalType" => "timestamp-millis"}
                   }
                 ]
               })

      assert schema == %Record{
               name: "analytics",
               fields: [
                 %Record.Field{
                   name: "timestamp",
                   type: %Primitive{type: :string, metadata: %{"logicalType" => "timestamp-millis"}}
                 }
               ]
             }

      # TODO figure out context
      # assert context == %Context{}
    end

    test "creating a record without a name will raise" do
      message =
        "Schema missing required key `name` in %{\"fields\" => [%{\"name\" => \"key\", \"type\" => \"long\"}], \"type\" => \"record\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "record",
          "fields" => [
            %{"name" => "key", "type" => "long"}
          ]
        })
      end
    end

    test "trying to use a logicalType on a field will raise" do
      message =
        "Unrecognized schema key `logicalType` for AvroEx.Schema.Record.Field in %{\"logicalType\" => \"timestamp-millis\", \"name\" => \"timestamp\", \"type\" => \"long\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "record",
          "name" => "analytics",
          "fields" => [
            %{"name" => "timestamp", "type" => "long", "logicalType" => "timestamp-millis"}
          ]
        })
      end
    end

    test "aliases" do
      flunk()
    end
  end
end
