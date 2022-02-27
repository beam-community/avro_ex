defmodule AvroEx.Schema.ParserTest do
  use ExUnit.Case

  alias AvroEx.{Schema}
  alias AvroEx.Schema.{Array, Context, Fixed, Parser, Primitive, Record, Union}
  alias AvroEx.Schema.Enum, as: AvroEnum

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
        "Schema missing required key `name` for AvroEx.Schema.Record in %{\"fields\" => [%{\"name\" => \"key\", \"type\" => \"long\"}], \"type\" => \"record\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "record",
          "fields" => [
            %{"name" => "key", "type" => "long"}
          ]
        })
      end
    end

    test "names must be valid" do
      message =
        "Invalid name `123` for `name` in %{\"fields\" => [%{\"name\" => \"key\", \"type\" => \"long\"}], \"name\" => \"123\", \"type\" => \"record\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "record",
          "name" => "123",
          "fields" => [
            %{"name" => "key", "type" => "long"}
          ]
        })
      end
    end

    test "namespace must be valid" do
      message =
        "Invalid name `1invalid` for `namespace` in %{\"fields\" => [%{\"name\" => \"key\", \"type\" => \"long\"}], \"name\" => \"valid\", \"namespace\" => \"1invalid\", \"type\" => \"record\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "record",
          "name" => "valid",
          "namespace" => "1invalid",
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

  describe "unions" do
    test "it can decode simple unions" do
      assert %Schema{schema: schema, context: context} = Parser.parse!(["null", "string"])

      assert schema == %Union{
               possibilities: [
                 %Primitive{type: :null},
                 %Primitive{type: :string}
               ]
             }

      assert context == %Context{}
    end

    test "unions cannot have duplicated unnamed types" do
      message =
        "Union conains duplicated %AvroEx.Schema.Primitive{metadata: %{}, type: :string} in [\"string\", \"int\", \"string\"]"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(["string", "int", "string"])
      end
    end

    test "unions can contain duplicated types if they are named" do
      assert %Schema{schema: schema, context: context} =
               Parser.parse!([
                 %{"type" => "enum", "name" => "directions", "symbols" => ["east", "north", "south", "west"]},
                 %{"type" => "enum", "name" => "primary_colors", "symbols" => ["blue", "red", "yellow"]}
               ])

      assert schema == %Union{
               possibilities: [
                 %AvroEnum{name: "directions", symbols: ["east", "north", "south", "west"]},
                 %AvroEnum{name: "primary_colors", symbols: ["blue", "red", "yellow"]}
               ]
             }

      assert context == %Context{}
    end

    test "unions cannot have unions as direct children" do
      message =
        "Union contains nested union %AvroEx.Schema.Union{possibilities: [%AvroEx.Schema.Primitive{metadata: %{}, type: :null}, %AvroEx.Schema.Primitive{metadata: %{}, type: :string}]} as immediate child in [\"string\", [\"null\", \"string\"]]"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(["string", ["null", "string"]])
      end
    end
  end

  describe "enums" do
    test "can parse a basic enum" do
      assert %Schema{schema: schema, context: context} =
               Parser.parse!(%{
                 "type" => "enum",
                 "name" => "directions",
                 "symbols" => ["east", "north", "south", "west"]
               })

      assert schema == %AvroEnum{name: "directions", symbols: ["east", "north", "south", "west"]}
    end

    test "cannot have duplicate symbols" do
      message =
        "Enum conains duplicated symbol `yes` in %{\"name\" => \"duplicate\", \"symbols\" => [\"yes\", \"no\", \"yes\"], \"type\" => \"enum\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "enum",
          "name" => "duplicate",
          "symbols" => ["yes", "no", "yes"]
        })
      end
    end

    test "symbols must by alphanumberic or underscores, and not start with a number" do
      message =
        "Invalid name `1` for `symbols` in %{\"name\" => \"non_string\", \"symbols\" => [1], \"type\" => \"enum\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "enum",
          "name" => "non_string",
          "symbols" => [1]
        })
      end

      message =
        "Invalid name `abcABC!` for `symbols` in %{\"name\" => \"bad_name_1\", \"symbols\" => [\"abcABC!\"], \"type\" => \"enum\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "enum",
          "name" => "bad_name_1",
          "symbols" => ["abcABC!"]
        })
      end

      message =
        "Invalid name `1a` for `symbols` in %{\"name\" => \"bad_name_2\", \"symbols\" => [\"1a\"], \"type\" => \"enum\"}"

      assert_raise AvroEx.Schema.DecodeError, message, fn ->
        Parser.parse!(%{
          "type" => "enum",
          "name" => "bad_name_2",
          "symbols" => ["1a"]
        })
      end
    end
  end
end
