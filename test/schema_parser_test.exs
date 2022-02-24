defmodule AvroEx.Schema.ParserTest do
  use ExUnit.Case

  alias AvroEx.{Schema}
  alias AvroEx.Schema.{Array, Context, Fixed, Parser, Primitive, Record, Union}

  describe "primitives" do
    test "it can decode primitives" do
      for p <- Parser.primitives() do
        p_atom = String.to_atom(p)
        assert %Schema{schema: %Primitive{type: ^p_atom}} = Parser.decode!(p)
      end
    end
  end
end
