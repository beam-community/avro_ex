defmodule AvroEx.Validation.Test do
  use ExUnit.Case

  import AvroEx.Error
  alias AvroEx.Schema.Record
  alias Ecto.Changeset

  @test_module AvroEx.Validation

  describe "validate_string" do
    cs =
      %Record{}
      |> Changeset.cast(%{"name" => :abc}, [:name])
      |> @test_module.validate_string(:name)

    assert error("must be a string") in errors(cs, :name)
  end
end
