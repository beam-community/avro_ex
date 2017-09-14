defmodule AvroEx.Schema.Fixed.Test do
  use ExUnit.Case
  #import AvroEx.Error

  @test_module AvroEx.Schema.Fixed

  @valid_params %{
    "type" => "fixed",
    "name" => "MySha",
    "namespace" => "me.cjpoll.avro_ex",
    "aliases" => ["TestSha"],
    "size" => 40
  }

  describe "changeset" do
    test "params are valid" do
      assert {:ok, %@test_module{}} =
        @test_module.cast(@valid_params)
    end

    test "errors if type is not fixed" do
      assert_raise FunctionClauseError, fn ->
        @test_module.cast(@valid_params |> Map.put("type", "record"))
      end
    end

    test "errors if name is not provided" do
      assert {:error, _} =
        @test_module.cast(@valid_params |> Map.delete("name"))
    end

    test "errors if size is not provided" do
      assert {:error, _} =
        @test_module.cast(@valid_params |> Map.delete("size"))
    end

    test "does not error if namespace is not given" do
      assert {:ok, %@test_module{}} =
        @test_module.cast(@valid_params |> Map.delete("namespace"))
    end

    test "does not error if aliases are not given" do
      assert {:ok, %@test_module{}} =
        @test_module.cast(@valid_params |> Map.delete("aliases"))
    end

    test "does not error if aliases is empty" do
      assert {:ok, %@test_module{}} =
        @test_module.cast(@valid_params |> Map.put("aliases", []))
    end
  end
end
