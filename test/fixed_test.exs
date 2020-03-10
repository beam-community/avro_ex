defmodule AvroEx.Schema.Fixed.Test do
  use ExUnit.Case
  # import AvroEx.Error

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
      assert {:ok, %@test_module{}} = @test_module.cast(@valid_params)
    end

    test "errors if type is not fixed" do
      assert_raise FunctionClauseError, fn ->
        @valid_params
        |> Map.put("type", "record")
        |> @test_module.cast()
      end
    end

    test "errors if name is not provided" do
      assert {:error, _} =
               @valid_params
               |> Map.delete("name")
               |> @test_module.cast()
    end

    test "errors if size is not provided" do
      assert {:error, _} =
               @valid_params
               |> Map.delete("size")
               |> @test_module.cast()
    end

    test "does not error if namespace is not given" do
      assert {:ok, %@test_module{}} =
               @valid_params
               |> Map.delete("namespace")
               |> @test_module.cast()
    end

    test "does not error if aliases are not given" do
      assert {:ok, %@test_module{}} =
               @valid_params
               |> Map.delete("aliases")
               |> @test_module.cast()
    end

    test "does not error if aliases is empty" do
      assert {:ok, %@test_module{}} =
               @valid_params
               |> Map.put("aliases", [])
               |> @test_module.cast()
    end
  end
end
