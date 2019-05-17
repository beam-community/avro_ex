defmodule AvroEx.Schema.Test.Macros do
  defmacro handles_metadata do
    quote do
      test "has a default empty metadata" do
        assert {:ok, %@test_module{schema: %@schema{metadata: %{}}}} = @test_module.parse(@json)
      end

      test "includes extra metadata if given" do
        assert {:ok, %@test_module{schema: %@schema{metadata: %{"meta_prop" => "abc"}}}} =
                 @json
                 |> json_add_property(:meta_prop, "abc")
                 |> @test_module.parse
      end
    end
  end

  defmacro cast(passed_in_type, primitive_type) do
    quote do
      test "simple #{unquote(passed_in_type)}" do
        assert {:ok, %AvroEx.Schema.Primitive{type: unquote(primitive_type)}} =
                 @test_module.cast(unquote(passed_in_type))
      end

      test "complex #{unquote(passed_in_type)}" do
        assert {:ok,
                %AvroEx.Schema.Primitive{
                  type: unquote(primitive_type),
                  metadata: %{"some" => "metadata"}
                }} = @test_module.cast(%{"type" => unquote(passed_in_type), "some" => "metadata"})
      end

      test "complex #{unquote(passed_in_type)} with metadata" do
        assert {:ok,
                %AvroEx.Schema.Primitive{
                  type: unquote(primitive_type),
                  metadata: %{"some" => "metadata"}
                }} = @test_module.cast(%{"type" => unquote(passed_in_type), "some" => "metadata"})
      end
    end
  end

  defmacro parse_primitive(passed_in_type, primitive_type) do
    quote do
      test "simple #{unquote(passed_in_type)}" do
        assert {:ok,
                %@test_module{
                  schema: %AvroEx.Schema.Primitive{type: unquote(primitive_type)},
                  context: %AvroEx.Schema.Context{names: %{}}
                }} = @test_module.parse(~s("#{unquote(passed_in_type)}"))
      end

      test "complex #{unquote(passed_in_type)}" do
        assert {:ok,
                %@test_module{
                  schema: %AvroEx.Schema.Primitive{type: unquote(primitive_type)},
                  context: %AvroEx.Schema.Context{names: %{}}
                }} = @test_module.parse(~s({"type": "#{unquote(passed_in_type)}"}))
      end

      test "complex #{unquote(passed_in_type)} with metadata" do
        assert {:ok,
                %@test_module{
                  schema: %AvroEx.Schema.Primitive{
                    type: unquote(primitive_type),
                    metadata: %{"some" => "metadata"}
                  },
                  context: %AvroEx.Schema.Context{names: %{}}
                }} = @test_module.parse(~s({"type": "#{unquote(passed_in_type)}", "some": "metadata"}))
      end
    end
  end
end
