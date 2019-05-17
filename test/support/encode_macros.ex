defmodule AvroEx.Encode.Test.Macros do
  defmacro assert_result(m, f, a, result) do
    quote do
      test "#{unquote(m)}.#{unquote(f)} - #{unquote(:erlang.unique_integer())}" do
        assert apply(unquote(m), unquote(f), unquote(a)) == unquote(result)
      end
    end
  end
end
