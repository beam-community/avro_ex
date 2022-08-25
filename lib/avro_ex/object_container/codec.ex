defmodule AvroEx.ObjectContainer.Codec do
  @callback encode!(binary()) :: binary()
  @callback decode!(binary()) :: binary()
  @callback name() :: atom()
end
