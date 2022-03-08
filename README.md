# AvroEx

An [Avro](https://avro.apache.org/) encoding/decoding library written in pure Elixir.

## Documentation

The docs can be found on [hex.pm](https://hexdocs.pm/avro_ex/AvroEx.html)

## Installation

```elixir
def deps do
  [{:avro_ex, "~> 2.0"}]
end
```

## Usage

### Schema Decoding

Avro uses schemas to define the shape and contract for data. The schemas that your 
application uses may be defined locally, or may come from a [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

In either case, the first step is to decode a schema defined as JSON or Elixir terms into a `t:AvroEx.Schema.t/0`

```elixir
iex> AvroEx.decode_schema!(["int", "string"])
%AvroEx.Schema{
  context: %AvroEx.Schema.Context{names: %{}},
  schema: %AvroEx.Schema.Union{
    possibilities: [
      %AvroEx.Schema.Primitive{metadata: %{}, type: :int},
      %AvroEx.Schema.Primitive{metadata: %{}, type: :string}
    ]
  }
}
```

`AvroEx` will automatically detect Elixir terms or JSON, so you can decode JSON schemas directly

``` elixir
iex> AvroEx.decode_schema!("[\"int\",\"string\"]")
%AvroEx.Schema{
  context: %AvroEx.Schema.Context{names: %{}},
  schema: %AvroEx.Schema.Union{
    possibilities: [
      %AvroEx.Schema.Primitive{metadata: %{}, type: :int},
      %AvroEx.Schema.Primitive{metadata: %{}, type: :string}
    ]
  }
}
```

#### Strict Schema Decoding

When writing an Avro schema, it is helpful to get feedback on unrecognized fields. For this purpose,
it is recommended to use the `:strict` option to provide additional checks. Note that it is not
recommended to use this option in production when pulling externally defined schemas, as they may
have published a schema with looser validations.

``` elixir
iex> AvroEx.decode_schema!(%{"type" => "map", "values" => "int", "bogus" => "value"}, strict: true)
** (AvroEx.Schema.DecodeError) Unrecognized schema key `bogus` for AvroEx.Schema.Map in %{"bogus" => "value", "type" => "map", "values" => "int"}
    (avro_ex 1.2.0) lib/avro_ex/schema/parser.ex:43: AvroEx.Schema.Parser.parse!/2
```


## Encoding

When publishing Avro data, it first must be encoded using the schema.

```elixir
iex> schema = AvroEx.decode_schema!(%{
                "type" => "record",
                "name" => "MyRecord",
                "fields" => [
                  %{"name" => "a", "type" => "int"},
                  %{"name" => "b", "type" => "string"},
                ]
              })
iex> AvroEx.encode!(schema, %{a: 1, b: "two"})
<<2, 6, 116, 119, 111>
```

## Decoding

When receiving Avro data, decode it using the schema

``` elixir
iex> AvroEx.decode!(schema, <<2, 6, 116, 119, 111>>)
%{"a" => 1, "b" => "two"}
```

## Schema Encoding

`AvroEx` also supports encoding schemas back to JSON. This may be needed when registering schemas or
serializing them to disk.

``` elixir
iex> AvroEx.encode_schema(schema)
"{\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"int\"}},{\"name\":\"b\",\"type\":{\"type\":\"string\"}}],\"name\":\"MyRecord\",\"type\":\"record\"}"
```

Additionally, schemas can be encoded to [Parsing Canonical Form](https://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas) using
the `:canonical` option.

``` elixir
iex> AvroEx.encode_schema(schema, canonical: true)
"{\"name\":\"MyRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}]}"
```

### Testing

For testing convenience, `AvroEx.encodable?/2` is exported to check if data can be
encoded against the given schema. Note that in production scenarios, it is not
recommended to use this function.

```elixir
defmodule MyModule.Test do
  use ExUnit.Case

  setup do
    data = ...
    schema = ...
    {:ok, %{data: data, schema: schema}}
  end

  describe "my_function/1" do
    test "builds a structure that can be encoded with our avro schema", context do
      result = MyModule.my_function(context.data)

      assert AvroEx.encodable?(context.schema, result)
    end
  end
end
```
