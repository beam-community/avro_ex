# AvroEx

A pure-elixir avro encoding/decoding library.

## Documentation

The docs can be found on [hex.pm](https://hexdocs.pm/avro_ex/AvroEx.html)

## Installation

```
def deps do
	[{:avro_ex, "~> 0.1.0-beta.0"}]
end
```

## Usage

### Decoding

If you have a worker which receives a raw avro message and some kind of
repository where you're storing your schemas, you might do something like this:

```elixir
defmodule MyWorker do
	alias AvroEx.Schema

	def start_link() do
		WorkerLib.start_link(__MODULE__)
	end

	@schema_id "some_schema"

	def handle_message(message) do
		{:ok, decoded_message} =
			@schema_id
			|> SchemaRepository.fetch_schema
			|> AvroEx.parse_schema!
			|> AvroEx.decode(message)

		# And do things with the message
	end
end
```

### Encoding

Let's say you have a LinkedList with the following schema:

```json
{
	"type": "record",
	"name": "LinkedList",
	"fields": [
		{"name": "value", "type": "int"},
		{"name": "next", "type": ["null", "LinkedList"]}
	]
}
```

If you wanted to encode it, you would do something like:

```elixir
def my_function(schema) do
	list =
		%{
			"value" => 9001,
			"next" => %{
				"value" => 42,
				"next" => nil
			}
		}

	{:ok, encoded_avro} = AvroEx.encode(schema, list)
	# Do something with encoded avro
end
```

### Testing

If you're unit testing your code and you want to ensure that your code builds a
structure that is encodable using the given schema:

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
