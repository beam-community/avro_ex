# Changelog

## v2.2.0 - July 31st, 2024

### Added

- Support for encoding and decoding Decimals

### Fixed

- Incorrect error for decimal encoding
- String.slice deprecation warning

## v2.1.0 - March 28th, 2023

### Added

- Support for tagged unions in named fields
- `AvroEx.encode/3` supports a `:include_block_byte_size` option for encoding block size

### Fixed

- Allow empty strings as namespaces
- Fix `encodeable?` checks for int and float
- Set time values to proper precision on decoding
- Fixed reference type name generation

## v2.0.1 - April 3rd, 2022

### Fixed

- Fixed bug where Array and Map children of Unions would fail to parse

## v2.0.0 - March 8th, 2022

### Changed

- `AvroEx.encode/2` now returns `{:error, AvroEx.EncodeError.t()}` in the case of an error
- Primitive integer types now represented as `%Primitive{type: :int}` instead of `%Primitive{type: :integer}`
- Primitive null types now represented as `%Primitive{type: :null}` instead of `%Primitive{type: nil}`
- Schema decoding now supports directly passing Elixir terms, will strictly validate the schema, and produce helpful error messages
- Removed `Ecto` as a dependency
- `AvroEx.Schema.full_name/2` - reverses the order of the arguments, accepting a Schema type or name, followed by the namespace

### Added

- `AvroEx.encode!/2` - identical to `encode/2`, but returns raw value. Raises on error
- `AvroEx.decode!/2` - identical to `decode/2`, but returns raw value. Raises on error
- `AvroEx.decode_schema/1` and `AvroEx.decode_schema!/` in place of `AvroEx.parse_schema/1`
- Support for encoding and decoding `date` logical times to and from `Date.t()`
- Schema decoding adds a `:strict` option that will strictly validate the schema for unrecognized fields
- `AvroEx.encode_schema/2` - encode a `AvroEx.Schema.t()` back to JSON. Supports encoding the schema to [Parsing Canonical Form](https://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas)
- `AvroEx.Schema.namespace/2` - Returns the namespace of the given Schema type

### Deprecated

- `AvroEx.parse_schema/1`
- `AvroEx.parse_schema!/1`
- `AvroEx.named_type!/2`

## v1.2.0 - Februrary 20th 2022

### Fixed

- Fix exception when encoding bad Record data
- Address dialyzer issues
- Add type for AvroEx.Schema.Record.Field to fix compilation error
- Fix long encoding
- Fix variable integer and long decoding

### Added

- Support encoding DateTime and Time to logical types in Union

### Changed

- Records can accept atoms for encoding keys
- String values can accept atoms for encoding
- Enums can accept atoms for encoding
- Simplify integer and long encoding
