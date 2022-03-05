# Changelog

## v2.0.0 (Unreleased)

### Changed
* `AvroEx.encode/2` now returns `{:error, AvroEx.EncodeError.t()}` in the case of an error
* Primitive integer types now represented as `%Primitive{type: :int}` instead of `%Primitive{type: :integer}`
* Primitive null types now represented as `%Primitive{type: :null}` instead of `%Primitive{type: nil}`
* Schema parser now supports Elixir terms and will strictly validate the schema
* Removed `Ecto` as a dependency

### Added
* `AvroEx.encode!/2` - identical to `encode/2`, but raises
* `AvroEx.decode_schema/1` and `AvroEx.decode_schema!/` in place of `AvroEx.parse_schema/1`

### Deprecated
* `AvroEx.parse_schema/1`
* `AvroEx.parse_schema!/1`
* `AvroEx.named_type!/2`

## v1.2.0

### Fixed
* Fix exception when encoding bad Record data
* Address dialyzer issues
* Add type for AvroEx.Schema.Record.Field to fix compilation error
* Fix long encoding
* Fix variable integer and long decoding

### Added
* Support encoding DateTime and Time to logical types in Union

### Changed
* Records can accept atoms for encoding keys
* String values can accept atoms for encoding
* Enums can accept atoms for encoding
* Simplify integer and long encoding


