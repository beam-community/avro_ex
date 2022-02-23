# Changelog

## v2.0.0 (Unreleased)

### Changed
* `AvroEx.encode/2` now returns `{:error, AvroEx.EncodeError.t()}` in the case of an error

### Added
* `AvroEx.encode!/2` - identical to `encode/2`, but raises
* `AvroEx.decode_schema/1` and `AvroEx.decode_schema!/` in place of `AvroEx.parse_schema/1`

### Deprecated
* `AvroEx.parse_schema/1`
* `AvroEx.parse_schema!/1`

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


