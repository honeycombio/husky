# Husky Changelog

## 0.23.1 2023-12-08

- fix: bug where we could error after writing a status code (#224) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- maint: add deps and docs to maintenance in release (#223) | [Jamie Danielson](https://github.com/JamieDanielson)
- maint: add extra detail to release doc (#222) | [Jamie Danielson](https://github.com/JamieDanielson)

## 0.23.0 2023-12-08

- feat: Add public functions for handling OTLP HTTP responses (#219) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- maint: update codeowners (#220) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- maint(deps): bump google.golang.org/grpc from 1.58.3 to 1.59.0 (#218)
- maint(deps): bump github.com/klauspost/compress from 1.17.2 to 1.17.4 (#217)
- maint(deps): bump google.golang.org/grpc from 1.58.2 to 1.58.3 (#215)
- maint(deps): bump golang.org/x/net from 0.12.0 to 0.17.0 (#214)
- maint: bump github.com/klauspost/compress from 1.16.7 to 1.17.2 (#213) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- maint(deps): bump google.golang.org/grpc from 1.56.1 to 1.58.2 (#210)
- maint(deps): bump github.com/klauspost/compress from 1.16.5 to 1.16.7 (#204)
- maint(deps): bump google.golang.org/grpc from 1.55.0 to 1.56.1 (#202)
- maint(deps): bump google.golang.org/protobuf from 1.30.0 to 1.31.0 (#203)
- maint(deps): bump google.golang.org/grpc from 1.54.0 to 1.55.0 (#200)
- maint(deps): bump github.com/stretchr/testify from 1.8.2 to 1.8.4 (#199)


## 0.22.4 2023-05-16

fix: Send the values not the Values in exception details (#197) | [Kent Quirk](https://github.com/kentquirk)

## 0.22.3 2023-05-12

- feat: copy exception details from span event to parent span (#191) | [Phillip Carter](https://github.com/cartermp)
- maint: Remove refs to proxy token and headers (#193) | [Kent Quirk](https://github.com/kentquirk)
- maint: update dependabot config (#195) | [Vera Reynolds](https://github.com/vreynolds)
- maint(deps): bump google.golang.org/grpc from 1.53.0 to 1.54.0 (#190)
- maint(deps): bump github.com/klauspost/compress from 1.16.0 to 1.16.3 (#189)
- maint(deps): bump google.golang.org/protobuf from 1.28.1 to 1.30.0 (#188)
- maint(deps): bump github.com/klauspost/compress from 1.16.3 to 1.16.5 (#194)


## 0.22.2 2023-03-09

- Use bytesToSpanID for parentID too. (#185) | [Robb Kidd](https://github.com/robbkidd)
- Fix bug in bytesToSpanID, add test for it. (#184) | [Robb Kidd](https://github.com/robbkidd)

## 0.22.1 2023-03-08

- Fix bug in BytesToTraceID, add test for it. (#182) | [Kent Quirk](https://github.com/kentquirk)

## 0.22.0 2023-03-08

This release fixes an issue where the traceID and spanID in OTLP/JSON data is being misinterpreted by protojson as base64,
rather than hex. We can't fix it in the protobuf, but we can reverse the bad decoding.

- fix: JSON ingestion issue (#179) | [Kent Quirk](https://github.com/kentquirk)
- maint: clean up after dependabot  (#180) | [Kent Quirk](https://github.com/kentquirk)
- maint(deps): bump github.com/stretchr/testify from 1.8.0 to 1.8.2 (#178) | [dependabot[bot]](https://github.com/)
- maint(deps): bump google.golang.org/grpc from 1.50.0 to 1.53.0 (#177) | [dependabot[bot]](https://github.com/)
- maint(deps): bump github.com/klauspost/compress from 1.15.11 to 1.16.0 (#176) | [dependabot[bot]](https://github.com/)

## 0.21.0 2023-02-22

### Enhancements

- fix: correct Ruby library/scope name prefix (#172) | [Robb Kidd](https://github.com/robbkidd)
- fix: update dotnet library name prefix (#174) | [Jamie Danielson](https://github.com/JamieDanielson)
- fix: update java library name prefix (#173) | [Jamie Danielson](https://github.com/JamieDanielson)

## 0.20.0 2023-02-16

### Enhancements

- Detect if scope is from instrumentation library (#170) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.19.0 2023-01-13

### Enhancements

- Add error field to events/links on spans that are error spans (#159) | [@cartermp](https://github.com/cartermp)

### Fixed

- Makes husky compatible with OTel SDK (#161) | [@maxedmands](https://github.com/maxedmands)

### Maintenance

- Make dependabot titles work better (#157) | [@kentquirk](https://github.com/kentquirk)
- Update validate PR title workflow (#152) | [@pkanal](https://github.com/pkanal)
- Validate PR title (#151) | [@pkanal](https://github.com/pkanal)

## 0.18.0 2022-11-07

### Fixed

- Fix kvlist and byte arrays marshalling (#145) | [@kentquirk](https://github.com/kentquirk)

### Maintenance

- maint: delete workflows for old board (#139) | [@vreynolds](https://github.com/vreynolds)
- maint: add release file (#138) | [@vreynolds](https://github.com/vreynolds)

## 0.17.0 2022-10-12

### Enhancements

- Add span sample rate to links and events (#133) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- Add new project workflow (#135) | [@vreynolds](https://github.com/vreynolds)
- Update some dependencies for protobuf and ioutil (#136) | [@kentquirk](https://github.com/kentquirk)
- Bump github.com/klauspost/compress from 1.15.9 to 1.15.11 (#134)

## 0.16.1 2022-09-27

### Fixed

- Avoid nil pointer (#129) | [@vreynolds](https://github.com/vreynolds)

### Maintenance

- Bump google.golang.org/grpc from 1.48.0 to 1.49.0 (#120)
- Bump github.com/grpc-ecosystem/grpc-gateway/v2 from 2.11.1 to 2.11.3 (#119)

## 0.16.0 2022-09-13

### Enhancements

- Allow OTLP/http-json for metrics (#121) | [@robbkidd](https://github.com/robbkidd)
- Dataset targeting rules for OTel Logs (#126) | [@robbkidd](https://github.com/robbkidd)

## 0.15.0 2022-08-24

### Enhancements

- Add support for translating otlp/json trace and log requests (#112) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Fixed

- Stop scope attributes from crossing scopes (#113) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.14.0 2022-08-16

### Enhancements

- Replace usage of OTLP proto with internal copy (#106) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
  - Exposes the forked OTLP proto files for internal and external use
- Update OTLP proto to v0.19.0 (#110) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- Add support for scope attributes when translating traces & metrics requests (#111) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.13.0 2022-08-02

### Enhancements

- Add copy of genereted OTLP proto files (#100) | [@kentquirk](https://github.com/kentquirk)
  - Includes re-adding previously depreate metrics types (IntSum, IntGauge & IntHistogram) and StringKeyValue

## 0.12.0 2022-07-26

### Enhancements

!!! Breaking Changes !!!

- Update OTLP proto to v0.18 (#87) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
  - Husky will no longer support translating Deprecated metric types into Honeycomb events.

### Fixed

- Fix flaky invalid OTLP request body test (#98) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.11.2 2022-07-22

### Fixed

- only set meta.annotation_type field when log is part of trace (#95) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- Rename translate OTLP request structs to be generic (#94) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.11.1 2022-07-20

### Enhancements

- Add meta.annotation_type to log events | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.11.0 2022-07-14

### Enhancements

- Add OTLP logs support (#82) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- Bump github.com/klauspost/compress from 1.15.5 to 1.15.8 (#85)
- Bump google.golang.org/grpc from 1.46.0 to 1.48.0 (#86)
- Bump github.com/stretchr/testify from 1.7.1 to 1.8.0 (#83)
- Bump github.com/klauspost/compress from 1.15.2 to 1.15.5 (#80)

## 0.10.6 2022-05-20

### Fixed

- Convert OTLP span status.code type to integer earlier in the transform (#78) | [@robbkidd](https://github.com/robbkidd)

## 0.10.5 2022-05-09

### Fixed

- Fix library.version spillover between spans in a single batch (#69) | [@robbkidd](https://github.com/robbkidd)
- Copy span attrs after resource attrs (#74) | [@robbkidd](https://github.com/robbkidd)

### Maintenance

- Bump google.golang.org/grpc from 1.45.0 to 1.46.0 (#72)
- Bump github.com/klauspost/compress from 1.15.1 to 1.15.2 (#71)

## 0.10.4 2022-04-25

### Maintenance

- bump otlp to v0.11 (#66) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- update go to 1.18 (#57) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- Bump google.golang.org/grpc from 1.40.0 to 1.45.0 (#62) | [dependabot](https://github.com/apps/dependabot)
- Bump github.com/klauspost/compress from 1.13.6 to 1.15.1 (#61) | [dependabot](https://github.com/apps/dependabot)
- Bump google.golang.org/protobuf from 1.27.1 to 1.28.0 (#59) | [dependabot](https://github.com/apps/dependabot)
- Bump github.com/stretchr/testify from 1.7.0 to 1.7.1 (#60) | [dependabot](https://github.com/apps/dependabot)
- Create dependabot.yml (#58) | [@vreynolds](https://github.com/vreynolds)

## 0.10.3 2022-03-28

### Fixed

- default sample rate to 1 if omitted or if 0 (#55) | [@asdvalenzuela](https://github.com/asdvalenzuela)

## 0.10.2 2022-03-07

### Fixed

- Trim whitespace when deriving from service.name (#52) | [@vreynolds](https://github.com/vreynolds)

## 0.10.1 2022-03-01

### Fixed

- Return sample rate of 1 when no sample rate key found (#50) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 0.10.0 2022-02-18

### Enhancements

- feat: update metrics header validation (#48) | [@vreynolds](https://github.com/vreynolds)

## 0.9.0 2022-02-15

### Enhancements

- feat: truncate dataset name when service is unknown (#46) | [@vreynolds](https://github.com/vreynolds)

## 0.8.1 2022-02-10

### Enhancements

- empty or missing service name should default to unknown_service (#42) | [@JamieDanielson](https://github.com/JamieDanielson)

### Maintenance

- maint: remove indent style from editorconfig (#39) | [@vreynolds](https://github.com/vreynolds)

## 0.8.0 2022-02-03

## !!! Breaking Changes !!!

- feat!: include individual batch sizes (#38)

## 0.7.0 2022-02-02

## !!! Breaking Changes !!!

- feat!: environments & services support (#35)

## 0.6.0 2021-12-15

### Enhancements

- Add versioning info (#32)

## 0.5.0 2021-12-07

### Enhancements

- Parse proxy version as part of RequestInfo (#30)
- Record number of span events and links (#29)

## 0.4.0 2021-12-03

### Dependencies

- Downgrade otlp-proto to v0.9.0 & grpc to 1.40.0 (#27)

## 0.3.0 2021-12-01

### Enhancements

- Update translate trace request calls to return struct instead of params (#23)

### Maintenance

- Add changelog & releasing docs (#20)
- Add case insensitive tests for parsing headers (#24)

## 0.2.0 2021-11-29

### Enhancements

- Remove dependency on http.Request (#15)
- Add support for array and KV list attributes (#17)
- Add error support (#14)
- Make bytesToTraceID public (#13)
- Use parent span's timestamp for span links (#10)

### Fixes

- Handle KeyValue attributes with nil value (#19)

### Maintenance

- Update otlp-proto to v0.11.0 (#16)
- Add CODEOWNERS and move PULL_REQUEST_TEMPLATE to .github (#5)
- Add CircleCI config (#9)
- Update repo to have standard OSS issues templates, codeowners, workflows, etc (#12)

## 0.1.0 2021-10-26

### Enhancements

- Add tests for trace and common (#6)
- Add content-encoding and grpc-accept-encoding to RequestInfo (#4)
- Add OTLP trace HTTP/gRPC translators (#1)

### Maintenance

- tidy up default project files, add missing OSS files, etc
