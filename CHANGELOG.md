# Husky Changelog

## 0.12.0 2022-07-26

### Enhancements

- Update OTLP proto to v0.18 (#87) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

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
