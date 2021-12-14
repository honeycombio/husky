# Husky Changelog

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
