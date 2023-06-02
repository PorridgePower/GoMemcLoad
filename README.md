# GO MEMC LOAD


## Build

### Build proto files
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
protoc --proto_path=api api/*.proto --go_out=api/
```

```
go build cmd/memc_load/main.go
```

## Run
```
go run cmd/memc_load/main.go -pattern=\*appinstalled/\*.tsv.gz -debug
```
