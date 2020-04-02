# DATA_PLAY

## Getting Start
### Start
To Run program, please run in env with (go 1.13), and with postgresDB config in `main.go`
```sh
go mod download
go run .
# if you use docker compose for postgresDB and create dir `pgdata`
docker-compose up -d
```

### Unit Test
```
go test ./...
```
