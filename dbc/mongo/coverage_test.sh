# go get -u github.com/stretchr/testify/assert
clear && printf '\e[3J'
GOCACHE=off go test -cover -coverprofile coverage.out -race -v ./
go tool cover -html=coverage.out -o coverage.html
open coverage.html
