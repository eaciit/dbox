# go get -u github.com/stretchr/testify/assert
clear && printf '\e[3J'
GOCACHE=off go test -cover -race -v ./
