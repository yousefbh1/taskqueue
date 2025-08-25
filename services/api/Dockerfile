FROM golang:1.23 AS build
WORKDIR /src

# bring in module files first (cacheable)
COPY go.mod go.sum ./
RUN go mod download

# now copy the rest (includes gen/*.go)
COPY . .

# tidy inside container so go.mod/go.sum match imports
RUN go mod tidy

# build
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api

FROM gcr.io/distroless/base-debian12
COPY --from=build /out/api /api
EXPOSE 8080
ENTRYPOINT ["/api"]
