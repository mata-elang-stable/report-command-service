ARG GO_VERSION=1.23

FROM golang:${GO_VERSION}-alpine AS build

ARG APP_NAME="report-command"
ARG APP_VERSION="dev"
ARG APP_COMMIT="none"

RUN apk add --no-cache librdkafka-dev pkgconf build-base musl-dev

WORKDIR /go/src/app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GO111MODULE=on

RUN go build -ldflags "-X main.appVersion=${APP_VERSION} -X main.appCommit=${APP_COMMIT} -X main.appLicense=${APP_LICENSE}" -tags dynamic -tags musl -o /go/bin/app ./cmd/report

FROM golang:${GO_VERSION}-alpine

RUN apk add --no-cache librdkafka

COPY --from=build /go/bin/app /go/bin/app

ENTRYPOINT ["/go/bin/app"]