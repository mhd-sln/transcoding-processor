FROM golang:1.16 AS build

WORKDIR /transcoding-processor/

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build ./cmd/process

FROM jrottenberg/ffmpeg:4.1-alpine

COPY --from=build /transcoding-processor/process .

ENTRYPOINT  ["/process"]
