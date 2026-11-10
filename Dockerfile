# Override ORT_IMAGE to build and test another ORT release without changing the runner.
ARG ORT_IMAGE=ghcr.io/oss-review-toolkit/ort:97.3.0@sha256:14512c67563376669116a7a5810e71d4e2da2aea5756e30457a97c19c3469be3
ARG GO_IMAGE=golang:1.26.5-bookworm

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG RUNNER_VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${RUNNER_VERSION}" -o /out/ort-runner ./cmd/ort-runner

FROM ${ORT_IMAGE}
ARG ORT_IMAGE
ENV ORT_RUNNER_ORT_IMAGE=${ORT_IMAGE}
COPY --from=build /out/ort-runner /usr/local/bin/ort-runner
ENTRYPOINT ["/usr/local/bin/ort-runner"]
CMD []
