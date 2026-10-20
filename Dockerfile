# Override ORT_IMAGE to build and test another ORT release without changing the runner.
ARG ORT_IMAGE=ghcr.io/oss-review-toolkit/ort:92.4.0@sha256:748afc9d459f6d2bb1dde90fe4bc378d8ea2c756f8c5e19607290ae42446a1d1
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
