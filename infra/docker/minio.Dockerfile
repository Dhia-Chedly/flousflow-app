# Official source releases; avoids dependency on retired MinIO image registries.
FROM golang:1.24.2-bookworm AS server-build
ADD --checksum=sha256:1a63209927b9d1fb239b2a776b7b01d1902eff19379a108f4bc7005edcc0968c https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-04-22T22-12-26Z /tmp/source.tar.gz
RUN mkdir /src && tar -xzf /tmp/source.tar.gz -C /src --strip-components=1
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /out/minio .
COPY infra/docker/minio-healthcheck.go /tmp/healthcheck.go
RUN CGO_ENABLED=0 go build -trimpath -o /out/healthcheck /tmp/healthcheck.go

FROM golang:1.24.2-bookworm AS client-build
ADD --checksum=sha256:526ec538e7f83e4bfa631788129b08ec85898ca572ce0523ebf9a597946bea27 https://codeload.github.com/minio/mc/tar.gz/refs/tags/RELEASE.2025-04-16T18-13-26Z /tmp/source.tar.gz
RUN mkdir /src && tar -xzf /tmp/source.tar.gz -C /src --strip-components=1
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /out/mc .

FROM alpine:3.21.3 AS server
COPY --from=server-build /out/minio /usr/local/bin/minio
COPY --from=server-build /out/healthcheck /usr/local/bin/minio-healthcheck
COPY --from=server-build /src/LICENSE /licenses/minio-LICENSE
ENTRYPOINT ["minio"]

FROM alpine:3.21.3 AS client
COPY --from=client-build /out/mc /usr/local/bin/mc
COPY --from=client-build /src/LICENSE /licenses/mc-LICENSE
ENTRYPOINT ["mc"]
