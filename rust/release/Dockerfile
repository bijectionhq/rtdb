ARG RUST_IMAGE
ARG RUNTIME_IMAGE
FROM ${RUST_IMAGE} AS build

ARG DEBIAN_SNAPSHOT
RUN . /etc/os-release && test "$VERSION_CODENAME" = bookworm \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s bookworm main\n' "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y cmake pkg-config perl libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /source
COPY . .
ARG RTDB_FEATURES
ARG RTDB_REVISION
ARG SOURCE_DATE_EPOCH
ENV RTDB_REVISION=${RTDB_REVISION}
ENV RTDB_TZDB_PATH=/source/release-tzdb.dat
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
RUN test "$(rustc --version | cut -d ' ' -f 2)" = 1.94.1 \
    && if [ -n "$RTDB_FEATURES" ]; then \
         cargo build --manifest-path rust/Cargo.toml --release --locked --no-default-features -p rtdb-server -p rtdb-config -p rtdb-ffi --features "$RTDB_FEATURES"; \
       else \
         cargo build --manifest-path rust/Cargo.toml --release --locked --no-default-features -p rtdb-server -p rtdb-config -p rtdb-ffi; \
       fi

FROM ${RUNTIME_IMAGE} AS runtime
ARG DEBIAN_SNAPSHOT
RUN . /etc/os-release && test "$VERSION_CODENAME" = bookworm \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s bookworm main\n' "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates tzdata libstdc++6 libjemalloc2 libsasl2-2 libsasl2-modules-gssapi-mit \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 20000 rtdb \
    && useradd --uid 20000 --gid rtdb --create-home --shell /usr/sbin/nologin rtdb \
    && install -d -o rtdb -g rtdb /var/lib/rtdb

ARG RTDB_REVISION
ARG RTDB_VERSION
ARG RTDB_VARIANT
LABEL org.opencontainers.image.source=https://github.com/bijectionhq/rtdb \
      org.opencontainers.image.licenses=MPL-2.0 \
      org.opencontainers.image.revision=${RTDB_REVISION} \
      org.opencontainers.image.version=${RTDB_VERSION} \
      io.rtdb.variant=${RTDB_VARIANT} \
      io.rtdb.source-dirty=false \
      io.rtdb.test-hooks=false

COPY --from=build /source/rust/target/release/rtdb /usr/local/bin/rtdb
COPY --from=build /source/rust/target/release/rtdb-config /usr/local/bin/rtdb-config
COPY --from=build /source/rust/target/release/librtdb_ffi.so /usr/local/lib/librtdb_ffi.so
COPY --from=build /source/rust/target/release/librtdb_ffi.a /usr/local/lib/librtdb_ffi.a
COPY rust/crates/ffi/include/rtdb.h /usr/local/include/rtdb.h
COPY LICENSE /usr/share/licenses/rtdb/LICENSE
COPY rust/crates/types/data/tzdb/LICENSE rust/crates/types/data/tzdb/ASSEMBLY_EXCEPTION rust/crates/types/data/tzdb/ADDITIONAL_LICENSE_INFO /usr/share/licenses/rtdb/tzdb/
COPY rust/release/default.yaml /etc/rtdb/config.yaml
COPY release-input.json /usr/share/rtdb/build.json
COPY release-tzdb.dat /usr/share/rtdb/tzdb.dat

USER rtdb
WORKDIR /var/lib/rtdb
ENV TZ=UTC
ENV LD_PRELOAD=libjemalloc.so.2
ENV MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0
EXPOSE 5432 3000 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/rtdb"]
CMD ["node", "--file", "/etc/rtdb/config.yaml"]
