# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_FLAGS="-O3 -march=x86-64" \
    && cmake --build build --parallel "$(nproc)"

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false mda
WORKDIR /app
COPY --from=builder /build/build/market_data_aggregator .

USER mda
EXPOSE 8080
ENTRYPOINT ["./market_data_aggregator"]
