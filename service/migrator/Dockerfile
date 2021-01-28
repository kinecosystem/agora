FROM alpine

ARG GRPC_HEALTH_PROBE_VERSION=v0.3.1

RUN apk add --no-cache curl
RUN curl -L https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/$GRPC_HEALTH_PROBE_VERSION/grpc_health_probe-linux-amd64 -o /bin/grpc_health_probe
RUN chmod +x /bin/grpc_health_probe

COPY build/linux-amd64/migrator /migrator

EXPOSE 8123
EXPOSE 8085

HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD /bin/grpc_health_probe -addr=localhost:8085 -connect-timeout 250ms -rpc-timeout 100ms || exit 1

CMD ["/migrator"]
