# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jdk-alpine@sha256:6ea5548706b60ac0a602eaf48af74792cbab012d90e811ca8db6184b16b5c3d6 AS build

ARG SOURCE_DATE_EPOCH=1788652800
WORKDIR /workspace

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY flowforge-domain/pom.xml flowforge-domain/pom.xml
COPY flowforge-messaging/pom.xml flowforge-messaging/pom.xml
COPY flowforge-observability/pom.xml flowforge-observability/pom.xml
COPY flowforge-kafka-support/pom.xml flowforge-kafka-support/pom.xml
COPY flowforge-application/pom.xml flowforge-application/pom.xml
COPY flowforge-control-plane/pom.xml flowforge-control-plane/pom.xml
COPY flowforge-worker/pom.xml flowforge-worker/pom.xml
COPY flowforge-load-test/pom.xml flowforge-load-test/pom.xml
RUN --mount=type=secret,id=build_truststore,required=false \
    if [ -f /run/secrets/build_truststore ]; then \
      cp /run/secrets/build_truststore "$JAVA_HOME/lib/security/cacerts"; \
    fi \
    && sed -i 's/\r$//' mvnw \
    && chmod 0755 mvnw \
    && ./mvnw -B -ntp -pl flowforge-control-plane -am dependency:go-offline

COPY . .
RUN sed -i 's/\r$//' mvnw \
    && chmod 0755 mvnw \
    && ./mvnw -B -ntp -pl flowforge-control-plane -am \
    -Dmaven.test.skip=true \
    -Dproject.build.outputTimestamp="${SOURCE_DATE_EPOCH}" \
    package

FROM eclipse-temurin:21-jre-noble@sha256:96975602e131485862eb8cd32927face8a06d7591a5e865944b634a701d9df72 AS runtime

ARG VERSION=0.1.0-SNAPSHOT
ARG REVISION=unknown
ARG SOURCE_URL=unknown
LABEL org.opencontainers.image.title="FlowForge Control Plane" \
      org.opencontainers.image.description="FlowForge workflow orchestration control plane" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE_URL}"

RUN groupadd --gid 10001 flowforge \
    && useradd --uid 10001 --gid flowforge --no-create-home --shell /usr/sbin/nologin flowforge
WORKDIR /opt/flowforge
COPY --from=build --chown=10001:10001 \
    /workspace/flowforge-control-plane/target/flowforge-control-plane-*-exec.jar \
    application.jar

ENV JAVA_TOOL_OPTIONS="-XX:+ExitOnOutOfMemoryError -XX:MaxRAMPercentage=75.0 -Djava.io.tmpdir=/tmp"
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=45s --retries=3 \
    CMD wget -q -T 2 -O - http://127.0.0.1:8080/actuator/health/liveness >/dev/null || exit 1
ENTRYPOINT ["java", "-jar", "/opt/flowforge/application.jar"]
