# syntax=docker/dockerfile:1.6

# Trino version to build the connector against and to base the runtime image on.
# Override at build time to target another release, e.g.:
#     docker build --build-arg TRINO_VERSION=481 -t trino-kairosdb:481 .
# Declared before the first FROM so it can parameterize the runtime base image.
ARG TRINO_VERSION=479

# ---- build stage ----
FROM maven:3.9.11-eclipse-temurin-25-alpine AS build

# Re-declare inside the stage to bring the global ARG into scope.
ARG TRINO_VERSION

WORKDIR /app

# Cache dependencies in a separate layer so iterative source changes don't
# trigger a full redownload.
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -Dtrino.version=${TRINO_VERSION} dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -Dtrino.version=${TRINO_VERSION} -DskipTests package

# ---- runtime stage ----
FROM trinodb/trino:${TRINO_VERSION}

# Trino discovers plugins by directory name; the directory name doesn't have
# to match the connector name but conventionally does.
COPY --from=build /app/target/kairosdb-connector-*.jar /usr/lib/trino/plugin/kairosdb/
