# syntax=docker/dockerfile:1.6

# ---- build stage ----
FROM maven:3.9.10-eclipse-temurin-24-alpine AS build

WORKDIR /app

# Cache dependencies in a separate layer so iterative source changes don't
# trigger a full redownload.
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package

# ---- runtime stage ----
FROM trinodb/trino:476

# Trino discovers plugins by directory name; the directory name doesn't have
# to match the connector name but conventionally does.
COPY --from=build /app/target/kairosdb-connector-*.jar /usr/lib/trino/plugin/kairosdb/
