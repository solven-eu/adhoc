# Multi-stage build for Pivotable on Render. Render's native Java runtime does not cover Java 25
# (the project's <java.version>), so we bring our own via Temurin. Build stage produces the
# Spring Boot exec-jar; run stage ships a minimal JRE image plus that jar.
#
# Cache notes:
# - The `-B -DskipTests -Dspotless.skip=true -Denforcer.skip=true` flags keep the build under
#   ~3 min on Render's Starter build instance; skip static-analysis too since this jar is only
#   expected to run, not ship.
# - We copy pom files ahead of sources to let Docker layer-cache the Maven dependency download
#   (the heavyweight step) when only Java sources change.

# ── Build stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS build
WORKDIR /src

# Temurin's base image is JDK-only — bring Maven in via apt. No `./mvnw` wrapper is checked
# into the repo.
RUN apt-get update && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

# Copy the full repo — the Maven reactor needs every module's pom.xml to resolve, and we need
# the source trees to compile. The `.dockerignore` at the repo root prunes `target/`, `.git/`,
# and `node_modules/` so this COPY stays reasonable.
COPY . .

# Build just the deployable server module and its reactor dependencies.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl :pivotable-server-webflux -am package \
        -DskipTests -Dspotless.skip=true -Denforcer.skip=true -Dcyclonedx.skip=true \
        -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true

# Spring Boot maven plugin produces an `-exec.jar` classifier — copy it to a stable path so the
# run stage doesn't need to know the version.
RUN cp pivotable/server-webflux/target/pivotable-server-webflux-*-exec.jar /tmp/app.jar

# ── Run stage ─────────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /tmp/app.jar /app/app.jar

# Render routes incoming traffic to whatever port the process binds; it injects PORT at runtime.
# `--server.port=${PORT}` forwards that binding to Spring Boot. JVM flags are conservative: 75 %
# of container memory for the heap, respect CGroup limits, enable string dedup for the 512 MB
# free plan.
ENV SPRING_PROFILES_ACTIVE=pivotable-unsafe,pivotable-simple_datasets
ENTRYPOINT ["sh", "-c", "java -XX:MaxRAMPercentage=75 -XX:+UseStringDeduplication -jar /app/app.jar --server.port=${PORT:-8080}"]
