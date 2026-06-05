# One Dockerfile for all services. docker-compose passes MODULE to pick which one to build.
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY ingestion-service/pom.xml ingestion-service/
COPY detector-service/pom.xml detector-service/
COPY alerts-api/pom.xml alerts-api/
# Download dependencies first so code changes don't re-download them.
RUN mvn -q -pl ${MODULE} -am dependency:go-offline -DskipTests || true
COPY . .
RUN mvn -q -pl ${MODULE} -am package -DskipTests \
    && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre
COPY --from=build /app.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
