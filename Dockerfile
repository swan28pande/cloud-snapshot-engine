FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:17-jre
COPY --from=build /src/target/snapshot-engine.jar /app/snapshot-engine.jar
ENV PORT=50051
EXPOSE 50051
ENTRYPOINT ["java", "-jar", "/app/snapshot-engine.jar"]
