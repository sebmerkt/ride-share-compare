FROM maven:4-jdk-8-slim AS build

ARG user=ubuntu
#ENV user=user
WORKDIR /home/$user/producer

# Fetch all dependencies
COPY pom.xml .
RUN mvn verify -DskipTests -Dgit.skip=true --fail-never

# Build application
COPY src ./src
RUN mvn verify

FROM azul/zulu-openjdk-alpine:11.0.3-jre
ENTRYPOINT ["java", "-jar", "/app/RideShareProducerV1.jar"]

WORKDIR /app
COPY --from=build /home/$user/producer/target .