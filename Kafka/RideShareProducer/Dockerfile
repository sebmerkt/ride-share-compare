FROM maven:3.6.3-jdk-8-openj9 AS build

ARG user=ubuntu
#ENV user=user
WORKDIR /home/$user/ride-share-compare/Kafka/RideShareProducer

# Fetch all dependencies
COPY pom.xml .
RUN mvn verify -DskipTests --fail-never

# Build application
COPY src ./src
RUN mvn verify

FROM azul/zulu-openjdk-alpine:11.0.3-jre
ENTRYPOINT ["java", "-jar", "/app/RideShareProducerV1.jar"]

WORKDIR /app
COPY --from=build /home/$user/ride-share-compare/Kafka/RideShareProducer/target .