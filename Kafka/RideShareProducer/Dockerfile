FROM openjdk:8-jdk-alpine

ARG JAR_FILE=target/RideShareProducerV1.jar
ARG JAR_LIB_FILE=target/lib/

# cd /usr/local/runme
WORKDIR /usr/local/runme

# copy target/RideShareProducerV1.jar /usr/local/runme/RideShareProducerV1.jar
COPY ${JAR_FILE} RideShareProducerV1.jar

# copy project dependencies
# cp -rf target/lib/  /usr/local/runme/lib
ADD ${JAR_LIB_FILE} lib/

# java -jar /usr/local/runme/app.jar
ENTRYPOINT ["java","-jar","RideShareProducerV1.jar"]