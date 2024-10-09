FROM bellsoft/liberica-openjdk-alpine:17.0.6

ARG JAR_FILE=target/*.jar

WORKDIR application

COPY ${JAR_FILE} application.jar

ENTRYPOINT exec java -jar application.jar