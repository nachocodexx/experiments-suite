FROM openjdk:8-alpine
COPY ./target/scala-2.13/experiments-suite.jar /app/src/app.jar
WORKDIR /app/src
#ENTRYPOINT ["java", "-jar","app.jar"]
ENTRYPOINT ["java", "-cp","app.jar","mx.cinvestav.Main"]
