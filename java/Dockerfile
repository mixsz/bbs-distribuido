FROM maven:3.9-eclipse-temurin-17
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q compile dependency:copy-dependencies -DoutputDirectory=target/lib
CMD ["java", "-cp", "target/classes:target/lib/*", "Servidor"]