# marrim nje imazh qe ka maven dhe java
# maven sherben per te menaxhuar automatikisht librarite qe na duhen (JUnit, Zookeeper, etc)
FROM maven:3.9-eclipse-temurin-11 AS build

# folderi i punes brenda Docker
WORKDIR /app

# kopjojme projektin nga kompjuteri yne ne container
COPY . .

# ekzekutojme Maven per te bere build projektin (pa testet)
RUN mvn clean package -DskipTests -B

# marrim nje imazh me te lehte
FROM eclipse-temurin:11-jre

# vendosim folder pune brenda container
WORKDIR /app

# marrim file .jar që u krijua ne hapin e pare dhe e fusim ketu
COPY --from=build /app/target/*-jar-with-dependencies.jar app.jar

# kur hapet container-i, kjo komand nis programin Java
ENTRYPOINT ["java","-jar","app.jar"]