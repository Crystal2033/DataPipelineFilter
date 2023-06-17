FROM openjdk:19
MAINTAINER KulikovPavel
COPY build/libs/JavaServiceFiltering-1.0-SNAPSHOT.jar Filter.jar
ENTRYPOINT ["java","-jar","/Filter.jar"]