FROM gcr.io/spark-operator/spark:v3.0.0
USER root

COPY /target/scala-2.12/mason-spark-assembly-latest.jar /opt/spark/jars/mason-spark-latest.jar
