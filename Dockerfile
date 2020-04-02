FROM gcr.io/spark-operator/spark:v2.4.5

COPY /target/scala-2.11/mason-spark-assembly-0.1.jar /opt/spark/jars

