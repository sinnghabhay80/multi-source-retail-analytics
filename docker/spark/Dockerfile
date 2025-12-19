FROM apache/spark:4.0.1

USER root

COPY requirements.txt /opt/requirements.txt

RUN pip install --no-cache-dir -r /opt/requirements.txt

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt
ENV PATH=/opt/spark/bin:$PATH

RUN curl -L -o /opt/spark/jars/iceberg.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar

RUN curl -L -o /opt/spark/jars/hadoop-aws.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.698/aws-java-sdk-bundle-1.12.698.jar

USER spark