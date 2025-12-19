FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# For S3 Support
RUN curl -L -o /opt/hive/lib/hadoop-aws.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/hive/lib/aws-sdk.jar \
   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.698/aws-java-sdk-bundle-1.12.698.jar

# For Postgres
RUN curl -L -o /opt/hadoop/share/hadoop/common/lib/postgresql.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER hive