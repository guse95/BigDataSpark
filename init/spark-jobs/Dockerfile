FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/jars

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.2/clickhouse-jdbc-0.6.2-all.jar /opt/spark/jars/clickhouse-jdbc-0.6.2-all.jar

RUN chmod 644 /opt/spark/jars/*.jar

USER spark