FROM eclipse-temurin:17-jdk

ENV DAEMON_RUN=true \
SPARK_HOME=/spark \
SPARK_VERSION=3.5.8 \
SCALA_HOME=/usr/share/scala \
SCALA_VERSION=2.13.12 \
HADOOP_VERSION=3 \
SBT_VERSION=1.12.0


RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates jq dbus-x11


RUN cd "/tmp" && \
    wget --no-verbose --show-progress --progress=bar:force "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    mkdir "${SCALA_HOME}" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*


RUN export PATH="/usr/local/sbt/bin:$PATH" \
    &&  apt update && apt install -y ca-certificates wget tar \
    && mkdir -p "/usr/local/sbt" \
    && wget -qO - --no-check-certificate --show-progress --progress=bar:force "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar xz -C /usr/local/sbt --strip-components=1 \
    && sbt --allow-empty sbtVersion -Dsbt.rootdir=true

RUN wget --no-verbose --show-progress --progress=bar:force https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
