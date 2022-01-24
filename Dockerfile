FROM openjdk:8

ARG UID=10000
ADD requirements.txt /tmp/requirements.txt

RUN apt-get update && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y --no-install-recommends -y python3-pip python3-setuptools python3-wheel sbt && \
    pip3 install -r /tmp/requirements.txt && \
    useradd -m -d /home/dockeruser -r -u $UID dockeruser

# install Spark
RUN mkdir -p /opt/graphsense && \
    wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-without-hadoop.tgz -O - | tar -xz -C /opt && \
    ln -s /opt/spark-3.1.2-bin-without-hadoop /opt/spark && \
    wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz -O - | tar -xz -C /opt && \
    ln -s /opt/hadoop-2.7.7 /opt/hadoop && \
    echo "#!/usr/bin/env bash\nexport SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh && \
    chmod 755 /opt/spark/conf/spark-env.sh


ENV SPARK_HOME /opt/spark

WORKDIR /opt/graphsense

ADD src/ ./src
ADD build.sbt .
RUN sbt package && \
    chown -R dockeruser /opt/graphsense && \
    rm -rf /root/.ivy2 /root/.cache /root/.sbt

ADD docker/ .
ADD scripts/ ./scripts

USER dockeruser

EXPOSE $SPARK_DRIVER_PORT $SPARK_UI_PORT $SPARK_BLOCKMGR_PORT
