FROM openjdk:8

ADD requirements.txt /tmp/requirements.txt

RUN apt-get update && \
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y --no-install-recommends -y python3-pip python3-setuptools python3-wheel sbt && \
    pip3 install -r /tmp/requirements.txt && \
    useradd -m -d /home/dockeruser -r -u 10000 dockeruser

# install Spark
RUN mkdir -p /opt/graphsense && \
    wget https://www.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-without-hadoop-scala-2.12.tgz -O - | tar -xz -C /opt && \
    ln -s /opt/spark-2.4.7-bin-without-hadoop-scala-2.12 /opt/spark && \
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
