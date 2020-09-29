FROM openjdk:8

RUN apt update && apt install -y python-pip
RUN pip install cqlsh
RUN apt-get install apt-transport-https ca-certificates
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt update && apt install -y sbt m4


# Install Spark
RUN mkdir /opt/spark
WORKDIR /opt/spark
RUN wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz -O ./spark.tgz
RUN tar xvf ./spark.tgz -C /usr/local
RUN mkdir /usr/local/spark && mv /usr/local/spark-3.0.1-bin-hadoop2.7/* /usr/local/spark/

ENV SPARK_HOME /usr/local/spark
WORKDIR /root
ADD entrypoint.sh .
ADD src/ ./src
ADD build.sbt .
ADD scalastyle-config.xml .
ADD submit.sh .
ENTRYPOINT ["/root/entrypoint.sh"]
