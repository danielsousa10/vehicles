FROM spark:3.5.3-scala2.12-java17-ubuntu

USER root

WORKDIR /opt/spark/work-dir

ADD https://jdbc.postgresql.org/download/postgresql-42.7.5.jar ./jars/

RUN chown -R spark:spark .

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

USER spark