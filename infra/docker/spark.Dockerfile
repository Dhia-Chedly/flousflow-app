FROM apache/spark:3.5.6-java17-python3 AS runtime
USER root
RUN mkdir -p /checkpoints /opt/spark/ivy /locks && chown -R 185:0 /checkpoints /opt/spark/ivy /locks
COPY --chown=185:0 spark/ /opt/flousflow/spark/
COPY --chown=185:0 infra/docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY --chown=185:0 infra/docker/ivysettings.xml /opt/spark/conf/ivysettings.xml
ENV PYTHONPATH=/opt/flousflow/spark
USER 185
WORKDIR /opt/flousflow
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]"]
CMD ["/opt/flousflow/spark/bronze/ingest.py"]

FROM runtime AS serving
USER root
RUN python3 -m pip install --no-cache-dir 'psycopg[binary]==3.2.6'
USER 185
CMD ["/opt/flousflow/spark/serving/refresh.py"]

FROM runtime AS ingestion
