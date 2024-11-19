FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
COPY --chown=2000:2000 script.py /opt/spark/work-dir/script.py
COPY --chown=2000:2000 ml-100k/u.data /opt/spark/work-dir/u.data
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/script.py"]