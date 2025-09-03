FROM mongo:6.0
COPY mongo-keyfile /etc/mongo-keyfile
RUN chmod 600 /etc/mongo-keyfile && chown 999:999 /etc/mongo-keyfile
