FROM busybox



ADD target/ /goim_bin/
RUN cd /    && ln -s /goim_bin/* . \
    && cd /bin && ln -s /goim_bin/* .

EXPOSE 3101 3102 3103  3111

VOLUME /data
VOLUME /etc/ssl/certs