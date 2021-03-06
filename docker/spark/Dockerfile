FROM docker.io/bitnami/minideb:buster
LABEL maintainer "Bitnami <containers@bitnami.com>"

ENV HOME="/" \
    OS_ARCH="amd64" \
    OS_FLAVOUR="debian-10" \
    OS_NAME="linux" \
    PATH="/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:/opt/bitnami/common/bin:$PATH"

ARG JAVA_EXTRA_SECURITY_DIR="/bitnami/java/extra-security"

COPY prebuildfs /
# Install required system packages and dependencies
RUN install_packages acl ca-certificates curl gzip libbz2-1.0 libc6 libffi6 libgcc1 liblzma5 libncursesw6 libreadline7 libsqlite3-0 libssl1.1 libstdc++6 libtinfo6 procps tar zlib1g
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "python" "3.8.12-12" --checksum c081f7f9663d4d64e7f26d6e6a77b3860ee1f39c309bd564f801c39192042d2f
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "java" "1.8.312-1" --checksum 2699ce844c410513ef7fd7180c2a0a6e4c740581a4a999ab5e3bed6bbe8be3bc
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "spark" "3.2.0-4" --checksum 25360dbb59f225150c00c81cb9a9eb2ca4d1926e32ddef341cece33e7867a534
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "gosu" "1.14.0-1" --checksum 16f1a317859b06ae82e816b30f98f28b4707d18fe6cc3881bff535192a7715dc
RUN chmod g+rwX /opt/bitnami

COPY rootfs /
RUN /opt/bitnami/scripts/spark/postunpack.sh
RUN /opt/bitnami/scripts/java/postunpack.sh
ENV BITNAMI_APP_NAME="spark" \
    BITNAMI_IMAGE_VERSION="3.2.0-debian-10-r71" \
    JAVA_HOME="/opt/bitnami/java" \
    LD_LIBRARY_PATH="/opt/bitnami/python/lib/:/opt/bitnami/spark/venv/lib/python3.8/site-packages/numpy.libs/:$LD_LIBRARY_PATH" \
    LIBNSS_WRAPPER_PATH="/opt/bitnami/common/lib/libnss_wrapper.so" \
    NSS_WRAPPER_GROUP="/opt/bitnami/spark/tmp/nss_group" \
    NSS_WRAPPER_PASSWD="/opt/bitnami/spark/tmp/nss_passwd" \
    SPARK_HOME="/opt/bitnami/spark"

# vim
RUN apt-get update
RUN apt-get install vim -y

# openssh
RUN apt-get install systemd -y
RUN apt-get install net-tools -y
RUN apt-get install ssh -y
RUN echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
RUN echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config
RUN echo "AcceptEnv JAVA_HOME" >> /etc/ssh/sshd_config
RUN echo "AcceptEnv PYSPARK_PYTHON" >> /etc/ssh/sshd_config
RUN mkdir -p /var/run/sshd
# !run.sh에 /usr/sbin/sshd -D 로 실행필요

# root pwd 변경
RUN echo "root:1234" | chpasswd

# 히스토리서버 실행 -> 왜안됨?
# RUN . /opt/bitnami/spark/sbin/start-history-server.sh


WORKDIR /opt/bitnami/spark
USER 1001
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]
