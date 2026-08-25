# bingops-runner 执行面镜像
# 同 VPC 部署，直连目标机 22 端口（设计文档 §5）
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        openssh-client \
        sshpass \
        unzip \
        curl \
    && rm -rf /var/lib/apt/lists/*

# terraform 二进制预装（P2 点亮，P1 不使用）
ARG TERRAFORM_VERSION=1.9.8
RUN curl -fsSL "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" -o /tmp/tf.zip \
    && unzip /tmp/tf.zip -d /usr/local/bin/ \
    && rm /tmp/tf.zip

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY runner/ ./runner/

# 非 root 运行；keyfile 0600 在自身 home 内生效
RUN useradd -m -u 10001 runner \
    && mkdir -p /var/lib/bingops-runner \
    && chown -R runner:runner /app /var/lib/bingops-runner
USER runner

ENV RUNNER_WORKDIR=/var/lib/bingops-runner \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "runner"]
