# 一次性验收镜像：仅依赖标准库 + pytest
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 先装依赖，利用构建缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝实现与测试
COPY local_repo ./local_repo
COPY tests ./tests
COPY pytest.ini ./pytest.ini

# verify 服务：一次性运行完整 pytest 套件后退出
CMD ["python", "-m", "pytest", "-v"]
