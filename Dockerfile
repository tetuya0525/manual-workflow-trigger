# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# Dockerfile
# ==============================================================================

# ベースイメージとして公式のPython 3.12スリム版を使用
FROM python:3.12-slim

# 環境変数
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PORT 8080

# 作業ディレクトリを作成して設定
WORKDIR $APP_HOME

# 要件ファイルをコピーしてインストール
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip -r requirements.txt

# アプリケーションのソースコードをコピー
COPY main.py .

# コンテナ起動時に実行するコマンドを設定
# GunicornをWebサーバーとして使用し、main.py内の'app'オブジェクトを実行
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
