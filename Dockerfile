# ==============================================================================
# Dockerfile for Manual Workflow Trigger Service
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# セキュリティ強化のため非rootユーザーを作成・使用
RUN adduser --system --group appuser
USER appuser

# 依存関係をインストール
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ソースコードをコピー
COPY --chown=appuser:appuser . .

# Cloud RunのPORT環境変数を設定
ENV PORT 8080

# エントリーポイント
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "main:app"]
