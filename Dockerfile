# Dockerfile
# ==============================================================================
# 建築手順書 for Manual Workflow Trigger
# ==============================================================================

# ベースイメージとして、公式のPython 3.12安定版を使用します。
FROM python:3.12-slim

# コンテナ内の作業ディレクトリを設定します。
WORKDIR /app

# まず、依存関係ファイル(部品リスト)をコピーします。
COPY requirements.txt .

# 部品リストに基づいて、必要なライブラリをインストールします。
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションの本体であるソースコードをコピーします。
COPY main.py .

# このサービスがリクエストを待ち受けるポートを8080に設定します。
ENV PORT=8080

# functions-frameworkを使って、main.py内の関数を起動します。
CMD ["functions-framework", "--target=manual_workflow_trigger", "--port=8080"]
