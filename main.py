# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# main.py (v2.3 Production)
#
# Role:         'received'ステータスの記事を検索し、処理ワークフローを開始するために
#               Pub/Subメッセージをバッチで発行する。
# Version:      2.3
# Last Updated: 2025-09-02
# ==============================================================================

# --- 標準ライブラリ ---
import json
import logging
import os
import sys
import uuid
from functools import wraps
from typing import Any, Dict, List

# --- サードパーティライブラリ ---
import firebase_admin
import google.auth.transport.requests
from firebase_admin import firestore
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
from google.oauth2 import id_token


# ==============================================================================
# Configuration Class
# ==============================================================================
class Config:
    """アプリケーション設定を保持するクラス（型変換はファクトリ内で行う）"""
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    ARTICLE_PROCESSING_TOPIC_ID = os.environ.get("ARTICLE_PROCESSING_TOPIC_ID")
    TARGET_AUDIENCE = os.environ.get("AUDIENCE")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "staging_articles")
    BATCH_SIZE = os.environ.get("BATCH_SIZE", "50")
    MAX_DOCUMENTS_PER_REQUEST = os.environ.get("MAX_DOCUMENTS_PER_REQUEST", "500")


# ==============================================================================
# Client Initialization (Lazy Loading)
# ==============================================================================
db_client: firestore.Client = None
publisher_client: pubsub_v1.PublisherClient = None

def get_firestore_client() -> firestore.Client:
    """Firestoreクライアントをシングルトンとして初期化・取得する"""
    global db_client
    if db_client is None:
        try:
            if not firebase_admin._apps:
                firebase_admin.initialize_app()
            db_client = firestore.client()
            logging.info("Firestoreクライアントの初期化に成功しました。")
        except Exception as e:
            logging.critical(f"Firebaseの初期化に失敗: {e}", exc_info=True)
            raise
    return db_client

def get_pubsub_publisher() -> pubsub_v1.PublisherClient:
    """Pub/Sub Publisherクライアントをシングルトンとして初期化・取得する"""
    global publisher_client
    if publisher_client is None:
        try:
            publisher_client = pubsub_v1.PublisherClient()
            logging.info("Pub/Sub Publisherの初期化に成功しました。")
        except Exception as e:
            logging.critical(f"Pub/Sub Publisherの初期化に失敗: {e}", exc_info=True)
            raise
    return publisher_client


# ==============================================================================
# Application Factory
# ==============================================================================
def create_app(config_class=Config):
    """Flaskアプリケーションインスタンスを生成・設定して返す"""
    app = Flask(__name__)

    # --- 1. ロギングを最初に設定 ---
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    # --- 2. 設定の読み込みと検証を包括的なエラーハンドリング内で行う ---
    try:
        app.config.from_object(config_class)

        # 必須項目の検証
        required = ["GCP_PROJECT_ID", "ARTICLE_PROCESSING_TOPIC_ID", "TARGET_AUDIENCE"]
        missing = [v for v in required if not app.config.get(v)]
        if missing:
            raise ValueError(f"不足している必須環境変数があります: {', '.join(missing)}")

        # 数値への型変換（ここでエラーが出ても捕捉できる）
        app.config["BATCH_SIZE"] = int(app.config["BATCH_SIZE"])
        app.config["MAX_DOCUMENTS_PER_REQUEST"] = int(app.config["MAX_DOCUMENTS_PER_REQUEST"])
        
        app.logger.info("アプリケーションの設定読み込みと検証が完了しました。")

    except (ValueError, TypeError) as e:
        app.logger.critical(f"FATAL: 設定の読み込みまたは型変換に失敗: {e}", exc_info=True)
        # コンテナをクラッシュさせて問題を知らせる
        raise

    # --- Decorators ---
    def service_auth_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return jsonify({"status": "error", "message": "認証ヘッダーがありません"}), 401
            token = auth_header.split("Bearer ")[1]
            try:
                id_token.verify_oauth2_token(
                    token,
                    google.auth.transport.requests.Request(),
                    audience=app.config["TARGET_AUDIENCE"],
                )
            except ValueError as e:
                app.logger.warning(f"無効な認証トークンです: {e}")
                return jsonify({"status": "error", "message": "無効な認証トークンです"}), 403
            return f(*args, **kwargs)
        return decorated_function

    # --- Core Logic ---
    @firestore.transactional
    def update_docs_and_prepare_messages(transaction, docs_query, batch_id):
        messages_to_publish = []
        docs = docs_query.stream(transaction=transaction)
        for doc in docs:
            if doc.to_dict().get("status") == "received":
                doc_id = doc.id
                message_payload = {"documentId": doc_id, "batchId": batch_id}
                messages_to_publish.append(message_payload)
                transaction.update(doc.reference, {
                    "status": "queued",
                    "queuedAt": firestore.SERVER_TIMESTAMP,
                    "batchId": batch_id,
                })
        return messages_to_publish

    # --- API Endpoints ---
    @app.route("/trigger-workflow", methods=["POST"])
    @service_auth_required
    def start_workflow():
        batch_id = str(uuid.uuid4())
        app.logger.info(f"ワークフロー開始リクエストを受信 (Batch ID: {batch_id})")
        total_processed = 0
        try:
            db = get_firestore_client()
            publisher = get_pubsub_publisher()
            topic_path = publisher.topic_path(
                app.config["GCP_PROJECT_ID"], app.config["ARTICLE_PROCESSING_TOPIC_ID"]
            )
            while total_processed < app.config["MAX_DOCUMENTS_PER_REQUEST"]:
                docs_query = (
                    db.collection(app.config["COLLECTION_NAME"])
                    .where("status", "==", "received")
                    .limit(app.config["BATCH_SIZE"])
                )
                transaction = db.transaction()
                messages_to_publish = update_docs_and_prepare_messages(
                    transaction, docs_query, batch_id
                )
                if not messages_to_publish:
                    app.logger.info(f"処理対象の記事が見つかりませんでした。(Batch ID: {batch_id})")
                    break
                
                futures = [
                    publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))
                    for msg in messages_to_publish
                ]
                for future in futures:
                    future.result()
                
                count = len(messages_to_publish)
                total_processed += count
                app.logger.info(f"{count}件の記事をキューに追加しました。(Batch ID: {batch_id})")

            message = f"ワークフローを開始し、合計{total_processed}件の記事をキューに追加しました。"
            app.logger.info(f"{message} (Batch ID: {batch_id})")
            return jsonify({
                "status": "success", "message": message,
                "processedCount": total_processed, "batchId": batch_id
            }), 200
        except Exception as e:
            app.logger.error(
                f"ワークフロー実行中にエラー (Batch ID: {batch_id}): {e}", exc_info=True
            )
            return jsonify({"status": "error", "message": "内部サーバーエラー"}), 500

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({"status": "healthy"}), 200

    return app


# ==============================================================================
# Gunicorn Entrypoint
# ==============================================================================
# Gunicornがこの `app` インスタンスを見つけて実行する
app = create_app()


# ==============================================================================
# Direct Execution (for local development)
# ==============================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)

