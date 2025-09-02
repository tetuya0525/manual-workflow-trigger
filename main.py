# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# main.py (v2.2 Production)
#
# Role:         'received'ステータスの記事を検索し、処理ワークフローを開始するために
#               Pub/Subメッセージをバッチで発行する。
# Version:      2.2
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
# Configuration
# ==============================================================================
class Config:
    """アプリケーション設定を環境変数から読み込む"""
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    ARTICLE_PROCESSING_TOPIC_ID = os.environ.get("ARTICLE_PROCESSING_TOPIC_ID")
    TARGET_AUDIENCE = os.environ.get("AUDIENCE") # サービス間認証の対象者URL
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "staging_articles")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
    MAX_DOCUMENTS_PER_REQUEST = int(os.environ.get("MAX_DOCUMENTS_PER_REQUEST", 500))

    @staticmethod
    def validate():
        """必須の環境変数が設定されているか検証する"""
        required = ["GCP_PROJECT_ID", "ARTICLE_PROCESSING_TOPIC_ID", "TARGET_AUDIENCE"]
        missing = [v for v in required if not getattr(Config, v)]
        if missing:
            raise ValueError(f"不足している必須環境変数があります: {', '.join(missing)}")


# ==============================================================================
# Application Initialization
# ==============================================================================

# 1. Flaskアプリケーションインスタンスを作成
app = Flask(__name__)

# 2. 起動前検証 (Gunicorn起動時に実行)
#    必須の環境変数がなければ、ここでコンテナはクラッシュし、ログに原因が記録される。
try:
    Config.validate()
except ValueError as e:
    # ロガーがまだ設定されていないため、標準エラー出力に書き出す
    print(f"FATAL: 起動前検証に失敗: {e}", file=sys.stderr)
    raise

# 3. ロギング設定 (Gunicorn起動時に実行)
gunicorn_logger = logging.getLogger("gunicorn.error")
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)
app.logger.info("アプリケーションの初期化とロギング設定が完了しました。")

# --- クライアントの遅延初期化 ---
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
            app.logger.info("Firestoreクライアントの初期化に成功しました。")
        except Exception as e:
            app.logger.critical(f"Firebaseの初期化に失敗: {e}", exc_info=True)
            raise
    return db_client

def get_pubsub_publisher() -> pubsub_v1.PublisherClient:
    """Pub/Sub Publisherクライアントをシングルトンとして初期化・取得する"""
    global publisher_client
    if publisher_client is None:
        try:
            publisher_client = pubsub_v1.PublisherClient()
            app.logger.info("Pub/Sub Publisherの初期化に成功しました。")
        except Exception as e:
            app.logger.critical(f"Pub/Sub Publisherの初期化に失敗: {e}", exc_info=True)
            raise
    return publisher_client


# ==============================================================================
# Decorators
# ==============================================================================
def service_auth_required(f):
    """サービス間認証用のIDトークンを検証するデコレータ"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"status": "error", "message": "認証ヘッダーがありません"}), 401

        token = auth_header.split("Bearer ")[1]
        try:
            # audienceを検証し、このサービス向けのトークンであることを確認
            id_token.verify_oauth2_token(
                token,
                google.auth.transport.requests.Request(),
                audience=Config.TARGET_AUDIENCE,
            )
        except ValueError as e:
            app.logger.warning(f"無効な認証トークンです: {e}")
            return jsonify({"status": "error", "message": "無効な認証トークンです"}), 403

        return f(*args, **kwargs)
    return decorated_function


# ==============================================================================
# Core Logic (Transaction)
# ==============================================================================
@firestore.transactional
def update_docs_and_prepare_messages(
    transaction: firestore.Transaction,
    docs_query: firestore.Query,
    batch_id: str
) -> List[Dict[str, Any]]:
    """
    トランザクション内でDB更新を行い、送信すべきメッセージのリストを作成する。
    これにより、DB更新とメッセージ作成の原子性を保証する。
    """
    messages_to_publish = []
    docs = docs_query.stream(transaction=transaction)

    for doc in docs:
        if doc.to_dict().get("status") == "received":
            doc_id = doc.id
            message_payload = {"documentId": doc_id, "batchId": batch_id}
            messages_to_publish.append(message_payload)

            transaction.update(
                doc.reference,
                {
                    "status": "queued",
                    "queuedAt": firestore.SERVER_TIMESTAMP,
                    "batchId": batch_id,
                },
            )
    return messages_to_publish


# ==============================================================================
# API Endpoints
# ==============================================================================
@app.route("/trigger-workflow", methods=["POST"])
@service_auth_required
def start_workflow():
    """
    'received'ステータスの記事を検索し、処理キューに投入するメインエンドポイント。
    """
    batch_id = str(uuid.uuid4())
    app.logger.info(f"ワークフロー開始リクエストを受信 (Batch ID: {batch_id})")

    total_processed = 0
    try:
        db = get_firestore_client()
        publisher = get_pubsub_publisher()
        topic_path = publisher.topic_path(
            Config.GCP_PROJECT_ID, Config.ARTICLE_PROCESSING_TOPIC_ID
        )

        while total_processed < Config.MAX_DOCUMENTS_PER_REQUEST:
            docs_query = (
                db.collection(Config.COLLECTION_NAME)
                .where("status", "==", "received")
                .limit(Config.BATCH_SIZE)
            )

            transaction = db.transaction()
            messages_to_publish = update_docs_and_prepare_messages(
                transaction, docs_query, batch_id
            )

            if not messages_to_publish:
                app.logger.info(f"処理対象の記事が見つかりませんでした。(Batch ID: {batch_id})")
                break

            # トランザクション成功後、Pub/Subメッセージを送信
            publish_futures = []
            for msg in messages_to_publish:
                payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
                future = publisher.publish(topic_path, payload)
                publish_futures.append(future)

            # 全てのメッセージ発行完了を待つ
            for future in publish_futures:
                future.result()

            batch_processed_count = len(messages_to_publish)
            total_processed += batch_processed_count
            app.logger.info(f"{batch_processed_count}件の記事をキューに追加しました。(Batch ID: {batch_id})")

        message = f"ワークフローを開始し、合計{total_processed}件の記事をキューに追加しました。"
        app.logger.info(f"{message} (Batch ID: {batch_id})")
        return jsonify({
            "status": "success",
            "message": message,
            "processedCount": total_processed,
            "batchId": batch_id
        }), 200

    except Exception as e:
        app.logger.error(
            f"ワークフロー実行中にエラーが発生 (Batch ID: {batch_id}): {e}",
            exc_info=True
        )
        return jsonify({"status": "error", "message": "内部サーバーエラーが発生しました。"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({"status": "healthy"}), 200


# ==============================================================================
# Direct Execution (for local development)
# ==============================================================================
if __name__ == "__main__":
    # このブロックは `python main.py` で直接実行された場合のみ使用される
    # (Gunicornからの起動では実行されない)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
