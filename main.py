# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# main.py (v2.1 Final)
#
# Role:         'received'ステータスの記事を検索し、処理ワークフローを開始するために
#               Pub/Subメッセージをバッチで発行する。
# Version:      2.1
# Last Updated: 2025-08-24
# ==============================================================================
import os
import json
import logging
import uuid
from functools import wraps
from typing import List, Dict, Any, Tuple

from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import firestore
from google.cloud import pubsub_v1
import google.auth.transport.requests
from google.oauth2 import id_token

# --- 設定クラス ---
class Config:
    """アプリケーション設定を集約"""
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    ARTICLE_PROCESSING_TOPIC_ID = os.environ.get("ARTICLE_PROCESSING_TOPIC_ID")
    # ★ [全AI共通] audience検証用の環境変数を追加
    TARGET_AUDIENCE = os.environ.get("AUDIENCE")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "staging_articles")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
    MAX_DOCUMENTS_PER_REQUEST = int(os.environ.get("MAX_DOCUMENTS_PER_REQUEST", 500))

    @staticmethod
    def validate():
        required = ["GCP_PROJECT_ID", "ARTICLE_PROCESSING_TOPIC_ID", "TARGET_AUDIENCE"]
        missing = [v for v in required if not getattr(Config, v)]
        if missing:
            raise ValueError(f"不足している必須環境変数があります: {', '.join(missing)}")

# --- 初期化 ---
app = Flask(__name__)

# --- ロギング設定 (構造化) ---
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

def log_structured(level: str, message: str, **kwargs: Any) -> None:
    # ★ [ChatGPT] ログレベルに応じて適切なロガーを使用するよう修正 [cite: 488-496]
    log_data = {"message": message, "severity": level, **kwargs}
    log_level_map = {
        "INFO": app.logger.info,
        "WARNING": app.logger.warning,
        "ERROR": app.logger.error,
        "CRITICAL": app.logger.critical,
    }
    logger_func = log_level_map.get(level, app.logger.info)
    logger_func(json.dumps(log_data, ensure_ascii=False))

# --- クライアント初期化 (シングルトン) ---
db = None
publisher = None
def get_firestore_client() -> firestore.Client:
    global db
    if db is None:
        try:
            if not firebase_admin._apps:
                firebase_admin.initialize_app()
            db = firestore.client()
        except Exception as e:
            log_structured('CRITICAL', "Firebaseの初期化に失敗", error=str(e))
            raise
    return db

def get_pubsub_publisher() -> pubsub_v1.PublisherClient:
    global publisher
    if publisher is None:
        try:
            publisher = pubsub_v1.PublisherClient()
        except Exception as e:
            log_structured('CRITICAL', "Pub/Sub Publisherの初期化に失敗", error=str(e))
            raise
    return publisher

# --- 認証デコレーター ---
def service_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({"status": "error", "message": "認証が必要です"}), 401
        
        token = auth_header.split('Bearer ')[1]
        try:
            # ★ [全AI共通] audienceを検証し、意図したサービス向けのトークンか確認 [cite: 554-557, 591-594]
            id_token.verify_oauth2_token(
                token, google.auth.transport.requests.Request(), audience=Config.TARGET_AUDIENCE
            )
        except ValueError as e:
            log_structured('WARNING', "無効な認証トークンです", error=str(e))
            return jsonify({"status": "error", "message": "無効な認証トークンです"}), 403
        
        return f(*args, **kwargs)
    return decorated

# --- トランザクション処理 ---
@firestore.transactional
def update_docs_and_prepare_messages(transaction, docs_query, batch_id: str) -> List[Dict[str, Any]]:
    """トランザクション内でDB更新を行い、送信すべきメッセージのリストを返す"""
    messages_to_publish = []
    docs = docs_query.get(transaction=transaction)

    for doc in docs:
        if doc.to_dict().get('status') == 'received':
            doc_id = doc.id
            # ★ [ChatGPT] Pub/Subメッセージにコンテキストを追加 [cite: 497-501]
            message_payload = {
                "documentId": doc_id,
                "batchId": batch_id
            }
            messages_to_publish.append(message_payload)
            
            transaction.update(doc.reference, {
                'status': 'queued',
                'queuedAt': firestore.SERVER_TIMESTAMP,
                'batchId': batch_id
            })
    return messages_to_publish

# --- メインロジック ---
@app.route('/', methods=['POST'])
@service_auth_required
def start_workflow():
    batch_id = str(uuid.uuid4())
    log_structured('INFO', "ワークフロー開始リクエストを受信", batch_id=batch_id)
    
    total_processed = 0
    try:
        db_client = get_firestore_client()
        pubsub_publisher = get_pubsub_publisher()
        topic_path = pubsub_publisher.topic_path(Config.GCP_PROJECT_ID, Config.ARTICLE_PROCESSING_TOPIC_ID)

        while total_processed < Config.MAX_DOCUMENTS_PER_REQUEST:
            docs_query = db_client.collection(Config.COLLECTION_NAME) \
                .where('status', '==', 'received') \
                .limit(Config.BATCH_SIZE)

            transaction = db_client.transaction()
            messages_to_publish = update_docs_and_prepare_messages(
                transaction, docs_query, batch_id
            )

            # トランザクション成功後、Pub/Subメッセージを送信
            if messages_to_publish:
                publish_futures = []
                for msg in messages_to_publish:
                    payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
                    future = pubsub_publisher.publish(topic_path, payload)
                    publish_futures.append(future)
                
                # 全てのメッセージ発行完了を待つ
                for future in publish_futures:
                    future.result()
                
                batch_processed_count = len(messages_to_publish)
                total_processed += batch_processed_count
                log_structured('INFO', f"{batch_processed_count}件の記事をキューに追加しました", batch_id=batch_id)
            else:
                break
        
        message = f"ワークフローを開始し、合計{total_processed}件の記事をキューに追加しました。"
        log_structured('INFO', message, batch_id=batch_id)
        return jsonify({
            "status": "success",
            "message": message,
            "processedCount": total_processed
        }), 200

    except Exception as e:
        log_structured('ERROR', "ワークフローの実行中にエラーが発生", error=str(e), batch_id=batch_id, exc_info=True)
        return jsonify({"status": "error", "message": "An internal error occurred."}), 500

# --- ヘルスチェック ---
@app.route('/health', methods=['GET'])
def health_check():
    try:
        get_firestore_client()
        get_pubsub_publisher()
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        log_structured('ERROR', "ヘルスチェック失敗", error=str(e))
        return jsonify({"status": "unhealthy"}), 503

# --- 起動 ---
if __name__ == "__main__":
    try:
        Config.validate()
        setup_logging()
        port = int(os.environ.get("PORT", 8080))
        app.run(host="0.0.0.0", port=port, debug=False)
    except ValueError as e:
        log_structured('CRITICAL', "起動前検証に失敗", error=str(e))
        exit(1)
