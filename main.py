# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# Role:         Kicks off the article processing workflow.
# Version:      1.1 (Lazy Initialization / Stable)
# Author:       心理 (Thinking Partner)
# Last Updated: 2025-07-11
# ==============================================================================
import os
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import firestore
from google.cloud import pubsub_v1
import logging

# Pythonの標準ロギングを設定
logging.basicConfig(level=logging.INFO)

# Flaskアプリケーションを初期化
app = Flask(__name__)

# グローバル変数としてクライアントを保持 (遅延初期化のためNoneで開始)
db = None
publisher = None

def get_firestore_client():
    """Firestoreクライアントをシングルトンとして取得・初期化する"""
    global db
    if db is None:
        try:
            # Cloud Run環境では引数なしで自動的に初期化される
            firebase_admin.initialize_app()
            db = firestore.client()
            app.logger.info("Firebase app initialized successfully.")
        except Exception as e:
            app.logger.error(f"Error initializing Firebase app: {e}")
            # 初期化に失敗した場合、dbはNoneのまま
    return db

def get_pubsub_publisher():
    """Pub/Sub Publisherクライアントをシングルトンとして取得・初期化する"""
    global publisher
    if publisher is None:
        try:
            publisher = pubsub_v1.PublisherClient()
            app.logger.info("Pub/Sub publisher initialized successfully.")
        except Exception as e:
            app.logger.error(f"Error initializing Pub/Sub client: {e}")
            # 初期化に失敗した場合、publisherはNoneのまま
    return publisher

@app.route('/', methods=['POST', 'OPTIONS'])
def start_workflow():
    """
    ワークフローを開始するHTTPエンドポイント。
    'received'ステータスの記事を検索し、処理のためにPub/Subメッセージを発行する。
    """
    # CORSプリフライトリクエストへの対応
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}

    # 必要なクライアントを取得 (ここで初めて初期化が試みられる)
    db_client = get_firestore_client()
    pubsub_publisher = get_pubsub_publisher()

    # 環境変数を取得
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID') 
    ARTICLE_PROCESSING_TOPIC_ID = os.environ.get('ARTICLE_PROCESSING_TOPIC_ID')

    # 依存関係のチェック
    if not all([db_client, pubsub_publisher, PROJECT_ID, ARTICLE_PROCESSING_TOPIC_ID]):
        app.logger.error("A critical component or environment variable is missing.")
        return jsonify({"status": "error", "message": "Server configuration error."}), 500, headers

    topic_path = pubsub_publisher.topic_path(PROJECT_ID, ARTICLE_PROCESSING_TOPIC_ID)
    app.logger.info(f"Workflow trigger received. Searching for 'received' status articles in topic: {topic_path}")
    
    processed_count = 0
    error_count = 0
    
    try:
        # ステータスが 'received' の記事を検索
        docs = db_client.collection('staging_articles').where('status', '==', 'received').stream()

        for doc in docs:
            doc_id = doc.id
            try:
                # Pub/SubにドキュメントIDをメッセージとして発行
                future = pubsub_publisher.publish(topic_path, doc_id.encode('utf-8'))
                future.result()  # 発行が完了するまで待機

                # Firestoreのドキュメントのステータスを 'queued' に更新
                doc.reference.update({'status': 'queued'})
                
                app.logger.info(f"Queued document {doc_id} for processing.")
                processed_count += 1
            except Exception as e:
                app.logger.error(f"Failed to queue document {doc_id}: {e}")
                error_count += 1

        message = f"Workflow started. Queued {processed_count} articles for processing."
        if error_count > 0:
            message += f" Failed to queue {error_count} articles."
        
        app.logger.info(message)
        return jsonify({"status": "success", "message": message, "processed": processed_count, "errors": error_count}), 200, headers

    except Exception as e:
        app.logger.error(f"An error occurred during workflow execution: {e}")
        return jsonify({"status": "error", "message": "An internal error occurred."}), 500, headers

