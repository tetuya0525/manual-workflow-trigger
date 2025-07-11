# ==============================================================================
# Memory Library - Manual Workflow Trigger Service
# Role:         Kicks off the article processing workflow.
# Version:      1.0 (Flask Architecture)
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

# 環境変数から設定を読み込む
PROJECT_ID = os.environ.get('GCP_PROJECT_ID') 
# 次のステップで作成するPub/SubトピックのID
ARTICLE_PROCESSING_TOPIC_ID = os.environ.get('ARTICLE_PROCESSING_TOPIC_ID') 

# Firebaseの初期化
try:
    firebase_admin.initialize_app()
    db = firestore.client()
    app.logger.info("Firebase app initialized successfully.")
except Exception as e:
    app.logger.error(f"Error initializing Firebase app: {e}")
    db = None

# Pub/Subクライアントの初期化
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, ARTICLE_PROCESSING_TOPIC_ID)
    app.logger.info(f"Pub/Sub publisher initialized for topic: {topic_path}")
except Exception as e:
    app.logger.error(f"Error initializing Pub/Sub client: {e}")
    publisher = None
    topic_path = None

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

    # 依存関係のチェック
    if not db or not publisher or not topic_path:
        app.logger.error("A critical component (Firestore or Pub/Sub) is not initialized.")
        return jsonify({"status": "error", "message": "Server configuration error."}), 500, headers

    app.logger.info("Workflow trigger received. Searching for 'received' status articles.")
    
    processed_count = 0
    error_count = 0
    
    try:
        # ステータスが 'received' の記事を検索
        docs = db.collection('staging_articles').where('status', '==', 'received').stream()

        for doc in docs:
            doc_id = doc.id
            try:
                # Pub/SubにドキュメントIDをメッセージとして発行
                future = publisher.publish(topic_path, doc_id.encode('utf-8'))
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
