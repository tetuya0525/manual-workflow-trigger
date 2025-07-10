# main.py
# ==============================================================================
# Memory Library - Manual Workflow Trigger
# Role:         Triggers the main content processing workflow via a secure
#               endpoint called by the API Gateway.
# Version:      1.0 (Based on Architectural Charter v6.0)
# Author:       心理 (Thinking Partner)
# ==============================================================================
import functions_framework
from flask import request, jsonify
import os
from google.cloud import pubsub_v1

# --- 定数 (Constants) ---
# 憲章2.6に基づき、設定は環境変数から取得します。
PROJECT_ID = os.environ.get('GCP_PROJECT')
TOPIC_ID = os.environ.get('PROCESS_BOOKS_TOPIC_ID') # 最初の処理(図書分類)を呼び出すトピック
SERVICE_NAME = "manual-workflow-trigger"

# --- 初期化 (Initialization) ---
publisher = None
topic_path = None
try:
    if PROJECT_ID and TOPIC_ID:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    else:
        print(f"CRITICAL in {SERVICE_NAME}: GCP_PROJECT or PROCESS_BOOKS_TOPIC_ID environment variables are not set.")
except Exception as e:
    print(f"CRITICAL in {SERVICE_NAME}: Failed to initialize Pub/Sub client. Error: {e}")


@functions_framework.http
def manual_workflow_trigger(request):
    """
    APIゲートウェイからの呼び出しを受け、Pub/Subにメッセージを発行して
    内部ワークフローを開始させる。
    """
    if not publisher or not topic_path:
        return jsonify({"status": "error", "message": "サーバー設定エラー: Pub/Subクライアントが初期化されていません。"}), 503

    if request.method != 'POST':
        return jsonify({"status": "error", "message": "POSTメソッドを使用してください。"}), 405

    try:
        message_data = b'{"message": "Manual workflow triggered."}'
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()

        print(f"SUCCESS: Workflow triggered. Message ID: {message_id} published to {topic_path}.")

        return jsonify({
            "status": "success",
            "message": "開館準備を開始するよう、全ての司書に通達しました。",
            "messageId": message_id
        }), 200

    except Exception as e:
        print(f"ERROR in {SERVICE_NAME}: Failed to publish message. Error: {e}")
        # ここではエラー報告サービスは呼び出さず、ログ出力のみとします。
        return jsonify({"status": "error", "message": "号令の発信中にサーバー内部でエラーが発生しました。"}), 500
