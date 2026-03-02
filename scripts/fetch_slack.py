#!/usr/bin/env python3
"""
Slack Log Fetcher
- 全チャンネルのメッセージ＋スレッド返信を取得
- レート制限対策済み
- 差分取得（前回取得済みのものはスキップ）
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
DATA_DIR = Path("docs/data")
STATE_FILE = DATA_DIR / "state.json"

HEADERS = {"Authorization": f"Bearer {SLACK_TOKEN}"}

def slack_get(endpoint, params=None, retry=5):
    """Slack API GET with rate limit handling"""
    url = f"https://slack.com/api/{endpoint}"
    for attempt in range(retry):
        resp = requests.get(url, headers=HEADERS, params=params or {})
        data = resp.json()
        if not data.get("ok"):
            error = data.get("error", "unknown")
            if error == "ratelimited":
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            print(f"API error [{endpoint}]: {error}")
            return None
        return data
    return None

def get_all_channels():
    """全チャンネル一覧取得（パブリック＋プライベート）"""
    channels = []
    cursor = None
    while True:
        params = {
            "types": "public_channel,private_channel",
            "limit": 200,
            "exclude_archived": False,
        }
        if cursor:
            params["cursor"] = cursor
        data = slack_get("conversations.list", params)
        if not data:
            break
        channels.extend(data.get("channels", []))
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(1)
    return channels

def get_channel_messages(channel_id, oldest=None):
    """チャンネルのメッセージ取得（差分）"""
    messages = []
    cursor = None
    while True:
        params = {"channel": channel_id, "limit": 200, "inclusive": True}
        if oldest:
            params["oldest"] = oldest
        if cursor:
            params["cursor"] = cursor
        data = slack_get("conversations.history", params)
        if not data:
            break
        messages.extend(data.get("messages", []))
        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor")
        time.sleep(1.2)
    return messages

def get_thread_replies(channel_id, thread_ts):
    """スレッド返信取得"""
    replies = []
    cursor = None
    while True:
        params = {"channel": channel_id, "ts": thread_ts, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = slack_get("conversations.replies", params)
        if not data:
            break
        msgs = data.get("messages", [])
        # 最初の要素は親メッセージなのでスキップ
        replies.extend(msgs[1:] if not cursor else msgs)
        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor")
        time.sleep(1.2)
    return replies

def get_users():
    """ユーザー情報取得"""
    users = {}
    cursor = None
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = slack_get("users.list", params)
        if not data:
            break
        for u in data.get("members", []):
            users[u["id"]] = {
                "name": u.get("real_name") or u.get("name", u["id"]),
                "display_name": u.get("profile", {}).get("display_name") or u.get("name", u["id"]),
                "avatar": u.get("profile", {}).get("image_48", ""),
                "is_bot": u.get("is_bot", False),
            }
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(1)
    return users

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"channels": {}, "last_run": None}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))

def load_channel_data(channel_id):
    path = DATA_DIR / f"{channel_id}.json"
    if path.exists():
        return json.loads(path.read_text())
    return {"messages": []}

def save_channel_data(channel_id, data):
    path = DATA_DIR / f"{channel_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[{datetime.now()}] Slack log fetch started")

    state = load_state()

    # ユーザー情報取得・保存
    print("Fetching users...")
    users = get_users()
    (DATA_DIR / "users.json").write_text(json.dumps(users, ensure_ascii=False, indent=2))
    print(f"  {len(users)} users fetched")

    # チャンネル一覧取得
    print("Fetching channels...")
    channels = get_all_channels()
    channel_meta = {c["id"]: {"name": c["name"], "is_private": c.get("is_private", False), "archived": c.get("is_archived", False)} for c in channels}
    (DATA_DIR / "channels.json").write_text(json.dumps(channel_meta, ensure_ascii=False, indent=2))
    print(f"  {len(channels)} channels found")

    for channel in channels:
        cid = channel["id"]
        cname = channel["name"]
        oldest = state["channels"].get(cid, {}).get("last_ts")

        print(f"Fetching #{cname} (oldest={oldest or 'all'})...")
        try:
            messages = get_channel_messages(cid, oldest)
        except Exception as e:
            print(f"  Error: {e}")
            continue

        if not messages:
            print(f"  No new messages")
            continue

        # 既存データ読み込み
        existing = load_channel_data(cid)
        existing_ts = {m["ts"] for m in existing["messages"]}

        new_msgs = []
        for msg in messages:
            ts = msg.get("ts", "")
            if ts in existing_ts:
                continue

            # スレッド返信取得
            if msg.get("reply_count", 0) > 0:
                time.sleep(1)
                replies = get_thread_replies(cid, ts)
                msg["replies_data"] = replies

            new_msgs.append(msg)
            existing_ts.add(ts)

        if new_msgs:
            existing["messages"].extend(new_msgs)
            # タイムスタンプでソート
            existing["messages"].sort(key=lambda m: float(m.get("ts", 0)))
            save_channel_data(cid, existing)
            print(f"  +{len(new_msgs)} new messages")

            # 最新tsを記録
            latest_ts = max(float(m["ts"]) for m in new_msgs)
            state["channels"].setdefault(cid, {})["last_ts"] = str(latest_ts)
        else:
            print(f"  No new messages")

        time.sleep(1)

    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    print(f"[{datetime.now()}] Done!")

if __name__ == "__main__":
    main()
