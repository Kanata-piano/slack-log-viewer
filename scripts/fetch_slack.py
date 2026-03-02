#!/usr/bin/env python3
"""
Slack Log Fetcher
- 全チャンネルのメッセージ＋スレッド返信を取得
- リアクション取得対応
- カスタム絵文字画像をダウンロード・保存
- レート制限対策済み
- 差分取得
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
DATA_DIR = Path("docs/data")
EMOJI_DIR = Path("docs/emoji")
STATE_FILE = DATA_DIR / "state.json"

HEADERS = {"Authorization": f"Bearer {SLACK_TOKEN}"}

def slack_get(endpoint, params=None, retry=5):
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
    channels = []
    cursor = None
    while True:
        params = {"types": "public_channel,private_channel", "limit": 200, "exclude_archived": False}
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
        replies.extend(msgs[1:] if not cursor else msgs)
        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor")
        time.sleep(1.2)
    return replies

def get_users():
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

def get_and_save_custom_emojis():
    """カスタム絵文字を取得してダウンロード保存"""
    data = slack_get("emoji.list")
    if not data:
        return {}
    emojis = data.get("emoji", {})
    if not emojis:
        return {}

    EMOJI_DIR.mkdir(parents=True, exist_ok=True)
    emoji_map = {}

    url_emojis = {k: v for k, v in emojis.items() if not v.startswith("alias:")}
    alias_emojis = {k: v for k, v in emojis.items() if v.startswith("alias:")}

    for name, url in url_emojis.items():
        ext = url.split("?")[0].split(".")[-1]
        if ext not in ("png", "jpg", "jpeg", "gif", "webp"):
            ext = "png"
        local_path = EMOJI_DIR / f"{name}.{ext}"
        if not local_path.exists():
            try:
                resp = requests.get(url, headers=HEADERS, timeout=10)
                if resp.status_code == 200:
                    local_path.write_bytes(resp.content)
            except Exception as e:
                print(f"  Failed to download emoji {name}: {e}")
        emoji_map[name] = f"emoji/{name}.{ext}"

    for name, alias_str in alias_emojis.items():
        target = alias_str.replace("alias:", "")
        emoji_map[name] = emoji_map.get(target)  # Noneならビルトイン絵文字

    return emoji_map

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

    print("Fetching users...")
    users = get_users()
    (DATA_DIR / "users.json").write_text(json.dumps(users, ensure_ascii=False, indent=2))
    print(f"  {len(users)} users fetched")

    print("Fetching custom emojis...")
    emoji_map = get_and_save_custom_emojis()
    (DATA_DIR / "emoji_map.json").write_text(json.dumps(emoji_map, ensure_ascii=False, indent=2))
    print(f"  {len(emoji_map)} custom emojis processed")

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

        existing = load_channel_data(cid)
        existing_ts = {m["ts"] for m in existing["messages"]}

        new_msgs = []
        for msg in messages:
            ts = msg.get("ts", "")
            if ts in existing_ts:
                continue
            if msg.get("reply_count", 0) > 0:
                time.sleep(1)
                replies = get_thread_replies(cid, ts)
                msg["replies_data"] = replies
            new_msgs.append(msg)
            existing_ts.add(ts)

        if new_msgs:
            existing["messages"].extend(new_msgs)
            existing["messages"].sort(key=lambda m: float(m.get("ts", 0)))
            save_channel_data(cid, existing)
            print(f"  +{len(new_msgs)} new messages")
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
