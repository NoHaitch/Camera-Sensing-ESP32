#!/usr/bin/env python3
import argparse
import json
import signal
import ssl
import sys
import time
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

stop_requested = False
message_counter = 0
pending_meta_queue = []


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def line(char="=", width=80):
    return char * width


def safe_decode(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return "<binary>"


def pretty_json(text: str) -> str:
    try:
        obj = json.loads(text)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return text


def handle_stop(signum, frame):
    global stop_requested
    stop_requested = True


def print_block(title: str, rows: list[tuple[str, str]]):
    print()
    print(line("="))
    print(title)
    print(line("-"))
    for key, value in rows:
        print(f"{key:<16}: {value}")
    print(line("="))


def sanitize_filename_part(value):
    text = str(value) if value is not None else "none"
    allowed = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_"):
            allowed.append(ch)
        else:
            allowed.append("_")
    return "".join(allowed)


def build_image_filename(meta: dict) -> str:
    device_id = sanitize_filename_part(meta.get("device_id", "unknown-device"))
    session_id = sanitize_filename_part(meta.get("session_id", "unknown-session"))
    mode = sanitize_filename_part(meta.get("mode", "unknown-mode"))
    experiment_id = sanitize_filename_part(meta.get("experiment_id", "no-experiment"))
    attempt_index = sanitize_filename_part(meta.get("attempt_index", "no-index"))
    attempt_total = sanitize_filename_part(meta.get("attempt_total", "no-total"))
    timestamp_us = sanitize_filename_part(meta.get("timestamp_us", "no-ts"))

    return (
        f"{device_id}__"
        f"{session_id}__"
        f"{mode}__"
        f"{experiment_id}__"
        f"idx-{attempt_index}-of-{attempt_total}__"
        f"ts-{timestamp_us}.jpg"
    )


def on_connect(client, userdata, flags, reason_code, properties=None):
    base_topic = userdata["base_topic"]
    meta_topic = f"{base_topic}/meta"
    raw_topic = f"{base_topic}/raw"

    print_block(
        "MQTT CONNECTED",
        [
            ("time", now_text()),
            ("reason_code", str(reason_code)),
            ("base_topic", base_topic),
            ("meta_topic", meta_topic),
            ("raw_topic", raw_topic),
            ("save_dir", str(userdata["save_dir"])),
        ],
    )

    client.subscribe(meta_topic, qos=1)
    client.subscribe(raw_topic, qos=1)

    print_block(
        "SUBSCRIPTION READY",
        [
            ("subscribed", meta_topic),
            ("subscribed", raw_topic),
        ],
    )


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print_block(
        "MQTT DISCONNECTED",
        [
            ("time", now_text()),
            ("reason_code", str(reason_code)),
        ],
    )


def on_message(client, userdata, msg):
    global message_counter
    message_counter += 1

    recv_time = now_text()
    payload_size = len(msg.payload)
    payload_text = safe_decode(msg.payload)

    if msg.topic.endswith("/meta"):
        try:
            meta = json.loads(payload_text)
        except Exception as exc:
            print_block(
                f"MESSAGE #{message_counter} | META PARSE ERROR",
                [
                    ("time", recv_time),
                    ("topic", msg.topic),
                    ("payload_bytes", str(payload_size)),
                    ("error", str(exc)),
                ],
            )
            return

        pending_meta_queue.append(meta)

        print()
        print(line("="))
        print(f"MESSAGE #{message_counter} | META RECEIVED")
        print(line("-"))
        print(f"time            : {recv_time}")
        print(f"topic           : {msg.topic}")
        print(f"payload_bytes   : {payload_size}")
        print(f"pending_meta    : {len(pending_meta_queue)}")
        print(line("-"))
        print("payload:")
        print(json.dumps(meta, indent=2, ensure_ascii=False))
        print(line("="))
        return

    if msg.topic.endswith("/raw"):
        if not pending_meta_queue:
            print_block(
                f"MESSAGE #{message_counter} | RAW WITHOUT META",
                [
                    ("time", recv_time),
                    ("topic", msg.topic),
                    ("payload_bytes", str(payload_size)),
                    ("status", "ignored, no pending meta available"),
                ],
            )
            return

        meta = pending_meta_queue.pop(0)
        save_dir: Path = userdata["save_dir"]
        filename = build_image_filename(meta)
        save_path = save_dir / filename

        with open(save_path, "wb") as f:
            f.write(msg.payload)

        print()
        print(line("="))
        print(f"MESSAGE #{message_counter} | RAW RECEIVED")
        print(line("-"))
        print(f"time            : {recv_time}")
        print(f"topic           : {msg.topic}")
        print(f"payload_bytes   : {payload_size}")
        print(f"saved_file      : {save_path}")
        print(f"device_id       : {meta.get('device_id')}")
        print(f"session_id      : {meta.get('session_id')}")
        print(f"mode            : {meta.get('mode')}")
        print(f"experiment_id   : {meta.get('experiment_id')}")
        print(f"attempt_index   : {meta.get('attempt_index')}")
        print(f"attempt_total   : {meta.get('attempt_total')}")
        print(f"timestamp_us    : {meta.get('timestamp_us')}")
        print(line("-"))
        print("payload:")
        print("<binary image data saved to file>")
        print(line("="))
        return

    print_block(
        f"MESSAGE #{message_counter} | OTHER TOPIC",
        [
            ("time", recv_time),
            ("topic", msg.topic),
            ("payload_bytes", str(payload_size)),
        ],
    )


def build_client(args, save_dir: Path):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=args.client_id,
        userdata={
            "base_topic": args.topic,
            "save_dir": save_dir,
        },
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    if args.username:
        client.username_pw_set(args.username, args.password)

    if args.tls:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    return client


def main():
    parser = argparse.ArgumentParser(
        description="MQTT receiver that saves raw image payloads to files"
    )
    parser.add_argument("--host", required=True, help="MQTT broker host")
    parser.add_argument("--port", type=int, required=True, help="MQTT broker port")
    parser.add_argument("--topic", default="test", help="Base topic, example: test")
    parser.add_argument("--username", default=None, help="MQTT username")
    parser.add_argument("--password", default=None, help="MQTT password")
    parser.add_argument(
        "--client-id",
        default=f"mqtt-rx-{int(time.time())}",
        help="MQTT client id",
    )
    parser.add_argument("--tls", action="store_true", help="Enable TLS connection")
    parser.add_argument(
        "--save-dir",
        default="received_images",
        help="Directory to save received image files",
    )

    args = parser.parse_args()

    save_dir = Path(args.save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    client = build_client(args, save_dir)

    print_block(
        "MQTT RECEIVER START",
        [
            ("time", now_text()),
            ("host", args.host),
            ("port", str(args.port)),
            ("base_topic", args.topic),
            ("tls", str(args.tls)),
            ("client_id", args.client_id),
            ("save_dir", str(save_dir.resolve())),
        ],
    )

    try:
        client.connect(args.host, args.port, keepalive=60)
    except Exception as exc:
        print_block(
            "CONNECTION FAILED",
            [
                ("time", now_text()),
                ("error", str(exc)),
            ],
        )
        sys.exit(1)

    client.loop_start()

    try:
        while not stop_requested:
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()
        print_block(
            "MQTT RECEIVER STOP",
            [
                ("time", now_text()),
                ("messages_received", str(message_counter)),
                ("pending_meta_left", str(len(pending_meta_queue))),
            ],
        )


if __name__ == "__main__":
    main()
    