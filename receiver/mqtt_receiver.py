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
experiments = {}


def now_us() -> int:
    return time.time_ns() // 1000


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def line(char="=", width=88):
    return char * width


def handle_stop(signum, frame):
    global stop_requested
    stop_requested = True


def print_block(title: str, rows: list[tuple[str, str]]):
    print()
    print(line("="))
    print(title)
    print(line("-"))
    for key, value in rows:
        print(f"{key:<20}: {value}")
    print(line("="))


def safe_decode(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def sanitize_filename_part(value):
    text = str(value) if value is not None else "none"
    out = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


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


def build_experiment_key(meta: dict) -> str:
    device_id = meta.get("device_id", "unknown-device")
    session_id = meta.get("session_id", "unknown-session")
    experiment_id = meta.get("experiment_id", "no-experiment")
    return f"{device_id}|{session_id}|{experiment_id}"


def ensure_experiment(meta: dict):
    key = build_experiment_key(meta)
    if key not in experiments:
        experiments[key] = {
            "device_id": meta.get("device_id"),
            "session_id": meta.get("session_id"),
            "mode": meta.get("mode"),
            "experiment_id": meta.get("experiment_id"),
            "attempt_total": meta.get("attempt_total"),
            "rows": [],
            "raw_recv_us_list": [],
            "interval_ms_list": [],
            "latency_ms_list": [],
            "done": False,
        }
    return key


def calc_stats(values):
    if not values:
        return None
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values),
    }


def fmt_stats(values):
    s = calc_stats(values)
    if not s:
        return "count=0, min=N/A, max=N/A, avg=N/A"
    return (
        f"count={s['count']}, "
        f"min={s['min']:.3f} ms, "
        f"max={s['max']:.3f} ms, "
        f"avg={s['avg']:.3f} ms"
    )


def print_experiment_summary(exp_key: str):
    exp = experiments[exp_key]

    print()
    print(line("="))
    print("LEVEL 1 EXPERIMENT SUMMARY")
    print(line("-"))
    print(f"experiment_key       : {exp_key}")
    print(f"device_id            : {exp['device_id']}")
    print(f"session_id           : {exp['session_id']}")
    print(f"mode                 : {exp['mode']}")
    print(f"experiment_id        : {exp['experiment_id']}")
    print(f"attempt_total        : {exp['attempt_total']}")
    print(f"received_raw_count   : {len(exp['rows'])}")
    print(line("-"))
    print(f"user_interval_stats  : {fmt_stats(exp['interval_ms_list'])}")
    print(f"e2e_latency_stats    : {fmt_stats(exp['latency_ms_list'])}")
    print(line("-"))
    print("DETAIL PER IMAGE")
    for row in exp["rows"]:
        interval_text = "N/A" if row["interval_ms"] is None else f"{row['interval_ms']:.3f} ms"
        latency_text = "N/A" if row["latency_ms"] is None else f"{row['latency_ms']:.3f} ms"
        print(
            f"attempt={row['attempt_index']}/{row['attempt_total']} | "
            f"recv_raw_us={row['recv_raw_us']} | "
            f"interval_user={interval_text} | "
            f"latency_e2e={latency_text} | "
            f"file={row['saved_file']}"
        )
    print(line("="))


def maybe_finalize_experiment(exp_key: str):
    exp = experiments[exp_key]
    total = exp["attempt_total"]

    if exp["done"]:
        return

    try:
        total_int = int(total)
    except Exception:
        total_int = None

    if total_int is not None and len(exp["rows"]) >= total_int:
        exp["done"] = True
        print_experiment_summary(exp_key)


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

    recv_time_text = now_text()
    recv_us = now_us()
    payload_size = len(msg.payload)
    payload_text = safe_decode(msg.payload)

    if msg.topic.endswith("/meta"):
        try:
            meta = json.loads(payload_text)
        except Exception as exc:
            print_block(
                f"MESSAGE #{message_counter} | META PARSE ERROR",
                [
                    ("time", recv_time_text),
                    ("topic", msg.topic),
                    ("payload_bytes", str(payload_size)),
                    ("error", str(exc)),
                ],
            )
            return

        pending_meta_queue.append(meta)
        exp_key = ensure_experiment(meta)

        print()
        print(line("="))
        print(f"MESSAGE #{message_counter} | META RECEIVED")
        print(line("-"))
        print(f"time                : {recv_time_text}")
        print(f"topic               : {msg.topic}")
        print(f"payload_bytes       : {payload_size}")
        print(f"experiment_key      : {exp_key}")
        print(f"attempt_index       : {meta.get('attempt_index')}")
        print(f"attempt_total       : {meta.get('attempt_total')}")
        print(f"timestamp_us        : {meta.get('timestamp_us')}")
        print(f"pending_meta        : {len(pending_meta_queue)}")
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
                    ("time", recv_time_text),
                    ("topic", msg.topic),
                    ("payload_bytes", str(payload_size)),
                    ("status", "ignored, no pending meta available"),
                ],
            )
            return

        meta = pending_meta_queue.pop(0)
        exp_key = ensure_experiment(meta)
        exp = experiments[exp_key]

        save_dir: Path = userdata["save_dir"]
        filename = build_image_filename(meta)
        save_path = save_dir / filename

        with open(save_path, "wb") as f:
            f.write(msg.payload)

        interval_ms = None
        if exp["raw_recv_us_list"]:
            prev_recv_us = exp["raw_recv_us_list"][-1]
            interval_ms = (recv_us - prev_recv_us) / 1000.0
            exp["interval_ms_list"].append(interval_ms)

        latency_ms = None
        try:
            send_ts_us = int(meta.get("timestamp_us"))
            latency_ms = (recv_us - send_ts_us) / 1000.0
            exp["latency_ms_list"].append(latency_ms)
        except Exception:
            latency_ms = None

        exp["raw_recv_us_list"].append(recv_us)
        exp["rows"].append(
            {
                "attempt_index": meta.get("attempt_index"),
                "attempt_total": meta.get("attempt_total"),
                "recv_raw_us": recv_us,
                "interval_ms": interval_ms,
                "latency_ms": latency_ms,
                "saved_file": str(save_path),
            }
        )

        print()
        print(line("="))
        print(f"MESSAGE #{message_counter} | RAW RECEIVED")
        print(line("-"))
        print(f"time                : {recv_time_text}")
        print(f"topic               : {msg.topic}")
        print(f"payload_bytes       : {payload_size}")
        print(f"saved_file          : {save_path}")
        print(f"experiment_key      : {exp_key}")
        print(f"attempt_index       : {meta.get('attempt_index')}")
        print(f"attempt_total       : {meta.get('attempt_total')}")
        print(f"timestamp_us        : {meta.get('timestamp_us')}")
        print(
            f"user_interval_ms    : "
            f"{'N/A' if interval_ms is None else f'{interval_ms:.3f}'}"
        )
        print(
            f"e2e_latency_ms      : "
            f"{'N/A' if latency_ms is None else f'{latency_ms:.3f}'}"
        )
        print(line("-"))
        print("payload:")
        print("<binary image data saved to file>")
        print(line("="))

        maybe_finalize_experiment(exp_key)
        return

    print_block(
        f"MESSAGE #{message_counter} | OTHER TOPIC",
        [
            ("time", recv_time_text),
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
        description="MQTT receiver for Level 1 interval and latency validation"
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

        for exp_key, exp in experiments.items():
            if exp["rows"] and not exp["done"]:
                print_experiment_summary(exp_key)

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
    