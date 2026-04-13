#!/usr/bin/env python3
import argparse
import json
import signal
import ssl
import sys
import time

import paho.mqtt.client as mqtt

stop_requested = False


def now_us():
    return time.time_ns() // 1000


def handle_stop(signum, frame):
    global stop_requested
    stop_requested = True


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"Connected with reason_code={reason_code}")
    client.subscribe("test/meta", qos=1)
    print("Subscribed to topic: test/meta")


def on_message(client, userdata, msg):
    recv_us = now_us()

    if msg.topic != "test/meta":
        return

    print("============================================================")
    print(f"topic      : {msg.topic}")
    print(f"recv_us    : {recv_us}")

    try:
        payload_text = msg.payload.decode("utf-8")
        print(f"payload    : {payload_text}")

        data = json.loads(payload_text)
        seq = data.get("seq")
        ts_us = data.get("timestamp_us")
        filename = data.get("filename")
        size_bytes = data.get("size_bytes")

        print(f"seq        : {seq}")
        print(f"timestamp  : {ts_us}")
        print(f"filename   : {filename}")
        print(f"size_bytes : {size_bytes}")

        if ts_us is not None:
            latency_ms = (recv_us - int(ts_us)) / 1000.0
            print(f"latency_ms : {latency_ms:.3f}")
    except Exception as e:
        print(f"raw_bytes  : {len(msg.payload)} bytes")
        print(f"parse_error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Simple MQTT terminal client for test/meta")
    parser.add_argument("--host", default="broker.hivemq.com")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--client-id", default=f"mqtt-meta-{int(time.time())}")
    parser.add_argument("--tls", action="store_true")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id)
    client.on_connect = on_connect
    client.on_message = on_message

    if args.username:
        client.username_pw_set(args.username, args.password)

    if args.tls:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    print("Waiting for messages on test/meta ... Press Ctrl+C to stop.")

    try:
        while not stop_requested:
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected")


if __name__ == "__main__":
    main()
    