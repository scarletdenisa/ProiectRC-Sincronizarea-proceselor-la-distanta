import argparse
import json
import socket
import sys
import threading
import uuid
from typing import Any, Dict, Optional


def build_request(request_type: str, client_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": request_type,
        "client_id": client_id,
        "request_id": str(uuid.uuid4()),
        "payload": payload,
    }


def send_request(writer: Any, request: Dict[str, Any]) -> None:
    writer.write(json.dumps(request) + "\n")
    writer.flush()


def read_response(reader: Any) -> Optional[Dict[str, Any]]:
    line = reader.readline()
    if line == "":
        return None
    return json.loads(line)


class SyncClient:
    def __init__(self, host: str, port: int, client_id: str) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.socket: Optional[socket.socket] = None
        self.reader = None
        self.writer = None

    def connect(self) -> None:
        self.socket = socket.create_connection((self.host, self.port), timeout=10)
        self.reader = self.socket.makefile("r", encoding="utf-8")
        self.writer = self.socket.makefile("w", encoding="utf-8")
        print(f"Connected to server at {self.host}:{self.port} as {self.client_id}")

    def close(self) -> None:
        if self.socket:
            self.socket.close()
            self.socket = None

    def run(self) -> None:
        try:
            self.connect()
        except Exception as exc:
            print(f"Connection failed: {exc}")
            return

        print("Enter commands: acquire <name>, release <name>, barrier <name> <participants>, status, ping, exit")
        try:
            while True:
                try:
                    command = input("> ").strip()
                except EOFError:
                    print("\nEnd of input. Exiting.")
                    break
                if not command:
                    continue
                tokens = command.split()
                cmd = tokens[0].lower()
                if cmd == "exit":
                    break
                elif cmd == "acquire" and len(tokens) == 2:
                    self.execute("acquire", {"name": tokens[1]})
                elif cmd == "release" and len(tokens) == 2:
                    self.execute("release", {"name": tokens[1]})
                elif cmd == "barrier" and len(tokens) == 3 and tokens[2].isdigit():
                    self.execute("barrier", {"name": tokens[1], "participants": int(tokens[2])})
                elif cmd == "status":
                    self.execute("status", {})
                elif cmd == "ping":
                    self.execute("ping", {})
                else:
                    print("Unknown command. Use acquire, release, barrier, status, ping, or exit.")
        except KeyboardInterrupt:
            print("\nInterrupted by user")
        finally:
            self.close()

    def execute(self, request_type: str, payload: Dict[str, Any]) -> None:
        if not self.writer or not self.reader:
            print("Not connected to the server.")
            return
        request = build_request(request_type, self.client_id, payload)
        try:
            send_request(self.writer, request)
            response = read_response(self.reader)
            if response is None:
                print("Server closed the connection.")
                sys.exit(0)
            self.print_response(response)
        except (BrokenPipeError, ConnectionResetError, socket.error) as exc:
            print(f"Connection error: {exc}")
            sys.exit(1)

    def print_response(self, response: Dict[str, Any]) -> None:
        status = response.get("status")
        if status == "ok":
            print("OK:", json.dumps(response.get("data", {}), indent=2))
        else:
            print("ERROR:", response.get("error", "Unknown error"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Client for the distributed synchronization server")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=6000, help="Server TCP port")
    parser.add_argument("--name", default=f"client-{uuid.uuid4().hex[:6]}", help="Client identifier")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = SyncClient(args.host, args.port, args.name)
    client.run()


if __name__ == "__main__":
    main()
