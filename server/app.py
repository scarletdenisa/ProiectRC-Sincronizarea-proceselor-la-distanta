import argparse
import json
import logging
import socket
import threading
from typing import Any, Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class BarrierState:
    def __init__(self, participants: int):
        self.participants = participants
        self.arrived = 0
        self.released = threading.Event()


class SyncServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((host, port))
        self.server_socket.listen(10)

        self.lock = threading.Lock()
        self.locks: Dict[str, str] = {}
        self.client_locks: Dict[str, set[str]] = {}
        self.barriers: Dict[str, BarrierState] = {}
        self.client_names: Dict[str, str] = {}
        self.shutdown_event = threading.Event()

    def start(self) -> None:
        logging.info("Server listening on %s:%s", self.host, self.port)
        try:
            while not self.shutdown_event.is_set():
                try:
                    client_socket, address = self.server_socket.accept()
                except OSError:
                    break
                logging.info("Accepted connection from %s:%s", *address)
                client_socket.settimeout(300)
                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True,
                )
                thread.start()
        finally:
            self.server_socket.close()
            logging.info("Server shutdown")

    def handle_client(self, client_socket: socket.socket, address: Tuple[str, int]) -> None:
        address_key = f"{address[0]}:{address[1]}"
        with client_socket:
            reader = client_socket.makefile("r", encoding="utf-8")
            writer = client_socket.makefile("w", encoding="utf-8")
            try:
                while True:
                    line = reader.readline()
                    if line == "":
                        logging.info("Client disconnected: %s", address_key)
                        break
                    try:
                        request = json.loads(line)
                    except json.JSONDecodeError:
                        response = self.build_response(None, False, None, "JSON decoding failed")
                        writer.write(json.dumps(response) + "\n")
                        writer.flush()
                        continue
                    response = self.process_request(request, address_key)
                    writer.write(json.dumps(response) + "\n")
                    writer.flush()
            except (ConnectionResetError, BrokenPipeError):
                logging.warning("Connection lost from %s", address_key)
            except Exception as exc:
                logging.exception("Unhandled error from %s: %s", address_key, exc)
            finally:
                self.cleanup_client(address_key)

    def process_request(self, request: dict, address_key: str) -> dict:
        request_id = request.get("request_id")
        request_type = request.get("type")
        client_id = request.get("client_id") or address_key
        payload = request.get("payload", {})

        self.register_client(address_key, client_id)

        if request_type == "ping":
            return self.build_response(request_id, True, {"message": "pong"})
        if request_type == "status":
            return self.handle_status(request_id)
        if request_type == "acquire":
            return self.handle_acquire(request_id, client_id, payload)
        if request_type == "release":
            return self.handle_release(request_id, client_id, payload)
        if request_type == "barrier":
            return self.handle_barrier(request_id, client_id, payload)

        return self.build_response(
            request_id,
            False,
            None,
            f"Unknown request type: {request_type}",
        )

    def register_client(self, address_key: str, client_id: str) -> None:
        with self.lock:
            if address_key not in self.client_names:
                self.client_names[address_key] = client_id
                logging.info("Registered client %s for address %s", client_id, address_key)
            elif self.client_names[address_key] != client_id:
                self.client_names[address_key] = client_id
                logging.info("Updated client id %s for address %s", client_id, address_key)

    def cleanup_client(self, address_key: str) -> None:
        with self.lock:
            client_id = self.client_names.pop(address_key, None)
            if not client_id:
                return
            locks_to_release = list(self.client_locks.get(client_id, []))
            for name in locks_to_release:
                owner = self.locks.get(name)
                if owner == client_id:
                    self.locks.pop(name, None)
            self.client_locks.pop(client_id, None)
            logging.info("Cleaned up client %s and released locks: %s", client_id, locks_to_release)

    def handle_acquire(self, request_id: Optional[str], client_id: str, payload: dict) -> dict:
        resource = payload.get("name")
        if not resource:
            return self.build_response(request_id, False, None, "Missing lock name")

        with self.lock:
            owner = self.locks.get(resource)
            if owner is None or owner == client_id:
                self.locks[resource] = client_id
                self.client_locks.setdefault(client_id, set()).add(resource)
                logging.info("Lock acquired: %s by %s", resource, client_id)
                return self.build_response(request_id, True, {"resource": resource, "owner": client_id})
            return self.build_response(
                request_id,
                False,
                None,
                f"Resource '{resource}' is locked by '{owner}'",
            )

    def handle_release(self, request_id: Optional[str], client_id: str, payload: dict) -> dict:
        resource = payload.get("name")
        if not resource:
            return self.build_response(request_id, False, None, "Missing lock name")

        with self.lock:
            owner = self.locks.get(resource)
            if owner != client_id:
                return self.build_response(
                    request_id,
                    False,
                    None,
                    f"Cannot release '{resource}': owned by '{owner or 'nobody'}'",
                )
            self.locks.pop(resource, None)
            self.client_locks.get(client_id, set()).discard(resource)
            logging.info("Lock released: %s by %s", resource, client_id)
            return self.build_response(request_id, True, {"resource": resource})

    def handle_barrier(self, request_id: Optional[str], client_id: str, payload: dict) -> dict:
        name = payload.get("name")
        participants = payload.get("participants")
        if not name or not isinstance(participants, int) or participants < 2:
            return self.build_response(
                request_id,
                False,
                None,
                "Barrier requires 'name' and integer 'participants' >= 2",
            )

        with self.lock:
            barrier = self.barriers.get(name)
            if barrier is None:
                barrier = BarrierState(participants)
                self.barriers[name] = barrier
                logging.info("Created barrier '%s' for %s participants", name, participants)
            elif barrier.participants != participants:
                return self.build_response(
                    request_id,
                    False,
                    None,
                    f"Barrier '{name}' already exists with {barrier.participants} participants",
                )

            barrier.arrived += 1
            current = barrier.arrived
            logging.info(
                "Client %s arrived at barrier '%s' (%s/%s)",
                client_id,
                name,
                current,
                barrier.participants,
            )
            if barrier.arrived >= barrier.participants:
                barrier.released.set()
                self.barriers.pop(name, None)

        released = barrier.released.wait(timeout=60)
        if not released:
            return self.build_response(
                request_id,
                False,
                None,
                f"Barrier '{name}' timed out waiting for {participants} participants",
            )
        return self.build_response(
            request_id,
            True,
            {"barrier": name, "participants": participants, "client_id": client_id},
        )

    def handle_status(self, request_id: Optional[str]) -> dict:
        with self.lock:
            return self.build_response(
                request_id,
                True,
                {
                    "locks": self.locks,
                    "barriers": {
                        name: {"participants": barrier.participants, "arrived": barrier.arrived}
                        for name, barrier in self.barriers.items()
                    },
                    "clients": self.client_names,
                },
            )

    def build_response(
        self,
        request_id: Optional[str],
        success: bool,
        data: Optional[dict],
        error: Optional[str] = None,
    ) -> dict:
        response = {
            "request_id": request_id,
            "status": "ok" if success else "error",
            "data": data or {},
        }
        if error:
            response["error"] = error
        return response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distributed synchronization server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=6000, help="TCP port to listen on")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = SyncServer(args.host, args.port)
    try:
        server.start()
    except KeyboardInterrupt:
        logging.info("Server interrupted by user")
    finally:
        server.shutdown_event.set()


if __name__ == "__main__":
    main()
