import argparse
import json
import logging
import socket
import threading
from typing import Any, Dict, Optional, Tuple, List

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
        # Structuri pentru cozi de asteptare si notificari push per client.
        # waiting_queues: semafor -> lista ordonata de client_id care asteapta
        # client_address: mapare inversa client_id -> address_key (pentru a gasi writer-ul)
        # client_writers: address_key -> obiect writer al socket-ului clientului
        # client_write_locks: address_key -> Lock pentru scriere thread-safe pe writer
        self.waiting_queues: Dict[str, List[str]] = {}
        self.client_address: Dict[str, str] = {}
        self.client_writers: Dict[str, Any] = {}
        self.client_write_locks: Dict[str, threading.Lock] = {}

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
            # Stocam writer-ul si lock-ul de scriere imediat la conectare,
            # inainte de orice cerere, pentru a putea trimite notificari push.
            write_lock = threading.Lock()
            with self.lock:
                self.client_writers[address_key] = writer
                self.client_write_locks[address_key] = write_lock
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
                        # Scriere thread-safe folosind write_lock pentru a nu interfera
                        # cu notificarile push trimise din alte thread-uri.
                        with write_lock:
                            writer.write(json.dumps(response) + "\n")
                            writer.flush()
                        continue
                    response = self.process_request(request, address_key)
                    with write_lock:
                        writer.write(json.dumps(response) + "\n")
                    writer.flush()
            except (ConnectionResetError, BrokenPipeError):
                logging.warning("Connection lost from %s", address_key)
            except Exception as exc:
                logging.exception("Unhandled error from %s: %s", address_key, exc)
            finally:
                # Eliminam writer-ul INAINTE de cleanup ca sa nu trimitem
                # notificari la un client deja deconectat.
                with self.lock:
                    self.client_writers.pop(address_key, None)
                    self.client_write_locks.pop(address_key, None)
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
                self.client_address[client_id] = address_key
                logging.info("Registered client %s for address %s", client_id, address_key)
            elif self.client_names[address_key] != client_id:
                old_id = self.client_names[address_key]
                self.client_address.pop(old_id, None)
                self.client_address[client_id] = address_key
                self.client_names[address_key] = client_id
                logging.info("Updated client id %s for address %s", client_id, address_key)

    def cleanup_client(self, address_key: str) -> None:
        # Cleanup extins: pe langa eliberarea lock-urilor, acum:
        # 1. Elimina clientul din toate cozile de asteptare in care se afla.
        # 2. La eliberarea unui semafor, acorda accesul urmatorului din coada
        #    si trimite notificarea push corespunzatoare.
        notifications: List[Tuple[str, dict]] = []
        with self.lock:
            client_id = self.client_names.pop(address_key, None)
            if not client_id:
                return
            self.client_address.pop(client_id, None)

            # Eliminare client din toate cozile de asteptare
            for resource in list(self.waiting_queues.keys()):
                queue = self.waiting_queues[resource]
                if client_id in queue:
                    queue.remove(client_id)
                    logging.info(
                        "Eliminat client '%s' din coada de asteptare pentru semaforul '%s'",
                        client_id, resource,
                    )
                    if not queue:
                        del self.waiting_queues[resource]

            # Eliberare semafoare detinute si acordare urmatorului din coada
            locks_to_release = list(self.client_locks.get(client_id, []))
            for name in locks_to_release:
                owner = self.locks.get(name)
                if owner == client_id:
                    self.locks.pop(name, None)
                    logging.info(
                        "Eliberat automat semaforul '%s' de la clientul deconectat '%s'",
                        name, client_id,
                    )
                    next_client, notification = self._grant_next_in_queue(name)
                    if next_client and notification:
                        notifications.append((next_client, notification))
            self.client_locks.pop(client_id, None)
            logging.info("Cleaned up client %s, released semaphores: %s", client_id, locks_to_release)
            # Trimitem notificarile in afara lock-ului principal pentru a evita deadlock
            for next_client, notification in notifications:
                self._send_notification(next_client, notification)

    def handle_acquire(self, request_id: Optional[str], client_id: str, payload: dict) -> dict:
        resource = payload.get("name")

        if not resource:
            return self.build_response(
                request_id,
                False,
                None,
                "Missing lock name",
            )

        with self.lock:
            owner = self.locks.get(resource)

            # Lock liber sau deja detinut de acelasi client
            if owner is None or owner == client_id:
                self.locks[resource] = client_id
                self.client_locks.setdefault(client_id, set()).add(resource)

                logging.info(
                    "Lock acquired: %s by %s",
                    resource,
                    client_id,
                )

                return self.build_response(
                    request_id,
                    True,
                    {
                        "resource": resource,
                        "owner": client_id,
                        "queued": False,
                    },
                )

            # Lock ocupat -> clientul intra in coada
            queue = self.waiting_queues.setdefault(resource, [])

            if client_id not in queue:
                queue.append(client_id)

            position = queue.index(client_id) + 1

            logging.info(
                "Client '%s' adaugat in coada pentru '%s' (pozitia %d)",
                client_id,
                resource,
                position,
            )

            return self.build_response(
                request_id,
                True,
                {
                    "resource": resource,
                    "owner": owner,
                    "queued": True,
                    "position": position,
                },
            )

    def handle_release(self, request_id: Optional[str], client_id: str, payload: dict) -> dict:
        resource = payload.get("name")

        if not resource:
            return self.build_response(
                request_id,
                False,
                None,
                "Missing lock name",
            )

        next_client: Optional[str] = None
        notification: Optional[dict] = None

        with self.lock:
            owner = self.locks.get(resource)

            # Clientul nu detine lock-ul
            if owner != client_id:
                return self.build_response(
                    request_id,
                    False,
                    None,
                    f"Cannot release '{resource}': owned by '{owner or 'nobody'}'",
                )

            # Eliberam lock-ul
            self.locks.pop(resource, None)
            self.client_locks.get(client_id, set()).discard(resource)

            logging.info(
                "Semaphore released: %s by %s",
                resource,
                client_id,
            )

            # Acordam lock-ul urmatorului client din coada
            next_client, notification = self._grant_next_in_queue(resource)

        # Trimitem notificarea in afara lock-ului principal
        if next_client and notification:
            self._send_notification(next_client, notification)

        return self.build_response(
            request_id,
            True,
            {
                "resource": resource,
            },
        )

    def _grant_next_in_queue(self, resource: str) -> Tuple[Optional[str], Optional[dict]]:
        """Acorda semaforul primului client conectat din coada.

        Trebuie apelat cu self.lock detinut.
        Returneaza (client_id, notificare) sau (None, None) daca nu e nimeni in coada.
        Sare peste clientii care s-au deconectat intre timp.
        """
        queue = self.waiting_queues.get(resource, [])
        while queue:
            next_client = queue.pop(0)
            if next_client in self.client_address:
                self.locks[resource] = next_client
                self.client_locks.setdefault(next_client, set()).add(resource)
                logging.info(
                    "Semaforul '%s' acordat din coada catre clientul '%s'",
                    resource, next_client,
                )
                if not queue:
                    self.waiting_queues.pop(resource, None)
                notification = {
                    "type": "notification",
                    "event": "granted",
                    "data": {"resource": resource, "owner": next_client},
                }
                return next_client, notification
            else:
                logging.info(
                    "Omis client deconectat '%s' din coada pentru semaforul '%s'",
                    next_client, resource,
                )
        self.waiting_queues.pop(resource, None)
        return None, None

    def _send_notification(self, client_id: str, notification: dict) -> None:
        """Trimite o notificare push unui client prin socket-ul sau.

        Apelat fara self.lock detinut. Foloseste write_lock per client
        pentru a nu interfera cu raspunsurile la cereri normale.
        """
        addr = self.client_address.get(client_id)
        if not addr:
            return
        writer = self.client_writers.get(addr)
        write_lock = self.client_write_locks.get(addr)
        if writer and write_lock:
            try:
                with write_lock:
                    writer.write(json.dumps(notification) + "\n")
                    writer.flush()
                logging.info(
                    "Notificare '%s' trimisa catre clientul '%s'",
                    notification.get("event"), client_id,
                )
            except Exception as exc:
                logging.warning(
                    "Eroare la trimiterea notificarii catre '%s': %s", client_id, exc
                )

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
                    "waiting_queues": {
                        name: list(queue)
                        for name, queue in self.waiting_queues.items()
                    },
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
