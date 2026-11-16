import hashlib
import http.client
import http.server
import json
import os
import re
import signal
import socket
import ssl
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit


BOUNDARY = "successful_object_http_payload_and_requests"
BUFFER_BYTES = 64 * 1024
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade"}


class Counters:
    def __init__(self, configuration):
        self.lock = threading.Lock()
        self.value = {"namespace": configuration["namespace"], "storage_placement": configuration["storage_placement"],
            "boundary": BOUNDARY, "observer_artifact_sha256": configuration["observer_artifact_sha256"],
            "successful_read_bytes": 0, "successful_read_requests": 0, "successful_write_bytes": 0, "successful_write_requests": 0,
            "failed_requests": 0, "failed_request_bytes": 0, "failed_response_bytes": 0, "in_flight_requests": 0,
            "rejected_requests": 0, "rejected_connections": 0}

    def reject(self, field):
        with self.lock:
            self.value[field] += 1

    def start(self):
        with self.lock:
            self.value["in_flight_requests"] += 1

    def finish(self, method, success, request_bytes, response_bytes):
        with self.lock:
            self.value["in_flight_requests"] -= 1
            if success:
                direction = "read" if method in {"GET", "HEAD"} else "write"
                self.value["successful_" + direction + "_requests"] += 1
                self.value["successful_" + direction + "_bytes"] += response_bytes if direction == "read" else request_bytes
            else:
                self.value["failed_requests"] += 1
                self.value["failed_request_bytes"] += request_bytes
                self.value["failed_response_bytes"] += response_bytes

    def snapshot(self):
        with self.lock:
            return dict(self.value)


class RequestRegion:
    def __init__(self, client, deadline):
        self.client, self.deadline = client, deadline
        self.transports = []
        self.cancelled = False
        self.lock = threading.Lock()

    def attach_transport(self, transport):
        with self.lock:
            self.transports.append(transport)
            if self.cancelled:
                try:
                    transport.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                raise InterruptedError("provider request owner cancelled")

    def check(self):
        with self.lock:
            if self.cancelled or time.monotonic() >= self.deadline:
                raise InterruptedError("provider request lifetime ended")

    def cancel(self):
        with self.lock:
            self.cancelled = True
            sockets = [self.client, *self.transports]
        for connection in sockets:
            if connection is not None:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close_transports(self):
        with self.lock:
            transports, self.transports = self.transports, []
        for transport in transports:
            transport.close()


class Upstream(http.client.HTTPConnection):
    def __init__(self, server, region):
        self.server, self.region = server, region
        selected = urlsplit(server.configuration["upstream"])
        self.selected = selected
        super().__init__(selected.hostname, selected.port or (443 if selected.scheme == "https" else 80), timeout=server.configuration["request_timeout_seconds"])

    def connect(self):
        family, kind, protocol, address = self.server.upstream_address
        self.sock = socket.socket(family, kind, protocol)
        self.region.attach_transport(self.sock)
        self.sock.settimeout(self.timeout)
        self.region.check()
        self.sock.connect(address)
        self.region.check()
        if self.selected.scheme == "https":
            self.sock = self.server.tls_context.wrap_socket(self.sock, server_hostname=self.selected.hostname, do_handshake_on_connect=False)
            self.region.attach_transport(self.sock)
            self.region.check()
            self.sock.do_handshake()
        self.region.check()


class RequestBody:
    def __init__(self, stream, length, chunked):
        self.stream, self.remaining, self.chunked = stream, length, chunked
        self.bytes = 0

    def __iter__(self):
        if not self.chunked:
            while self.remaining:
                data = self.stream.read(min(BUFFER_BYTES, self.remaining))
                if not data:
                    raise ValueError("truncated provider request body")
                self.bytes += len(data)
                self.remaining -= len(data)
                yield data
            return
        while True:
            line = self.stream.readline(1025)
            if len(line) > 1024 or not line.endswith(b"\r\n"):
                raise ValueError("invalid provider request chunk header")
            size = int(line[:-2].split(b";", 1)[0], 16)
            if size < 0:
                raise ValueError("negative provider request chunk size")
            if size == 0:
                if self.stream.readline(3) != b"\r\n":
                    raise ValueError("HTTP request trailers require a qualified observer adapter")
                return
            while size:
                data = self.stream.read(min(BUFFER_BYTES, size))
                if not data:
                    raise ValueError("truncated provider request chunk")
                self.bytes += len(data)
                size -= len(data)
                yield data
            if self.stream.read(2) != b"\r\n":
                raise ValueError("invalid provider request chunk terminator")


def forwarded_headers(headers):
    connection = {part.strip().lower() for value in headers.get_all("Connection", []) for part in value.split(",")}
    return [(name, value) for name, value in headers.items() if name.lower() not in HOP_HEADERS | connection | {"expect"}]


def scoped_prefix(template, namespace, absolute=False):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", namespace):
        raise ValueError("observer namespace must be one canonical path component")
    if absolute and not template.startswith("/") or not template.endswith("/"):
        raise ValueError("observer scope needs an absolute object path and a trailing slash delimiter")
    components = template.removeprefix("/").removesuffix("/").split("/")
    if components.count("{namespace}") != 1 or template.count("{namespace}") != 1 or any(part in {"", ".", ".."} for part in components):
        raise ValueError("observer scope must contain one complete namespace component")
    if any(char in template for char in "%\\?#") or any(ord(char) < 32 or ord(char) == 127 for char in template):
        raise ValueError("observer scope must be an unencoded canonical path")
    return template.replace("{namespace}", namespace)


def decoded_path(value, encoded=True):
    if encoded and re.search(r"%(?![0-9A-Fa-f]{2})", value):
        raise ValueError("malformed provider path encoding")
    decoded = unquote(value, errors="strict") if encoded else value
    current = decoded
    for _ in range(8):
        if "\\" in current or any(ord(char) < 32 or ord(char) == 127 for char in current) or any(part in {".", ".."} for part in current.split("/")):
            raise ValueError("provider path contains a decoded traversal segment")
        following = unquote(current, errors="strict")
        if following == current:
            return decoded
        current = following
    raise ValueError("provider path exceeds the supported encoding depth")


def isolated_path(configuration, method, target):
    try:
        if any(ord(char) < 32 or ord(char) == 127 for char in target):
            return False
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            return False
        namespace = configuration["namespace"]
        prefix = scoped_prefix(configuration["request_path_prefix"], namespace, absolute=True)
        path = decoded_path(parsed.path)
        if path.startswith(prefix):
            return True
        for listing in configuration.get("list_paths", []):
            listing_prefix = scoped_prefix(listing["prefix"], namespace)
            if path != listing["path"]:
                continue
            if method == "HEAD" and not parsed.query:
                return True
            if re.search(r"%(?![0-9A-Fa-f]{2})", parsed.query):
                return False
            selected = parse_qs(parsed.query, keep_blank_values=True, errors="strict", max_num_fields=128).get(listing["prefix_parameter"], [])
            if method == "GET" and len(selected) == 1 and decoded_path(selected[0], encoded=False).startswith(listing_prefix):
                return True
    except (ValueError, UnicodeError):
        return False
    return False


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        self.request.settimeout(self.server.configuration["request_timeout_seconds"])
        super().setup()

    def log_message(self, *_):
        pass

    def proxy(self):
        self.close_connection = True
        if self.path == "/__rtdb_observer__" and self.command == "GET":
            data = json.dumps(self.server.counters.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
            return
        configuration = self.server.configuration
        if not isolated_path(configuration, self.command, self.path):
            self.server.counters.reject("rejected_requests")
            self.send_error(403, "request is outside the isolated measurement namespace")
            return
        if self.command not in {"GET", "HEAD", "PUT", "POST", "DELETE"}:
            self.server.counters.reject("rejected_requests")
            self.send_error(405)
            return
        region = self.server.current_region.value
        connection = Upstream(self.server, region)
        response = None
        success, response_bytes, response_started = False, 0, False
        body = RequestBody(self.rfile, 0, False)
        self.server.counters.start()
        try:
            lengths = self.headers.get_all("Content-Length", [])
            transfer = self.headers.get_all("Transfer-Encoding", [])
            if len(lengths) > 1 or len(transfer) > 1 or lengths and transfer or transfer and transfer != ["chunked"]:
                raise ValueError("ambiguous provider request framing")
            length = int(lengths[0]) if lengths else 0
            if length < 0 or length > configuration["max_request_bytes"]:
                raise ValueError("provider request length exceeds bound")
            body = RequestBody(self.rfile, length, bool(transfer))
            connection.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for name, value in forwarded_headers(self.headers):
                connection.putheader(name, value)
            if transfer:
                connection.putheader("Transfer-Encoding", "chunked")
            connection.putheader("Connection", "close")
            def bounded_body():
                for data in body:
                    if body.bytes > configuration["max_request_bytes"]:
                        raise ValueError("chunked provider request exceeds bound")
                    yield data
            connection.endheaders(bounded_body(), encode_chunked=bool(transfer))
            response = connection.getresponse()
            if response.getheader("Trailer"):
                raise ValueError("HTTP response trailers require a qualified observer adapter")
            expected_response_bytes = response.length
            response_started = True
            self.send_response_only(response.status, response.reason)
            for name, value in forwarded_headers(response.headers):
                self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            while data := response.read(BUFFER_BYTES):
                region.check()
                self.wfile.write(data)
                response_bytes += len(data)
            self.wfile.flush()
            if expected_response_bytes is not None and response_bytes != expected_response_bytes:
                raise ValueError("truncated provider response body")
            success = 200 <= response.status < 300
        except (OSError, ValueError, http.client.HTTPException):
            if not response_started:
                self.send_error(502, "provider observer could not complete the request")
        finally:
            if response is not None:
                response.close()
            connection.close()
            region.close_transports()
            self.server.counters.finish(self.command, success, body.bytes, response_bytes)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = proxy


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True

    def __init__(self, configuration):
        self.configuration = configuration
        self.counters = Counters(configuration)
        self.capacity = threading.BoundedSemaphore(configuration["workers"])
        self.stopping = threading.Event()
        self.current_region = threading.local()
        self.regions = set()
        self.regions_lock = threading.Lock()
        selected = urlsplit(configuration["upstream"])
        address = socket.getaddrinfo(selected.hostname, selected.port or (443 if selected.scheme == "https" else 80), type=socket.SOCK_STREAM)[0]
        self.upstream_address = address[0], address[1], address[2], address[4]
        self.tls_context = ssl.create_default_context(cafile=configuration.get("ca_file")) if selected.scheme == "https" else None
        super().__init__(("0.0.0.0", configuration["port"]), Handler)
        self.timeout = 0.25

    def process_request(self, request, address):
        if not self.capacity.acquire(blocking=False):
            self.counters.reject("rejected_connections")
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.capacity.release()
            raise

    def process_request_thread(self, request, address):
        region = RequestRegion(request, time.monotonic() + self.configuration["request_timeout_seconds"])
        self.current_region.value = region
        with self.regions_lock:
            self.regions.add(region)
            cancelled = self.stopping.is_set()
        if cancelled:
            region.cancel()
        try:
            super().process_request_thread(request, address)
        finally:
            with self.regions_lock:
                self.regions.remove(region)
            del self.current_region.value
            self.capacity.release()

    def cancel_expired(self, all_requests=False):
        now = time.monotonic()
        with self.regions_lock:
            selected = [region for region in self.regions if all_requests or now >= region.deadline]
        for region in selected:
            region.cancel()


def configuration(environment):
    value = json.loads(environment["RTDB_OBSERVER_CONFIGURATION"])
    upstream = urlsplit(value["upstream"])
    if upstream.scheme not in {"http", "https"} or not upstream.hostname or upstream.username or upstream.password or upstream.path not in {"", "/"} or upstream.query or upstream.fragment:
        raise ValueError("observer upstream must be an explicit HTTP(S) origin without credentials")
    scoped_prefix(value["request_path_prefix"], value["namespace"], absolute=True)
    for listing in value.get("list_paths", []):
        scoped_prefix(listing["prefix"], value["namespace"])
        path = listing["path"]
        if not path.startswith("/") or path.startswith("//") or decoded_path(path) != path or any(char in path for char in "?#%") or not listing["prefix_parameter"]:
            raise ValueError("observer listing must retain its explicit namespace prefix")
    for name, maximum in (("workers", 256), ("port", 65535), ("request_timeout_seconds", 300), ("max_request_bytes", 1 << 40)):
        if type(value[name]) is not int or not 1 <= value[name] <= maximum:
            raise ValueError("invalid bounded observer setting: " + name)
    with open(__file__, "rb") as source:
        value["observer_artifact_sha256"] = hashlib.file_digest(source, "sha256").hexdigest()
    return value


def main():
    selected = configuration(os.environ)
    with Server(selected) as server:
        previous = {kind: signal.getsignal(kind) for kind in (signal.SIGINT, signal.SIGTERM)}
        def stop(*_):
            server.stopping.set()
        for kind in previous:
            signal.signal(kind, stop)
        try:
            while not server.stopping.is_set():
                server.handle_request()
                server.cancel_expired()
        finally:
            server.stopping.set()
            server.cancel_expired(all_requests=True)
            server.server_close()
            for kind, handler in previous.items():
                signal.signal(kind, handler)


if __name__ == "__main__":
    main()
