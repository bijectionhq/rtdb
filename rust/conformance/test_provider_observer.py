import io
import http.client
import json
import socket
import threading
import types
import unittest
from email.message import Message
from unittest.mock import Mock, patch

from clients.provider_observer import Counters, Handler, RequestBody, RequestRegion, Server, configuration, forwarded_headers, isolated_path


class ProviderPayloadBoundary(unittest.TestCase):
    def test_scope_configuration_requires_a_delimited_complete_namespace_component(self):
        base = {"namespace": "run-a", "upstream": "https://provider.invalid", "request_path_prefix": "/bucket/{namespace}/",
                "workers": 1, "port": 9090, "request_timeout_seconds": 10, "max_request_bytes": 1024,
                "list_paths": [{"path": "/bucket", "prefix_parameter": "prefix", "prefix": "{namespace}/"}]}
        self.assertEqual(base["request_path_prefix"], configuration({"RTDB_OBSERVER_CONFIGURATION": json.dumps(base)})["request_path_prefix"])
        for scope in ("/bucket/{namespace}", "/bucket/{namespace}-other/", "/bucket/a{namespace}/", "/bucket/../{namespace}/", "/bucket/%2e/{namespace}/"):
            with self.subTest(scope=scope), self.assertRaises(ValueError):
                configuration({"RTDB_OBSERVER_CONFIGURATION": json.dumps({**base, "request_path_prefix": scope})})
        for prefix in ("{namespace}", "{namespace}-other/", "../{namespace}/"):
            with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                configuration({"RTDB_OBSERVER_CONFIGURATION": json.dumps({**base, "list_paths": [{**base["list_paths"][0], "prefix": prefix}]})})

    def test_neighboring_namespaces_and_decoded_dot_segments_are_outside_both_routes(self):
        config = {"namespace": "run-a", "request_path_prefix": "/bucket/{namespace}/",
                  "list_paths": [{"path": "/bucket", "prefix_parameter": "prefix", "prefix": "{namespace}/"}]}
        for path in ("/bucket/run-ab/object", "/bucket/run-ab%2Fobject", "/bucket?prefix=run-ab", "/bucket?prefix=run-ab%2F",
                     "/bucket/run-a/../outside", "/bucket/run-a/./object", "/bucket/run-a/%2E%2e/outside",
                     "/bucket/run-a%2F..%2Foutside", "/bucket/run-a/%252e%252e%252foutside", "/bucket/run-a/%5c..%5coutside",
                     "/bucket/../bucket?prefix=run-a%2F", "/bucket?prefix=run-a/../outside", "/bucket?prefix=run-a%2F%2e%2e%2Foutside",
                     "/bucket?prefix=run-a%252F%252e%252e%252Foutside", "/bucket/run-a/bad%zz", "/bucket?prefix=run-a%2Fbad%zz",
                     "/bucket?prefix=run-a%2F&prefix=", "/bucket?prefix=run-a%2F&prefix"):
            with self.subTest(path=path):
                self.assertFalse(isolated_path(config, "GET", path))
        self.assertFalse(isolated_path({**config, "request_path_prefix": "/bucket/{namespace}"}, "GET", "/bucket/run-ab/object"))

    def test_valid_encoded_keys_and_listing_prefixes_remain_admitted(self):
        config = {"namespace": "run-a", "request_path_prefix": "/bucket/{namespace}/",
                  "list_paths": [{"path": "/bucket", "prefix_parameter": "prefix", "prefix": "{namespace}/"}]}
        for path in ("/bucket/run-a/object%20one", "/bucket/run-a%2Fnested%2Fobject", "/bucket/run-a/%E2%82%AC",
                     "/bucket/run-a/ratio%25", "/bucket?prefix=run-a%2Fnested%2Fobject%20one", "/bucket?prefix=run-a%2F%E2%82%AC"):
            with self.subTest(path=path):
                self.assertTrue(isolated_path(config, "GET", path))

    def test_admitted_encoded_request_target_and_signed_headers_are_forwarded_unchanged(self):
        config = {"namespace": "run-a", "request_path_prefix": "/bucket/{namespace}/", "max_request_bytes": 1024,
                  "storage_placement": "zone", "observer_artifact_sha256": "a" * 64,
                  "list_paths": [{"path": "/bucket", "prefix_parameter": "prefix", "prefix": "{namespace}/"}]}
        target = "/bucket/run-a%2Fnested%2Fobject%20one?X-Signature=a%2Bb%3D"
        handler = Handler.__new__(Handler)
        handler.path, handler.command = target, "GET"
        handler.headers = Message()
        handler.headers["Host"], handler.headers["Authorization"], handler.headers["Range"] = "signed-host", "signed-bytes", "bytes=3-7"
        handler.server = types.SimpleNamespace(configuration=config, counters=Counters(config), current_region=types.SimpleNamespace(value=Mock()))
        handler.rfile, handler.wfile = io.BytesIO(), io.BytesIO()
        handler.send_response_only, handler.send_header, handler.end_headers = Mock(), Mock(), Mock()
        with patch("clients.provider_observer.Upstream") as upstream:
            response = upstream.return_value.getresponse.return_value
            response.length, response.status, response.reason, response.headers = 0, 200, "OK", Message()
            response.getheader.return_value, response.read.return_value = None, b""
            handler.proxy()
            upstream.return_value.putrequest.assert_called_once_with("GET", target, skip_host=True, skip_accept_encoding=True)
            for name, value in (("Host", "signed-host"), ("Authorization", "signed-bytes"), ("Range", "bytes=3-7")):
                upstream.return_value.putheader.assert_any_call(name, value)
            self.assertEqual(target, handler.path)
            listing = "/bucket?prefix=run-a%2Fnested%2F&X-Signature=a%2Bb%3D"
            handler.path = listing
            upstream.return_value.putrequest.reset_mock()
            handler.proxy()
            upstream.return_value.putrequest.assert_called_once_with("GET", listing, skip_host=True, skip_accept_encoding=True)
            self.assertEqual(listing, handler.path)

    def test_owner_cancellation_shutdown_reaches_both_admitted_sockets(self):
        client, client_peer = socket.socketpair()
        upstream, upstream_peer = socket.socketpair()
        try:
            region = RequestRegion(client, float("inf"))
            region.attach_transport(upstream)
            region.cancel()
            self.assertEqual(b"", client_peer.recv(1))
            self.assertEqual(b"", upstream_peer.recv(1))
            with self.assertRaises(InterruptedError):
                region.check()
        finally:
            for connection in (client, client_peer, upstream, upstream_peer):
                connection.close()

    def test_whole_request_deadline_applies_even_when_each_read_is_live(self):
        client, peer = socket.socketpair()
        try:
            with self.assertRaises(InterruptedError):
                RequestRegion(client, 0).check()
        finally:
            client.close()
            peer.close()

    def test_connection_close_response_transport_remains_cancellable_while_body_dribbles(self):
        for reason in ("shutdown", "whole-deadline"):
            with self.subTest(reason=reason):
                transport, provider = socket.socketpair()
                client, client_peer = socket.socketpair()
                stop = threading.Event()
                connection = http.client.HTTPConnection("upstream")
                connection.sock = transport
                transport.settimeout(2)
                region = RequestRegion(client, 0 if reason == "whole-deadline" else float("inf"))
                region.attach_transport(transport)
                response = None
                reader = None
                received = []
                def dribble():
                    try:
                        provider.sendall(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1000000\r\n\r\n")
                        while not stop.wait(0.01):
                            provider.sendall(b"a")
                    except OSError:
                        pass
                thread = threading.Thread(target=dribble)
                thread.start()
                try:
                    connection.request("GET", "/object")
                    response = connection.getresponse()
                    self.assertIsNone(connection.sock)
                    self.assertEqual(b"a", response.read(1))
                    if reason == "shutdown":
                        region.cancel()
                    else:
                        owner = types.SimpleNamespace(regions={region}, regions_lock=threading.Lock())
                        Server.cancel_expired(owner)
                    reader = threading.Thread(target=lambda: received.append(response.read(1000000)))
                    reader.start()
                    reader.join(1)
                    self.assertFalse(reader.is_alive())
                    self.assertLess(len(received[0]), 999999)
                finally:
                    stop.set()
                    for value in (provider, client, client_peer):
                        try:
                            value.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        value.close()
                    if reader is not None:
                        reader.join(2)
                    thread.join(2)
                    if response is not None:
                        response.close()
                    connection.close()
                    region.close_transports()
                self.assertFalse(thread.is_alive())

    def test_chunk_framing_is_excluded_from_successful_payload_bytes(self):
        body = RequestBody(io.BytesIO(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"), 0, True)
        self.assertEqual(b"abcde", b"".join(body))
        self.assertEqual(5, body.bytes)

    def test_truncated_and_trailer_bearing_requests_remain_unqualified(self):
        for wire in (b"4\r\nabc", b"0\r\nDigest: checksum\r\n\r\n", b"-1\r\n"):
            with self.subTest(wire=wire), self.assertRaises(ValueError):
                list(RequestBody(io.BytesIO(wire), 0, True))

    def test_failed_provider_calls_are_not_successful_zero_byte_requests(self):
        counters = Counters({"namespace": "run", "storage_placement": "zone", "observer_artifact_sha256": "a" * 64})
        counters.start()
        counters.finish("PUT", False, 100, 5)
        counters.start()
        counters.finish("GET", True, 0, 37)
        result = counters.snapshot()
        self.assertEqual((0, 1, 37), (result["successful_write_requests"], result["successful_read_requests"], result["successful_read_bytes"]))
        self.assertEqual((1, 100, 5, 0), (result["failed_requests"], result["failed_request_bytes"], result["failed_response_bytes"], result["in_flight_requests"]))

    def test_range_signature_and_content_encoding_headers_survive_forwarding(self):
        headers = Message()
        for name, value in (("Host", "object-observer:9090"), ("Range", "bytes=3-7"), ("Authorization", "signed-value"),
                            ("Content-Encoding", "aws-chunked"), ("Connection", "keep-alive, private-hop"), ("Private-Hop", "removed")):
            headers[name] = value
        self.assertEqual([("Host", "object-observer:9090"), ("Range", "bytes=3-7"), ("Authorization", "signed-value"),
                          ("Content-Encoding", "aws-chunked")], forwarded_headers(headers))

    def test_object_and_listing_requests_must_retain_run_namespace(self):
        config = {"namespace": "run-a", "request_path_prefix": "/bucket/{namespace}/",
                  "list_paths": [{"path": "/bucket", "prefix_parameter": "prefix", "prefix": "{namespace}/"}]}
        self.assertTrue(isolated_path(config, "GET", "/bucket/run-a/object"))
        self.assertTrue(isolated_path(config, "GET", "/bucket?list-type=2&prefix=run-a%2F"))
        self.assertFalse(isolated_path(config, "GET", "/bucket?list-type=2"))
        self.assertFalse(isolated_path(config, "PUT", "/bucket/run-ab/object"))
        self.assertFalse(isolated_path(config, "GET", "http://elsewhere/bucket/run-a/object"))


if __name__ == "__main__":
    unittest.main()
