"""Run the placed RTDB Flight fixture and Bijection's native client test."""

import argparse
import asyncio
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import tempfile


def fixture_tls(directory):
    (directory / "tls.cnf").write_text(
        "[req]\ndistinguished_name=subject\n[subject]\n"
        "[ca]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n"
        "[leaf]\nbasicConstraints=critical,CA:FALSE\nsubjectAltName=IP:127.0.0.1\n"
        "extendedKeyUsage=serverAuth\n"
    )

    def openssl(*arguments):
        subprocess.run(
            ["openssl", *arguments], cwd=directory, check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    openssl("req", "-new", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=Spatial test CA", "-config", "tls.cnf", "-extensions", "ca",
            "-keyout", "ca.key", "-out", "ca.pem")
    openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1",
            "-config", "tls.cnf", "-keyout", "server.key", "-out", "server.csr")
    openssl("x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-out", "server.pem", "-days", "1", "-extfile", "tls.cnf",
            "-extensions", "leaf")
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(directory / "server.pem", directory / "server.key")
    tls.set_alpn_protocols(["h2"])
    return tls


async def relay(reader, writer):
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()


async def proxy(reader, writer, port):
    upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", port)
    tasks = [asyncio.create_task(relay(reader, upstream_writer)),
             asyncio.create_task(relay(upstream_reader, writer))]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        upstream_writer.close()
        writer.close()


async def fixture_port(server, directory):
    address = directory / "listen"
    while not address.exists() or not address.stat().st_size:
        if server.returncode is not None:
            raise RuntimeError("The RTDB fixture exited before accepting connections")
        await asyncio.sleep(0.02)
    host, port = address.read_text().split(":")
    if host != "127.0.0.1":
        raise RuntimeError("The RTDB fixture must listen on loopback")
    return int(port)


async def stop(process):
    if process is not None and process.returncode is None:
        process.kill()
        await process.wait()


async def run(server_binary, client_binary):
    with tempfile.TemporaryDirectory(prefix="hosted-spatial-pair-") as temporary:
        directory = Path(temporary)
        tls = fixture_tls(directory)
        password = secrets.token_hex(24)
        env = {**os.environ, "RTDB_SPATIAL_PAIR_DIR": temporary,
               "RTDB_SPATIAL_FLIGHT_PASSWORD": password, "RUST_MIN_STACK": "16777216"}
        log = directory / "rtdb.log"
        with log.open("wb") as output:
            server = await asyncio.create_subprocess_exec(
                server_binary, "--ignored", "--exact", "serve_hosted_spatial_pair", "--nocapture",
                env=env, stdout=output, stderr=output,
            )
        client = None
        try:
            port = await asyncio.wait_for(fixture_port(server, directory), 30)
            async with await asyncio.start_server(
                lambda reader, writer: proxy(reader, writer, port), "127.0.0.1", 0, ssl=tls,
            ) as bridge:
                tls_port = bridge.sockets[0].getsockname()[1]
                client = await asyncio.create_subprocess_exec(
                    client_binary, "--ignored", "--exact", "--nocapture",
                    "rtdb::owned_flight::hosted_spatial_pair::hosted_spatial_cutover_orders_the_backfill_and_hook_on_one_commit",
                    env={**env, "RTDB_SPATIAL_FLIGHT_CA": str(directory / "ca.pem"),
                         "RTDB_SPATIAL_FLIGHT_URL": f"https://127.0.0.1:{tls_port}"},
                )
                if await asyncio.wait_for(client.wait(), 140) != 0:
                    raise RuntimeError("The Bijection client test failed")
                if await asyncio.wait_for(server.wait(), 15) != 0:
                    raise RuntimeError("The RTDB fixture failed")
        finally:
            await stop(client)
            await stop(server)
            print(log.read_text().replace(password, "[redacted]"), end="")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rtdb_flight_test", type=Path)
    parser.add_argument("bijection_native_test", type=Path)
    arguments = parser.parse_args()
    asyncio.run(run(arguments.rtdb_flight_test.resolve(strict=True),
                    arguments.bijection_native_test.resolve(strict=True)))
