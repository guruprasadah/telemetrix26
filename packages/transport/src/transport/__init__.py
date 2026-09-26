"""
quic_log_transport
==================

A single-file Python module that ships JSONL logs over **QUIC datagrams**
(RFC 9221 DATAGRAM frames, NOT streams). Vehicles are identified by the
Common Name (CN) on their self-signed client certificate — this is the
right model for IoT/SIM deployments where vehicles do **not** have stable
IPs (so we cannot key logs by source address, and TOFU-on-IP would not
work).

Modes
-----
- **TX** (client / vehicle): reads JSONL from stdin, sends each line as
  one QUIC DATAGRAM frame to the server. The vehicle's identity is the CN
  of its self-signed client cert, provisioned via ``--vehicle-id``.
- **RX** (server): listens for QUIC connections, requires client certs
  (mutual TLS), and writes each received datagram to
  ``<out_dir>/<vehicle_id>.jsonl`` where ``vehicle_id`` is the CN on
  the presented client cert.

Certificates
------------
- A self-signed ECDSA-P256 cert is generated on first use for each role
  (``tx`` and ``rx``) and stored at ``~/.quic_logs/certs/{role}.{crt,key}``.
- For TX, the cert's CN is the value of ``--vehicle-id``. If a cert with
  a different CN already exists, we error out (delete the cert file to
  re-provision).
- For RX, the cert's CN is ``quic-logs-rx`` (informational only).

Identity model
--------------
- Vehicles are identified by the CN on their client cert. The cert's
  private key is the vehicle's secret — whoever holds it can speak for
  that vehicle ID.
- No CA, no TOFU, no SPKI pinning: the cert itself *is* the identity.
  This is appropriate when vehicles are provisioned with their cert at
  deployment time and the cert is stable across reconnections.

Limits
------
- QUIC DATAGRAM frames can't span multiple QUIC packets, so the practical
  max payload is roughly the path MTU minus QUIC overhead. We advertise
  ``DATAGRAM_MAX_SIZE = 1100`` bytes. Lines longer than this are skipped
  with a warning. Chunk them upstream if you need more.

Usage
-----
RX (server)::

    python quic_log_transport/__init__.py --rx \\
        --listen 0.0.0.0:4433 --out /path/to/logs

TX (vehicle)::

    some_log_producer | python quic_log_transport/__init__.py \\
        --tx --server 1.2.3.4:4433 --vehicle-id truck-42

Optional env var:
- ``QUIC_LOG_KEYLOG`` — file path; if set, the TLS keylog is appended
  there for debugging with Wireshark.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import os
import ssl
import sys
from pathlib import Path
from typing import Awaitable, Callable, Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from aioquic.asyncio.client import connect as quic_connect
from aioquic.asyncio.server import serve as quic_serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import (
    ConnectionTerminated,
    DatagramFrameReceived,
    HandshakeCompleted,
)

# ============================================================================
# Constants
# ============================================================================

CERT_DIR = Path.home() / ".quic_logs" / "certs"

# Max payload size for a single QUIC DATAGRAM frame. DATAGRAM frames can't
# be fragmented across QUIC packets, so the practical limit is the path MTU
# (~1500 for Ethernet) minus QUIC packet overhead (~50-100 bytes). 1100 is
# a conservative value that works on basically any network.
DATAGRAM_MAX_SIZE = 1100

ROLE_TX = "tx"
ROLE_RX = "rx"

logger = logging.getLogger("quic_log_transport")


# ============================================================================
# Logging setup
# ============================================================================

def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


# ============================================================================
# Certificate generation
# ============================================================================

def _cert_paths(role: str) -> tuple[Path, Path]:
    """Return (cert_path, key_path) for the given role."""
    return CERT_DIR / f"{role}.crt", CERT_DIR / f"{role}.key"


def _cert_cn(cert_pem: bytes) -> Optional[str]:
    """Extract the CN from a PEM-encoded cert, or None if missing."""
    cert = x509.load_pem_x509_certificate(cert_pem)
    attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    return attrs[0].value if attrs else None


def ensure_cert(role: str, vehicle_id: Optional[str] = None) -> tuple[Path, Path]:
    """
    Ensure a self-signed ECDSA-P256 cert exists for the given role.
    - For role=tx, ``vehicle_id`` is used as the cert's CN (required).
    - For role=rx, ``vehicle_id`` is ignored; the CN is ``quic-logs-rx``.
    If the cert exists but its CN doesn't match the requested one (TX only),
    we error out — delete the cert file to re-provision.
    """
    if role == ROLE_TX and not vehicle_id:
        raise ValueError("--vehicle-id is required for TX mode")

    CERT_DIR.mkdir(parents=True, exist_ok=True)
    cert_path, key_path = _cert_paths(role)
    cn = vehicle_id if role == ROLE_TX else f"quic-logs-{role}"

    if cert_path.exists() and key_path.exists():
        if role == ROLE_TX:
            existing_cn = _cert_cn(cert_path.read_bytes())
            if existing_cn != cn:
                raise RuntimeError(
                    f"Existing TX cert at {cert_path} has CN='{existing_cn}' "
                    f"but --vehicle-id='{cn}'. Delete the cert file "
                    "to re-provision."
                )
        logger.debug("Reusing existing cert for role=%s cn=%s", role, cn)
        return cert_path, key_path

    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, cn),
    ])

    # Timezone-aware datetime (Python 3.12+ deprecates utcnow()).
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        # Allow 5 min backdate for clock skew; valid for 10 years.
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True,
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(cn)]),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )

    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    logger.info(
        "Generated new self-signed cert for role=%s cn=%s at %s",
        role, cn, cert_path,
    )
    return cert_path, key_path


# ============================================================================
# Peer vehicle ID extraction from aioquic
# ============================================================================

def _extract_peer_vehicle_id(quic) -> str:
    """
    Extract the vehicle identifier (peer cert's CN) from an aioquic
    QuicConnection. Returns 'unknown' if the peer didn't present a cert
    or has no CN.
    """
    tls_ctx = getattr(quic, "tls", None)
    if tls_ctx is None:
        return "unknown"
    cert = getattr(tls_ctx, "_peer_certificate", None)
    if cert is None:
        return "unknown"
    try:
        attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        if attrs and attrs[0].value:
            return attrs[0].value
    except Exception:
        pass
    return "unknown"


# ============================================================================
# QuicConfiguration factory
# ============================================================================

def make_quic_config(
    role: str,
    is_client: bool,
    vehicle_id: Optional[str] = None,
) -> QuicConfiguration:
    """Build a QuicConfiguration with our self-signed cert, datagrams
    enabled, and ``verify_mode=CERT_NONE`` (self-signed; identity comes
    from the cert CN, not from CA verification)."""
    cert_path, key_path = ensure_cert(role, vehicle_id=vehicle_id)

    config = QuicConfiguration(
        is_client=is_client,
        max_datagram_frame_size=DATAGRAM_MAX_SIZE,
        verify_mode=ssl.CERT_NONE,
        alpn_protocols=["quic-logs/1"],
    )
    config.load_cert_chain(str(cert_path), str(key_path))

    keylog = os.environ.get("QUIC_LOG_KEYLOG")
    if keylog:
        config.secrets_log_file = open(keylog, "a")
    return config


# ============================================================================
# Protocol classes
# ============================================================================

class ServerProtocol(QuicConnectionProtocol):
    """
    Server-side protocol: receives datagrams, identifies the vehicle by
    the CN on the presented client cert, writes per-vehicle JSONL files.

    Mutual TLS is enabled by monkey-patching the QuicConnection instance's
    ``_initialize`` method to flip aioquic's private
    ``_request_client_certificate`` flag on the underlying TLS context.
    This is the only way to make aioquic request a client cert (the public
    QuicConfiguration doesn't expose it).
    """

    def __init__(
        self,
        *args,
        datagram_handler: Callable[[bytes, str], Awaitable[None]],
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._datagram_handler = datagram_handler
        self._vehicle_id: Optional[str] = None

        # Enable mutual TLS (request client cert). Server side only.
        quic = self._quic
        if not getattr(quic, "_is_client", True):
            original_init = quic._initialize

            def patched_initialize(peer_cid, _orig=original_init):
                _orig(peer_cid)
                tls_ctx = getattr(quic, "tls", None)
                if tls_ctx is not None:
                    tls_ctx._request_client_certificate = True

            quic._initialize = patched_initialize

    def quic_event_received(self, event) -> None:
        if isinstance(event, HandshakeCompleted):
            self._vehicle_id = _extract_peer_vehicle_id(self._quic)
            logger.info("RX: handshake OK vehicle=%s", self._vehicle_id)
        elif isinstance(event, DatagramFrameReceived):
            if self._vehicle_id is None:
                self._vehicle_id = _extract_peer_vehicle_id(self._quic)
            asyncio.ensure_future(
                self._datagram_handler(event.data, self._vehicle_id)
            )
        elif isinstance(event, ConnectionTerminated):
            logger.info(
                "RX: connection closed vehicle=%s code=%s reason=%s",
                self._vehicle_id, event.error_code, event.reason_phrase,
            )


class ClientProtocol(QuicConnectionProtocol):
    """Client-side protocol: tracks handshake completion for gating sends."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handshake_done = asyncio.Event()
        self._connection_closed = asyncio.Event()

    def quic_event_received(self, event) -> None:
        if isinstance(event, HandshakeCompleted):
            self._handshake_done.set()
            logger.debug("TX: handshake completed")
        elif isinstance(event, ConnectionTerminated):
            self._connection_closed.set()
            logger.info(
                "TX: connection closed code=%s reason=%s",
                event.error_code, event.reason_phrase,
            )

    async def wait_handshake(self) -> None:
        await self._handshake_done.wait()

    async def wait_closed(self) -> None:
        await self._connection_closed.wait()

    def send_datagram(self, data: bytes) -> None:
        """Send a single QUIC DATAGRAM frame."""
        self._quic.send_datagram_frame(data)
        self.transmit()

    def close_gracefully(self) -> None:
        try:
            self._quic.close()
            self.transmit()
        except Exception as exc:
            logger.warning("TX: failed to close gracefully: %s", exc)


# ============================================================================
# RX (server) mode
# ============================================================================

async def rx_main(listen_host: str, listen_port: int, out_dir: Path) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    config = make_quic_config(ROLE_RX, is_client=False)

    # Per-vehicle file handles (cached for efficient appends).
    file_handles: dict[str, object] = {}

    def _file_for_vehicle(vehicle_id: str):
        if vehicle_id not in file_handles:
            # Sanitize: keep [A-Za-z0-9._-], replace others with '_'.
            safe = "".join(
                c if (c.isalnum() or c in "._-") else "_"
                for c in vehicle_id
            )
            if not safe:
                safe = "unknown"
            path = out_dir / f"{safe}.jsonl"
            file_handles[vehicle_id] = open(path, "a", encoding="utf-8")
        return file_handles[vehicle_id]

    async def handle_datagram(data: bytes, vehicle_id: str) -> None:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            logger.warning(
                "RX: undecodable datagram from %s (%s); skipping",
                vehicle_id, exc,
            )
            return
        f = _file_for_vehicle(vehicle_id)
        if not text.endswith("\n"):
            text += "\n"
        f.write(text)
        f.flush()
        logger.debug("RX: %d bytes from %s", len(data), vehicle_id)

    def protocol_factory(*args, **kwargs):
        return ServerProtocol(
            *args, datagram_handler=handle_datagram, **kwargs,
        )

    server = await quic_serve(
        listen_host,
        listen_port,
        configuration=config,
        create_protocol=protocol_factory,
    )

    logger.info(
        "RX: listening on [%s]:%d, writing per-vehicle JSONL to %s",
        listen_host, listen_port, out_dir,
    )
    print(
        f"RX listening on [{listen_host}]:{listen_port}, "
        f"writing per-vehicle JSONL to {out_dir}",
        file=sys.stderr,
    )

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        server.close()
        try:
            await server.wait_closed()
        except Exception:
            pass
        for f in file_handles.values():
            try:
                f.close()
            except Exception:
                pass


# ============================================================================
# TX (client / vehicle) mode
# ============================================================================

async def tx_main(
    server_host: str, server_port: int, vehicle_id: str,
) -> int:
    config = make_quic_config(ROLE_TX, is_client=True, vehicle_id=vehicle_id)
    logger.info(
        "TX: vehicle=%s connecting to %s:%d",
        vehicle_id, server_host, server_port,
    )

    def protocol_factory(*args, **kwargs):
        return ClientProtocol(*args, **kwargs)

    sent = 0
    skipped = 0
    try:
        async with quic_connect(
            server_host,
            server_port,
            configuration=config,
            create_protocol=protocol_factory,
        ) as protocol:
            try:
                await asyncio.wait_for(protocol.wait_handshake(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("TX: handshake timed out")
                return 1

            logger.info(
                "TX: vehicle=%s connected to %s:%d; reading JSONL from stdin",
                vehicle_id, server_host, server_port,
            )

            loop = asyncio.get_running_loop()
            while True:
                line = await loop.run_in_executor(
                    None, sys.stdin.buffer.readline
                )
                if not line:
                    break  # EOF

                line = line.rstrip(b"\r\n")
                if not line:
                    continue

                if len(line) > DATAGRAM_MAX_SIZE:
                    logger.warning(
                        "TX: skipping oversized line (%d bytes > %d limit)",
                        len(line), DATAGRAM_MAX_SIZE,
                    )
                    skipped += 1
                    continue

                try:
                    protocol.send_datagram(line)
                    sent += 1
                except Exception as exc:
                    logger.error("TX: failed to send datagram: %s", exc)
                    skipped += 1
                    if protocol._connection_closed.is_set():
                        break

            logger.info("TX: sent %d datagrams, skipped %d", sent, skipped)

            protocol.close_gracefully()
            try:
                await asyncio.wait_for(protocol.wait_closed(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("TX: timed out waiting for graceful close")

    except Exception as exc:
        logger.error("TX: connection error: %s", exc)
        return 1

    return 0


# ============================================================================
# CLI
# ============================================================================

def parse_endpoint(s: Optional[str], default_port: int = 4433) -> tuple[str, int]:
    """Parse 'host:port' or '[host]:port' (for IPv6) into (host, port)."""
    if s is None:
        return "0.0.0.0", default_port
    s = s.strip()
    if s.startswith("["):
        end = s.index("]")
        host = s[1:end]
        rest = s[end + 1:]
        if rest.startswith(":"):
            return host, int(rest[1:])
        return host, default_port
    if ":" in s:
        host, _, port_str = s.rpartition(":")
        return host, int(port_str)
    return s, default_port


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="quic_log_transport",
        description=(
            "Send/receive JSONL logs over QUIC datagrams; vehicles are "
            "identified by the CN on their self-signed client cert "
            "(suitable for IoT/SIM where IPs are not stable)."
        ),
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--tx", action="store_true",
        help="Run as transmitter (vehicle / client).",
    )
    mode.add_argument(
        "--rx", action="store_true",
        help="Run as receiver (server).",
    )
    p.add_argument(
        "--server",
        help="TX: server endpoint as host:port (e.g. 1.2.3.4:4433).",
    )
    p.add_argument(
        "--vehicle-id",
        help="TX: vehicle ID, used as the client cert's CN. Required for TX.",
    )
    p.add_argument(
        "--listen",
        help="RX: bind endpoint as host:port (default 0.0.0.0:4433).",
    )
    p.add_argument(
        "--out",
        default=str(Path.home() / "quic-logs"),
        help="RX: output directory for per-vehicle JSONL files "
             "(default: ~/quic-logs).",
    )
    p.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging.",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    if args.tx:
        if not args.server:
            print("--server host:port is required in TX mode", file=sys.stderr)
            sys.exit(2)
        if not args.vehicle_id:
            print(
                "--vehicle-id is required in TX mode (used as cert CN)",
                file=sys.stderr,
            )
            sys.exit(2)
        host, port = parse_endpoint(args.server)
        try:
            rc = asyncio.run(tx_main(host, port, args.vehicle_id))
        except KeyboardInterrupt:
            rc = 130
        sys.exit(rc)

    elif args.rx:
        host, port = parse_endpoint(args.listen)
        try:
            asyncio.run(rx_main(host, port, Path(args.out)))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
