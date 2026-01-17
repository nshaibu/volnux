from __future__ import annotations

import hashlib
import logging
import os
import socket
import ssl
import typing
import grpc
from contextlib import contextmanager
from datetime import datetime, timezone
from ssl import SSLContext

from volnux.exceptions import (
    CertificateExpiredError,
    CertificatePinningError,
    CertificateVerificationError,
    HandshakeError,
    HostnameVerificationError,
    SSLConfigurationError,
)
from .ssl_config import SSLConfig

logger = logging.getLogger(__name__)


class SecureSocketManager:
    """
    Manages SSL/TLS connections for TCP and gRPC protocols.

    This class provides a unified interface for creating secure connections
    with proper certificate validation, hostname verification, and error handling.
    """

    # Default secure cipher suites
    # Picked from https://developers.cloudflare.com/ssl/edge-certificates/additional-options/cipher-suites/recommendations/
    DEFAULT_CIPHERS = (
        "DHE-RSA-AES256-GCM-SHA384:"
        "AECDHE-ECDSA-AES256-SHA"
    )

    def __init__(self, config: SSLConfig) -> None:
        """
        Initialize the SecureSocketManager with the given configuration.

        Args:
            config: SSL configuration settings
        """
        self._config = config
        self._validate_config()

        # Check certificate expiration on initialization
        if config.has_client_cert:
            self._check_certificate_expiration(config.cert_path)

    def create_client_context(self) -> ssl.SSLContext:
        """
        Create an SSL context for client connections.

        Returns:
            ssl.SSLContext: Configured SSL context for client use
        """
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._configure_tls_versions(context)
        self._configure_ciphers(context)
        self._configure_client_verification(context)
        self._load_client_certificates(context)

        return context

    def create_server_context(self) -> ssl.SSLContext:
        """
        Create an SSL context for server connections.

        Returns:
            ssl.SSLContext: Configured SSL context for server use
        """
        if not self._config.has_client_cert:
            raise SSLConfigurationError(
                f"Server certificate and key are required for server context. "
                f"cert_path={self._config.cert_path}, key_path={self._config.key_path}"
            )

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._configure_tls_versions(context)
        self._configure_ciphers(context)
        self._configure_server_verification(context)
        self._load_server_certificates(context)

        return context

    def wrap_client_socket(
        self,
        sock: socket.socket,
        server_hostname: str,
        timeout: typing.Optional[float] = None,
    ) -> ssl.SSLSocket:
        """
        Wrap a client socket with SSL/TLS encryption.

        Args:
            sock: Plain socket to wrap
            server_hostname: Expected hostname for verification
            timeout: Optional timeout for the handshake (seconds)

        Returns:
            ssl.SSLSocket: Wrapped socket with SSL/TLS
        """
        context = self.create_client_context()

        # Set timeout if specified
        original_timeout = sock.gettimeout()
        if timeout is not None:
            sock.settimeout(timeout)

        try:
            with self.handle_ssl_errors(server_hostname):
                # Wrap socket with SSL
                ssl_sock = context.wrap_socket(
                    sock,
                    server_hostname=server_hostname if self._config.verify_hostname else None,
                )

                # Perform certificate pinning validation if configured
                if self._config.has_pinned_certificates:
                    self._verify_certificate_pinning(ssl_sock)

                logger.debug(
                    f"SSL connection established to {server_hostname} "
                    f"using {ssl_sock.version()}"
                )

                return ssl_sock

        except Exception:
            # Restore original timeout on failure
            if timeout is not None:
                try:
                    sock.settimeout(original_timeout)
                except Exception:
                    pass
            raise

    def wrap_server_socket(
        self,
        sock: socket.socket,
        timeout: typing.Optional[float] = None,
    ) -> ssl.SSLSocket:
        """
        Wrap a server socket with SSL/TLS encryption.

        Args:
            sock: Plain socket to wrap (usually the accepted connection)
            timeout: Optional timeout for the handshake (seconds)

        Returns:
            ssl.SSLSocket: Wrapped socket with SSL/TLS
        """
        context = self.create_server_context()

        # Set timeout if specified
        original_timeout = sock.gettimeout()
        if timeout is not None:
            sock.settimeout(timeout)

        try:
            with self.handle_ssl_errors():
                ssl_sock = context.wrap_socket(sock, server_side=True)

                logger.debug(
                    f"SSL server connection established using {ssl_sock.version()}"
                )

                return ssl_sock

        except Exception:
            # Restore original timeout on failure
            if timeout is not None:
                try:
                    sock.settimeout(original_timeout)
                except Exception:
                    pass
            raise

    def create_grpc_client_credentials(self) -> "grpc.ChannelCredentials":
        """
        Create gRPC client SSL credentials.

        Returns:
            grpc.ChannelCredentials: Credentials for a secure gRPC channel
        """
        # Load CA certificate
        root_certificates = None
        if self._config.ca_cert_path:
            root_certificates = self._read_certificate_file(self._config.ca_cert_path)

        # Load client certificate for mTLS
        private_key = None
        certificate_chain = None
        if self._config.has_client_cert:
            private_key = self._read_certificate_file(self._config.key_path)
            certificate_chain = self._read_certificate_file(self._config.cert_path)

        return grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )

    def create_grpc_server_credentials(self) -> "grpc.ServerCredentials":
        """
        Create gRPC server SSL credentials.

        Returns:
            grpc.ServerCredentials: Credentials for secure gRPC server
        """
        if not self._config.has_client_cert:
            raise SSLConfigurationError(
                "Server certificate and key are required for gRPC server credentials"
            )

        # Load server certificate and key
        private_key = self._read_certificate_file(self._config.key_path)
        certificate_chain = self._read_certificate_file(self._config.cert_path)

        # Load CA certificate for client verification
        root_certificates = None
        if self._config.ca_cert_path and self._config.require_client_cert:
            root_certificates = self._read_certificate_file(self._config.ca_cert_path)

        return grpc.ssl_server_credentials(
            [(private_key, certificate_chain)],
            root_certificates=root_certificates,
            require_client_auth=self._config.require_client_cert,
        )

    @contextmanager
    def handle_ssl_errors(
        self, server_hostname: typing.Optional[str] = None
    ) -> typing.Generator[None, None, None]:
        """
        Context manager for graceful SSL error handling.

        Args:
            server_hostname: Optional hostname for error context
        """
        try:
            yield
        except ssl.SSLCertVerificationError as e:
            # Parse the error to provide more specific information
            error_str = str(e).lower()

            if "hostname" in error_str or "match" in error_str:
                raise HostnameVerificationError(
                    f"Hostname verification failed for '{server_hostname}': {e}"
                ) from e

            if "expired" in error_str:
                raise CertificateExpiredError(
                    f"Server certificate has expired: {e}"
                ) from e

            if "self-signed" in error_str or "self signed" in error_str:
                raise CertificateVerificationError(
                    f"Self-signed certificate not allowed: {e}"
                ) from e

            raise CertificateVerificationError(
                f"Certificate verification failed: {e}"
            ) from e

        except ssl.SSLError as e:
            error_str = str(e).lower()

            if "handshake" in error_str:
                raise HandshakeError(
                    f"SSL/TLS handshake failed: {e}"
                ) from e

            if "certificate" in error_str:
                raise CertificateVerificationError(
                    f"Certificate error: {e}"
                ) from e

            raise SSLConfigurationError(
                f"SSL error: {e}"
            ) from e

        except socket.timeout as e:
            raise HandshakeError(
                "SSL/TLS handshake timed out"
            ) from e

        except ConnectionError as e:
            raise HandshakeError(
                f"Connection error during SSL/TLS handshake: {e}"
            ) from e

    def _validate_config(self) -> None:
        """
        Validate the SSL configuration and check for file existence.
        """
        # Check a certificate file exists
        if self._config.cert_path:
            self._check_file_exists(self._config.cert_path, "certificate")

        # Check a key file exists
        if self._config.key_path:
            self._check_file_exists(self._config.key_path, "private key")

        # Check CA certificate file exists
        if self._config.ca_cert_path:
            self._check_file_exists(self._config.ca_cert_path, "CA certificate")

    def _check_file_exists(self, path: str, file_type: str) -> None:
        """Check if a file exists and is readable."""
        if not os.path.exists(path):
            raise SSLConfigurationError(
                f"{file_type.capitalize()} file not found: {path}"
            )
        if not os.path.isfile(path):
            raise SSLConfigurationError(
                f"{file_type.capitalize()} path is not a file: {path}"
            )
        if not os.access(path, os.R_OK):
            raise SSLConfigurationError(
                f"{file_type.capitalize()} file is not readable: {path}"
            )

    def _configure_tls_versions(self, context: ssl.SSLContext) -> None:
        """Configure minimum and maximum TLS versions."""
        context.minimum_version = self._config.min_tls_version
        context.maximum_version = self._config.max_tls_version

        logger.debug(
            f"Configured TLS versions: {self._config.min_tls_version.name} - "
            f"{self._config.max_tls_version.name}"
        )

    def _configure_ciphers(self, context: ssl.SSLContext) -> None:
        """Configure secure cipher suites."""
        cipher_string = self._config.cipher_suites or self.DEFAULT_CIPHERS
        try:
            context.set_ciphers(cipher_string)
            logger.debug(f"Configured cipher suites: {cipher_string[:50]}...")
        except ssl.SSLError as e:
            raise SSLConfigurationError(
                f"Invalid cipher suite configuration: {e}"
            ) from e

    def _configure_client_verification(self, context: ssl.SSLContext) -> None:
        """Configure certificate verification for client context."""
        if self._config.verify_certificates:
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = self._config.verify_hostname

            # Load CA certificates
            if self._config.ca_cert_path:
                try:
                    context.load_verify_locations(cafile=self._config.ca_cert_path)
                    logger.debug(f"Loaded CA certificates from {self._config.ca_cert_path}")
                except ssl.SSLError as e:
                    raise SSLConfigurationError(
                        f"Failed to load CA certificates from {self._config.ca_cert_path}: {e}"
                    ) from e
            else:
                # Use system default CA bundle
                try:
                    context.load_default_certs()
                    logger.debug("Loaded system default CA certificates")
                except ssl.SSLError as e:
                    logger.warning(f"Failed to load default CA certificates: {e}")
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            logger.warning("Certificate verification is DISABLED - connection is insecure")

    def _configure_server_verification(self, context: ssl.SSLContext) -> None:
        """Configure certificate verification for server context."""
        if self._config.require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED

            if self._config.ca_cert_path:
                try:
                    context.load_verify_locations(cafile=self._config.ca_cert_path)
                    logger.debug(
                        f"Loaded client CA certificates from {self._config.ca_cert_path}"
                    )
                except ssl.SSLError as e:
                    raise SSLConfigurationError(
                        f"Failed to load client CA certificates from {self._config.ca_cert_path}: {e}"
                    ) from e
            else:
                raise SSLConfigurationError(
                    "CA certificate path is required when require_client_cert=True"
                )
        else:
            context.verify_mode = ssl.CERT_OPTIONAL

    def _load_client_certificates(self, context: ssl.SSLContext) -> None:
        """Load client certificate and private key for mTLS."""
        if not self._config.has_client_cert:
            return

        self._load_certificate(context)

    def _load_certificate(self, context: SSLContext):
        try:
            context.load_cert_chain(
                certfile=self._config.cert_path,
                keyfile=self._config.key_path,
                password=self._config.key_password,
            )
            logger.debug(f"Loaded certificate from {self._config.cert_path}")
        except ssl.SSLError as e:
            if "password" in str(e).lower() or "decrypt" in str(e).lower():
                raise SSLConfigurationError(
                    f"Failed to decrypt private key at {self._config.key_path}. Check key_password."
                ) from e
            raise SSLConfigurationError(
                f"Failed to load certificate/key from {self._config.cert_path}: {e}"
            ) from e

    def _load_server_certificates(self, context: ssl.SSLContext) -> None:
        """Load server certificate and private key."""
        self._load_certificate(context)

    def _check_certificate_expiration(self, cert_path: str) -> None:
        """
        Check if a certificate is expired or about to expire.

        Args:
            cert_path: Path to the certificate file
        """
        try:
            # Use ssl module to parse certificate
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_cert_chain(cert_path, keyfile=self._config.key_path)
        except ssl.SSLError as e:
            logger.warning(f"Could not check certificate expiration: {e}")

    def _verify_certificate_pinning(self, ssl_socket: ssl.SSLSocket) -> None:
        """
        Verify the server certificate against pinned fingerprints.

        Args:
            ssl_socket: Connected SSL socket
        """
        if not self._config.pinned_certificates:
            return

        # Get the DER-encoded certificate
        cert_der = ssl_socket.getpeercert(binary_form=True)
        if not cert_der:
            raise CertificatePinningError(
                "No certificate received from server for pinning validation"
            )

        # Calculate SHA-256 fingerprint
        fingerprint = hashlib.sha256(cert_der).hexdigest().upper()

        # Normalize pinned fingerprints for comparison
        normalized_pins = [
            pin.upper().replace(":", "").replace(" ", "")
            for pin in self._config.pinned_certificates
        ]

        if fingerprint not in normalized_pins:
            raise CertificatePinningError(
                f"Certificate does not match any pinned fingerprint. "
                f"Expected: {self._config.pinned_certificates}, Got: {fingerprint}"
            )

        logger.debug(f"Certificate pinning validation successful: {fingerprint[:16]}...")

    def _read_certificate_file(self, path: str) -> bytes:
        """
        Read a certificate or key file.

        Args:
            path: Path to the file

        Returns:
            File contents as bytes
        """
        try:
            with open(path, "rb") as f:
                return f.read()
        except OSError as e:
            raise SSLConfigurationError(
                f"Failed to read certificate file {path}: {e}"
            ) from e

    @property
    def config(self) -> SSLConfig:
        """Return the SSL configuration."""
        return self._config

    @property
    def is_mtls_enabled(self) -> bool:
        """Return True if mutual TLS is configured."""
        return self._config.is_mtls_enabled

    def __repr__(self) -> str:
        """Return a string representation."""
        return (
            f"SecureSocketManager("
            f"mtls={self.is_mtls_enabled}, "
            f"verify_hostname={self._config.verify_hostname}, "
            f"tls_range={self._config.min_tls_version.name}-{self._config.max_tls_version.name})"
        )
