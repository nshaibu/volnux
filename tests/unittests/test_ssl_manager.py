"""
Unit tests for SecureSocketManager class.
"""

import hashlib
import socket
import ssl
import unittest
from unittest.mock import MagicMock, Mock, patch, mock_open
import tempfile
import os

from volnux.security.ssl_config import SSLConfig
from volnux.security.ssl_manager import SecureSocketManager
from volnux.exceptions import (
    SSLConfigurationError,
    CertificateVerificationError,
    CertificateExpiredError,
    HostnameVerificationError,
    HandshakeError,
    CertificatePinningError,
)


class TestSecureSocketManagerInit(unittest.TestCase):
    """Tests for SecureSocketManager initialization."""

    def test_basic_initialization(self):
        """Test basic initialization with minimal config."""
        config = SSLConfig()
        manager = SecureSocketManager(config)
        self.assertEqual(manager.config, config)

    def test_initialization_validates_config(self):
        """Test that initialization validates file paths."""
        config = SSLConfig(
            cert_path="/nonexistent/cert.pem",
            key_path="/nonexistent/key.pem",
        )
        with self.assertRaises(SSLConfigurationError) as cm:
            SecureSocketManager(config)
        self.assertIn("not found", str(cm.exception))

    @patch.object(SecureSocketManager, "_check_certificate_expiration")
    @patch("os.path.exists")
    @patch("os.path.isfile")
    @patch("os.access")
    def test_initialization_with_valid_paths(
        self, mock_access, mock_isfile, mock_exists, mock_check_expiration
    ):
        """Test initialization succeeds with valid certificate paths."""
        mock_exists.return_value = True
        mock_isfile.return_value = True
        mock_access.return_value = True

        config = SSLConfig(
            cert_path="/valid/cert.pem",
            key_path="/valid/key.pem",
            ca_cert_path="/valid/ca.crt",
        )
        manager = SecureSocketManager(config)
        self.assertIsNotNone(manager)


class TestSecureSocketManagerContextCreation(unittest.TestCase):
    """Tests for SSL context creation."""

    def test_create_client_context_basic(self):
        """Test creating a basic client context."""
        config = SSLConfig(verify_certificates=False)

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            context = manager.create_client_context()

            self.assertIsInstance(context, ssl.SSLContext)
            self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
            self.assertGreaterEqual(context.maximum_version, ssl.TLSVersion.TLSv1_2)

    def test_create_client_context_with_verification(self):
        """Test creating client context with certificate verification."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as f:
            f.write("-----BEGIN CERTIFICATE-----\n")
            f.write("TEST\n")
            f.write("-----END CERTIFICATE-----\n")
            ca_path = f.name

        try:
            config = SSLConfig(
                ca_cert_path=ca_path,
                verify_certificates=True,
                verify_hostname=True,
            )

            with patch.object(SecureSocketManager, "_validate_config"):
                manager = SecureSocketManager(config)
                # This will fail because the cert is invalid, but we're testing the code path
                with self.assertRaises(SSLConfigurationError):
                    manager.create_client_context()
        finally:
            os.unlink(ca_path)

    def test_create_server_context_requires_cert(self):
        """Test that server context requires certificate."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(SSLConfigurationError) as cm:
                manager.create_server_context()
            self.assertIn("required", str(cm.exception).lower())


class TestSecureSocketManagerCipherSuites(unittest.TestCase):
    """Tests for cipher suite configuration."""
    def test_custom_cipher_configuration(self):
        """Test custom cipher suite configuration."""
        config = SSLConfig(
            cipher_suites="ECDHE-ECDSA-AES256-SHA",
            verify_certificates=False,
        )

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            context = manager.create_client_context()
            # Context should be created successfully with custom ciphers
            self.assertIsNotNone(context)

    def test_invalid_cipher_raises_error(self):
        """Test that invalid cipher suite raises error."""
        config = SSLConfig(
            cipher_suites="INVALID_CIPHER_THAT_DOES_NOT_EXIST",
            verify_certificates=False,
        )

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            with self.assertRaises(SSLConfigurationError):
                manager.create_client_context()


class TestSecureSocketManagerErrorHandling(unittest.TestCase):
    """Tests for SSL error handling context manager."""

    def test_handle_hostname_mismatch(self):
        """Test handling of hostname mismatch errors."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(HostnameVerificationError):
                with manager.handle_ssl_errors("example.com"):
                    raise ssl.SSLCertVerificationError(
                        1, "hostname 'example.com' doesn't match"
                    )

    def test_handle_expired_certificate(self):
        """Test handling of expired certificate errors."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(CertificateExpiredError):
                with manager.handle_ssl_errors():
                    raise ssl.SSLCertVerificationError(
                        1, "certificate has expired"
                    )

    def test_handle_self_signed_certificate(self):
        """Test handling of self-signed certificate errors."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(CertificateVerificationError) as cm:
                with manager.handle_ssl_errors():
                    raise ssl.SSLCertVerificationError(
                        1, "self-signed certificate"
                    )
            self.assertIn("Self-signed", str(cm.exception))

    def test_handle_handshake_error(self):
        """Test handling of handshake errors."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(HandshakeError):
                with manager.handle_ssl_errors():
                    raise ssl.SSLError(1, "handshake failure")

    def test_handle_timeout(self):
        """Test handling of timeout during handshake."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(HandshakeError) as cm:
                with manager.handle_ssl_errors():
                    raise socket.timeout("timed out")
            self.assertIn("timed out", str(cm.exception).lower())

    def test_handle_connection_error(self):
        """Test handling of connection errors."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(HandshakeError):
                with manager.handle_ssl_errors():
                    raise ConnectionRefusedError("Connection refused")


class TestSecureSocketManagerCertificatePinning(unittest.TestCase):
    """Tests for certificate pinning."""

    def test_pinning_validation_success(self):
        """Test successful certificate pinning validation."""
        # Create a mock certificate
        cert_der = b"mock certificate data"
        expected_fingerprint = hashlib.sha256(cert_der).hexdigest().upper()

        config = SSLConfig(pinned_certificates=[expected_fingerprint])

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=ssl.SSLSocket)
            mock_socket.getpeercert.return_value = cert_der

            # Should not raise an exception
            manager._verify_certificate_pinning(mock_socket)

    def test_pinning_validation_failure(self):
        """Test certificate pinning validation failure."""
        config = SSLConfig(pinned_certificates=["WRONGFINGERPRINT"])

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=ssl.SSLSocket)
            mock_socket.getpeercert.return_value = b"mock certificate data"

            with self.assertRaises(CertificatePinningError) as cm:
                manager._verify_certificate_pinning(mock_socket)
            self.assertIn("does not match", str(cm.exception))

    def test_pinning_with_no_certificate(self):
        """Test pinning validation when no certificate is received."""
        config = SSLConfig(pinned_certificates=["SOMEFINGERPRINT"])

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=ssl.SSLSocket)
            mock_socket.getpeercert.return_value = None

            with self.assertRaises(CertificatePinningError):
                manager._verify_certificate_pinning(mock_socket)

    def test_pinning_with_colon_formatted_fingerprint(self):
        """Test pinning with colon-formatted fingerprint."""
        cert_der = b"mock certificate data"
        fingerprint = hashlib.sha256(cert_der).hexdigest().upper()
        # Format with colons
        colon_fingerprint = ":".join(
            fingerprint[i : i + 2] for i in range(0, len(fingerprint), 2)
        )

        config = SSLConfig(pinned_certificates=[colon_fingerprint])

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=ssl.SSLSocket)
            mock_socket.getpeercert.return_value = cert_der

            # Should not raise - colons should be stripped
            manager._verify_certificate_pinning(mock_socket)


class TestSecureSocketManagerSocketWrapping(unittest.TestCase):
    """Tests for socket wrapping functionality."""

    @patch("volnux.security.ssl_manager.SecureSocketManager.create_client_context")
    def test_wrap_client_socket(self, mock_create_context):
        """Test wrapping a client socket."""
        mock_context = Mock(spec=ssl.SSLContext)
        mock_ssl_socket = Mock(spec=ssl.SSLSocket)
        mock_ssl_socket.version.return_value = "TLSv1.3"
        mock_context.wrap_socket.return_value = mock_ssl_socket
        mock_create_context.return_value = mock_context

        config = SSLConfig(verify_certificates=False)

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=socket.socket)
            mock_socket.gettimeout.return_value = None

            result = manager.wrap_client_socket(mock_socket, "example.com")

            self.assertEqual(result, mock_ssl_socket)
            mock_context.wrap_socket.assert_called_once()

    @patch("volnux.security.ssl_manager.SecureSocketManager.create_client_context")
    def test_wrap_client_socket_with_timeout(self, mock_create_context):
        """Test wrapping a client socket with timeout."""
        mock_context = Mock(spec=ssl.SSLContext)
        mock_ssl_socket = Mock(spec=ssl.SSLSocket)
        mock_ssl_socket.version.return_value = "TLSv1.3"
        mock_context.wrap_socket.return_value = mock_ssl_socket
        mock_create_context.return_value = mock_context

        config = SSLConfig(verify_certificates=False)

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            mock_socket = Mock(spec=socket.socket)
            mock_socket.gettimeout.return_value = 30

            result = manager.wrap_client_socket(
                mock_socket, "example.com", timeout=10
            )

            mock_socket.settimeout.assert_called_with(10)
            self.assertEqual(result, mock_ssl_socket)

    @patch("volnux.security.ssl_manager.SecureSocketManager.create_server_context")
    def test_wrap_server_socket(self, mock_create_context):
        """Test wrapping a server socket."""
        mock_context = Mock(spec=ssl.SSLContext)
        mock_ssl_socket = Mock(spec=ssl.SSLSocket)
        mock_ssl_socket.version.return_value = "TLSv1.3"
        mock_context.wrap_socket.return_value = mock_ssl_socket
        mock_create_context.return_value = mock_context

        config = SSLConfig(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
        )

        with patch.object(SecureSocketManager, "_validate_config"):
            with patch.object(SecureSocketManager, "_check_certificate_expiration"):
                manager = SecureSocketManager(config)

                mock_socket = Mock(spec=socket.socket)
                mock_socket.gettimeout.return_value = None

                result = manager.wrap_server_socket(mock_socket)

                self.assertEqual(result, mock_ssl_socket)
                mock_context.wrap_socket.assert_called_once_with(
                    mock_socket, server_side=True
                )


class TestSecureSocketManagerGRPCCredentials(unittest.TestCase):
    """Tests for gRPC credentials creation."""

    @patch("volnux.security.ssl_manager.grpc")
    def test_create_grpc_client_credentials(self, mock_grpc):
        """Test creating gRPC client credentials."""
        mock_credentials = Mock()
        mock_grpc.ssl_channel_credentials.return_value = mock_credentials

        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            result = manager.create_grpc_client_credentials()

            self.assertEqual(result, mock_credentials)
            mock_grpc.ssl_channel_credentials.assert_called_once()

    @patch("volnux.security.ssl_manager.grpc")
    @patch("builtins.open", mock_open(read_data=b"cert data"))
    def test_create_grpc_client_credentials_with_mtls(self, mock_grpc):
        """Test creating gRPC client credentials with mTLS."""
        mock_credentials = Mock()
        mock_grpc.ssl_channel_credentials.return_value = mock_credentials

        config = SSLConfig(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
            ca_cert_path="/path/to/ca.crt",
        )

        with patch.object(SecureSocketManager, "_validate_config"):
            with patch.object(SecureSocketManager, "_check_certificate_expiration"):
                manager = SecureSocketManager(config)
                result = manager.create_grpc_client_credentials()

                self.assertEqual(result, mock_credentials)
                call_kwargs = mock_grpc.ssl_channel_credentials.call_args[1]
                self.assertIsNotNone(call_kwargs.get("root_certificates"))
                self.assertIsNotNone(call_kwargs.get("private_key"))
                self.assertIsNotNone(call_kwargs.get("certificate_chain"))

    @patch("volnux.security.ssl_manager.grpc")
    @patch("builtins.open", mock_open(read_data=b"cert data"))
    def test_create_grpc_server_credentials(self, mock_grpc):
        """Test creating gRPC server credentials."""
        mock_credentials = Mock()
        mock_grpc.ssl_server_credentials.return_value = mock_credentials

        config = SSLConfig(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
        )

        with patch.object(SecureSocketManager, "_validate_config"):
            with patch.object(SecureSocketManager, "_check_certificate_expiration"):
                manager = SecureSocketManager(config)
                result = manager.create_grpc_server_credentials()

                self.assertEqual(result, mock_credentials)
                mock_grpc.ssl_server_credentials.assert_called_once()

    @patch("volnux.security.ssl_manager.grpc")
    def test_create_grpc_server_credentials_requires_cert(self, mock_grpc):
        """Test that server credentials require certificate."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)

            with self.assertRaises(SSLConfigurationError):
                manager.create_grpc_server_credentials()


class TestSecureSocketManagerProperties(unittest.TestCase):
    """Tests for SecureSocketManager properties."""

    def test_config_property(self):
        """Test config property returns the configuration."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            self.assertEqual(manager.config, config)

    def test_is_mtls_enabled_property(self):
        """Test is_mtls_enabled property."""
        config1 = SSLConfig()
        with patch.object(SecureSocketManager, "_validate_config"):
            manager1 = SecureSocketManager(config1)
            self.assertFalse(manager1.is_mtls_enabled)

        config2 = SSLConfig(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
            ca_cert_path="/path/to/ca.crt",
        )
        with patch.object(SecureSocketManager, "_validate_config"):
            with patch.object(SecureSocketManager, "_check_certificate_expiration"):
                manager2 = SecureSocketManager(config2)
                self.assertTrue(manager2.is_mtls_enabled)

    def test_repr(self):
        """Test string representation."""
        config = SSLConfig()

        with patch.object(SecureSocketManager, "_validate_config"):
            manager = SecureSocketManager(config)
            repr_str = repr(manager)

            self.assertIn("SecureSocketManager", repr_str)
            self.assertIn("mtls=", repr_str)
            self.assertIn("verify_hostname=", repr_str)


class TestSecureSocketManagerFileValidation(unittest.TestCase):
    """Tests for file validation."""

    def test_missing_cert_file(self):
        """Test error when certificate file is missing."""
        config = SSLConfig(
            cert_path="/nonexistent/cert.pem",
            key_path="/nonexistent/key.pem",
        )

        with self.assertRaises(SSLConfigurationError) as cm:
            SecureSocketManager(config)
        self.assertIn("not found", str(cm.exception))

    @patch("os.path.exists")
    def test_cert_path_not_a_file(self, mock_exists):
        """Test error when certificate path is a directory."""
        mock_exists.return_value = True

        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = False

            config = SSLConfig(
                cert_path="/path/to/directory",
                key_path="/path/to/key.pem",
            )

            with self.assertRaises(SSLConfigurationError) as cm:
                SecureSocketManager(config)
            self.assertIn("not a file", str(cm.exception))

    @patch("os.path.exists")
    @patch("os.path.isfile")
    def test_cert_file_not_readable(self, mock_isfile, mock_exists):
        """Test error when certificate file is not readable."""
        mock_exists.return_value = True
        mock_isfile.return_value = True

        with patch("os.access") as mock_access:
            mock_access.return_value = False

            config = SSLConfig(
                cert_path="/path/to/cert.pem",
                key_path="/path/to/key.pem",
            )

            with self.assertRaises(SSLConfigurationError) as cm:
                SecureSocketManager(config)
            self.assertIn("not readable", str(cm.exception))