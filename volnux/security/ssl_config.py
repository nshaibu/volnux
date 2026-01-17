from __future__ import annotations

import logging
import os
import ssl
import typing
import warnings
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

def _parse_bool(value: typing.Optional[str], default: bool = False) -> bool:
    """Parse a boolean value from an environment variable string."""
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def _parse_tls_version(
    value: typing.Optional[str], default: ssl.TLSVersion
) -> ssl.TLSVersion:
    """Parse a TLS version from a string."""
    if value is None:
        return default

    version_map = {
        "TLSv1": ssl.TLSVersion.TLSv1,
        "TLSv1.0": ssl.TLSVersion.TLSv1,
        "TLSv1_0": ssl.TLSVersion.TLSv1,
        "TLSv1.1": ssl.TLSVersion.TLSv1_1,
        "TLSv1_1": ssl.TLSVersion.TLSv1_1,
        "TLSv1.2": ssl.TLSVersion.TLSv1_2,
        "TLSv1_2": ssl.TLSVersion.TLSv1_2,
        "TLSv1.3": ssl.TLSVersion.TLSv1_3,
        "TLSv1_3": ssl.TLSVersion.TLSv1_3,
    }

    normalized = value.strip().replace(" ", "")
    if normalized in version_map:
        return version_map[normalized]

    logger.warning(f"Unknown TLS version '{value}', using default {default.name}")
    return default


@dataclass
class SSLConfig:
    """
    Configuration for SSL/TLS connections.

    This dataclass encapsulates all SSL/TLS settings needed for secure
    communication. It supports loading from environment variables.
    """
    cert_path: typing.Optional[str] = None
    key_path: typing.Optional[str] = None
    key_password: typing.Optional[str] = None
    ca_cert_path: typing.Optional[str] = None
    min_tls_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_2
    max_tls_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_3
    verify_hostname: bool = True
    verify_certificates: bool = True
    allow_self_signed: bool = False
    require_client_cert: bool = False
    cipher_suites: typing.Optional[str] = None
    pinned_certificates: typing.List[str] = field(default_factory=list)
    expiration_warning_days: int = 30

    def __post_init__(self) -> None:
        """Validate the configuration after initialization."""
        self._validate()

    def _validate(self) -> None:
        """
        Validate the SSL configuration.

        Raises:
            ValueError: If the configuration is invalid
        """
        # Validate TLS versions
        if self.min_tls_version > self.max_tls_version:
            raise ValueError(
                f"min_tls_version ({self.min_tls_version.name}) cannot be greater "
                f"than max_tls_version ({self.max_tls_version.name})"
            )

        # Warn about insecure TLS versions
        if self.min_tls_version < ssl.TLSVersion.TLSv1_2:
            warnings.warn(
                f"TLS version {self.min_tls_version.name} is considered insecure. "
                "TLS 1.2 or higher is recommended.",
                SecurityWarning,
                stacklevel=3,
            )

        # Validate certificate/key pair
        if self.cert_path and not self.key_path:
            raise ValueError("key_path is required when cert_path is provided")
        if self.key_path and not self.cert_path:
            raise ValueError("cert_path is required when key_path is provided")

        # Validate expiration warning days
        if self.expiration_warning_days < 0:
            raise ValueError("expiration_warning_days must be non-negative")

        # Warn about self-signed certificates
        if self.allow_self_signed:
            warnings.warn(
                "allow_self_signed=True is a security risk and should only be "
                "used in development environments.",
                SecurityWarning,
                stacklevel=3,
            )

        # Warn about disabled verification
        if not self.verify_certificates:
            warnings.warn(
                "verify_certificates=False disables certificate verification. "
                "This is a serious security risk.",
                SecurityWarning,
                stacklevel=3,
            )

        if not self.verify_hostname:
            warnings.warn(
                "verify_hostname=False disables hostname verification. "
                "This makes the connection vulnerable to MITM attacks.",
                SecurityWarning,
                stacklevel=3,
            )

    @property
    def has_client_cert(self) -> bool:
        """Return True if client certificate is configured."""
        return bool(self.cert_path and self.key_path)

    @property
    def has_ca_cert(self) -> bool:
        """Return True if CA certificate is configured."""
        return bool(self.ca_cert_path)

    @property
    def is_mtls_enabled(self) -> bool:
        """Return True if mutual TLS is configured."""
        return self.has_client_cert and self.has_ca_cert

    @property
    def has_pinned_certificates(self) -> bool:
        """Return True if certificate pinning is configured."""
        return bool(self.pinned_certificates)

    @classmethod
    def from_env(cls) -> "SSLConfig":
        """
        Create an SSLConfig from environment variables.

        Returns:
            SSLConfig: Configuration loaded from environment variables
        """
        pinned_certs_str = os.environ.get("ENV_SSL_PINNED_CERTIFICATES", "")
        pinned_certs = [
            fp.strip() for fp in pinned_certs_str.split(",") if fp.strip()
        ]

        warning_days_str = os.environ.get("ENV_SSL_EXPIRATION_WARNING_DAYS")
        warning_days = 30
        if warning_days_str:
            try:
                warning_days = int(warning_days_str)
            except ValueError:
                logger.warning(
                    f"Invalid VOLNUX_SSL_EXPIRATION_WARNING_DAYS value: "
                    f"'{warning_days_str}', using default 30"
                )

        return cls(
            cert_path=os.environ.get("ENV_SSL_CERT_PATH"),
            key_path=os.environ.get("ENV_SSL_KEY_PATH"),
            key_password=os.environ.get("ENV_SSL_KEY_PASSWORD"),
            ca_cert_path=os.environ.get("ENV_SSL_CA_CERT_PATH"),
            min_tls_version=_parse_tls_version(
                os.environ.get("ENV_SSL_MIN_TLS_VERSION"), ssl.TLSVersion.TLSv1_2
            ),
            max_tls_version=_parse_tls_version(
                os.environ.get("ENV_SSL_MAX_TLS_VERSION"), ssl.TLSVersion.TLSv1_3
            ),
            verify_hostname=_parse_bool(
                os.environ.get("ENV_SSL_VERIFY_HOSTNAME"), default=True
            ),
            verify_certificates=_parse_bool(
                os.environ.get("ENV_SSL_VERIFY_CERTIFICATES"), default=True
            ),
            allow_self_signed=_parse_bool(
                os.environ.get("ENV_SSL_ALLOW_SELF_SIGNED"), default=False
            ),
            require_client_cert=_parse_bool(
                os.environ.get("ENV_SSL_REQUIRE_CLIENT_CERT"), default=False
            ),
            cipher_suites=os.environ.get("ENV_SSL_CIPHER_SUITES"),
            pinned_certificates=pinned_certs,
            expiration_warning_days=warning_days,
        )

    @classmethod
    def development_config(cls) -> "SSLConfig":
        """
        Create a development-friendly (but INSECURE) configuration.

        Returns:
            SSLConfig: Insecure configuration for development
        """
        logger.warning(
            "Creating INSECURE development SSL configuration. "
            "DO NOT USE IN PRODUCTION!"
        )

        return cls(
            verify_hostname=False,
            verify_certificates=False,
            allow_self_signed=True,
            min_tls_version=ssl.TLSVersion.TLSv1_2,
            max_tls_version=ssl.TLSVersion.TLSv1_3,
        )

    @classmethod
    def from_dict(cls, config_dict: typing.Dict[str, typing.Any]) -> "SSLConfig":
        """
        Create an SSLConfig from a dictionary.

        Args:
            config_dict: Dictionary with SSL configuration

        Returns:
            SSLConfig: Configuration from dictionary

        """
        # Handle TLS version strings
        if "min_tls_version" in config_dict:
            min_ver = config_dict["min_tls_version"]
            if isinstance(min_ver, str):
                config_dict["min_tls_version"] = _parse_tls_version(
                    min_ver, ssl.TLSVersion.TLSv1_2
                )

        if "max_tls_version" in config_dict:
            max_ver = config_dict["max_tls_version"]
            if isinstance(max_ver, str):
                config_dict["max_tls_version"] = _parse_tls_version(
                    max_ver, ssl.TLSVersion.TLSv1_3
                )

        # Filter to only known fields
        known_fields = {
            "cert_path",
            "key_path",
            "key_password",
            "ca_cert_path",
            "min_tls_version",
            "max_tls_version",
            "verify_hostname",
            "verify_certificates",
            "allow_self_signed",
            "require_client_cert",
            "cipher_suites",
            "pinned_certificates",
            "expiration_warning_days",
        }
        filtered_dict = {k: v for k, v in config_dict.items() if k in known_fields}

        return cls(**filtered_dict)

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        """
        Convert configuration to a dictionary.

        Note: Sensitive fields like key_password are excluded.

        Returns:
            Dict containing non-sensitive configuration values
        """
        return {
            "cert_path": self.cert_path,
            "key_path": self.key_path,
            "ca_cert_path": self.ca_cert_path,
            "min_tls_version": self.min_tls_version.name,
            "max_tls_version": self.max_tls_version.name,
            "verify_hostname": self.verify_hostname,
            "verify_certificates": self.verify_certificates,
            "allow_self_signed": self.allow_self_signed,
            "require_client_cert": self.require_client_cert,
            "cipher_suites": self.cipher_suites,
            "has_pinned_certificates": self.has_pinned_certificates,
            "expiration_warning_days": self.expiration_warning_days,
        }

    def __repr__(self) -> str:
        """Return a string representation (without sensitive data)."""
        return (
            f"SSLConfig("
            f"cert_path={self.cert_path!r}, "
            f"ca_cert_path={self.ca_cert_path!r}, "
            f"min_tls={self.min_tls_version.name}, "
            f"max_tls={self.max_tls_version.name}, "
            f"verify_hostname={self.verify_hostname}, "
            f"verify_certs={self.verify_certificates})"
        )


class SecurityWarning(UserWarning):
    """Warning for security-related configuration issues."""
    pass
