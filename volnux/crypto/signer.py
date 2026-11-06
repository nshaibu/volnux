from typing import Tuple, Optional, Union
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.types import (
    PrivateKeyTypes,
    PublicKeyTypes,
)


class KeyLoader:
    def __init__(self, key_pem: bytes):
        self.key_pem = key_pem
        self.key_type = None
        self.key: Optional[Union[PrivateKeyTypes, PublicKeyTypes]] = None

    def load_key(self):
        try:
            self.key = serialization.load_pem_private_key(self.key_pem, password=None)
            self.key_type = "private"
        except ValueError:
            try:
                self.key = serialization.load_pem_public_key(self.key_pem)
                self.key_type = "public"
            except ValueError:
                raise ValueError("Invalid key format")

    def key_pairs(self) -> Tuple[Optional[PrivateKeyTypes], PublicKeyTypes]:
        if self.key is None:
            self.load_key()

        if self.key_type == "private":
            return self.key, self.key.public_key()

        if self.key_type == "public":
            return None, self.key

        raise ValueError("Key was not loaded correctly")


class Signer:
    def __init__(
        self,
        private_key: Optional[PrivateKeyTypes] = None,
        public_key: Optional[PublicKeyTypes] = None,
    ):
        self.private_key = private_key
        self.public_key = public_key or (
            private_key.public_key() if private_key else None
        )

    def sign(self, data):
        if self.private_key is None:
            raise ValueError("private_key is required to sign data")
        return self.private_key.sign(data)

    def verify(self, data, signature):
        if self.public_key is None:
            raise ValueError("public_key is required to verify data")

        try:
            self.public_key.verify(signature, data)
            return True
        except InvalidSignature:
            return False
