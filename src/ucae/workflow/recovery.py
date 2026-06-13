import gzip
import base64
import json
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class RecoveryArtifact:
    """
    Encapsulates a compressed recovery state artifact, including versioning tags
    to ensure safe deserialization upon workflow replay.
    """
    schema_version: str
    provider_version: str
    serialization_version: str
    payload: str  # Base64 encoded payload
    compression: str = "gzip"

    @property
    def compressed_payload(self) -> str:
        return self.payload

    @classmethod
    def serialize(
        cls, 
        payload_dict: Dict[str, Any], 
        schema_version: str, 
        provider_version: str, 
        serialization_version: str = "1.0",
        compression: str = "gzip"
    ) -> "RecoveryArtifact":
        """
        Serializes a payload dictionary into a compressed, metadata-wrapped RecoveryArtifact.
        """
        json_bytes = json.dumps(payload_dict, default=str).encode("utf-8")
        if compression == "gzip":
            compressed_bytes = gzip.compress(json_bytes)
            b64_str = base64.b64encode(compressed_bytes).decode("utf-8")
        elif compression == "none":
            b64_str = base64.b64encode(json_bytes).decode("utf-8")
        elif compression == "zstd":
            try:
                import zstandard
                compressor = zstandard.ZstdCompressor()
                compressed_bytes = compressor.compress(json_bytes)
                b64_str = base64.b64encode(compressed_bytes).decode("utf-8")
            except ImportError:
                raise ValueError("zstandard library is not installed")
        else:
            raise ValueError(f"Unsupported compression method: {compression}")

        return cls(
            schema_version=schema_version,
            provider_version=provider_version,
            serialization_version=serialization_version,
            payload=b64_str,
            compression=compression
        )

    def deserialize(self) -> Dict[str, Any]:
        """
        Decompresses and deserializes the payload back to a dictionary.
        """
        compressed_bytes = base64.b64decode(self.payload.encode("utf-8"))
        if self.compression == "gzip":
            json_bytes = gzip.decompress(compressed_bytes)
        elif self.compression == "none":
            json_bytes = compressed_bytes
        elif self.compression == "zstd":
            try:
                import zstandard
                decompressor = zstandard.ZstdDecompressor()
                json_bytes = decompressor.decompress(compressed_bytes)
            except ImportError:
                raise ValueError("zstandard library is not installed")
        else:
            raise ValueError(f"Unsupported compression method: {self.compression}")
        return json.loads(json_bytes.decode("utf-8"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "provider_version": self.provider_version,
            "serialization_version": self.serialization_version,
            "compression": self.compression,
            "payload": self.payload
        }
