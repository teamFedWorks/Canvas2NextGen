import datetime
from typing import Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass(frozen=True)
class RegisteredAsset:
    checksum: str
    s3_key: str
    cdn_url: str
    size_bytes: int
    mime_type: str


class AssetRegistry:
    """
    Registry for global course asset deduplication.
    Provides transactional reservation capabilities to ensure concurrent workers
    don't duplicate uploads for the same file.
    """
    def __init__(self, db_client=None):
        self._db_client = db_client
        # In-memory cache fallback for testing/offline environments
        self._cache: Dict[str, RegisteredAsset] = {}

    def get_asset(self, checksum: str) -> Optional[RegisteredAsset]:
        """
        Retrieves a registered asset by checksum if it has been fully uploaded.
        """
        if not checksum:
            return None

        # 1. Query persistent DB client first
        if self._db_client:
            try:
                db = self._db_client.get_database()
                doc = db.assets.find_one({"checksum": checksum})
                if doc and doc.get("status") == "COMPLETED":
                    asset = RegisteredAsset(
                        checksum=doc["checksum"],
                        s3_key=doc["s3_key"],
                        cdn_url=doc["cdn_url"],
                        size_bytes=doc.get("size_bytes", 0),
                        mime_type=doc.get("mime_type", "application/octet-stream")
                    )
                    self._cache[checksum] = asset
                    return asset
            except Exception:
                pass

        # 2. Fall back to local cache
        return self._cache.get(checksum)

    def reserve_asset(self, checksum: str, worker_id: str, lease_secs: int = 300) -> Tuple[str, Optional[RegisteredAsset]]:
        """
        Attempts to atomically reserve the asset upload lock.
        Returns a tuple: (status, asset)
        
        Statuses:
        - "RESERVED": Successfully reserved by the caller. Caller must upload the asset.
        - "UPLOADING": Currently reserved and being uploaded by another worker.
        - "VERIFYING": Currently uploading/verifying.
        - "COMPLETED": Asset has already been successfully uploaded. Returns the RegisteredAsset.
        """
        if not checksum:
            return "UPLOADING", None

        now = datetime.datetime.utcnow()
        expires_at = now + datetime.timedelta(seconds=lease_secs)

        if self._db_client:
            try:
                db = self._db_client.get_database()
                col = db.assets
                
                # Atomic check-and-update using find_one_and_update
                res = col.find_one_and_update(
                    {
                        "checksum": checksum,
                        "status": {"$ne": "COMPLETED"},
                        "$or": [
                            {"expiresAt": {"$lt": now}},
                            {"owner": worker_id}
                        ]
                    },
                    {
                        "$set": {
                            "status": "RESERVED",
                            "owner": worker_id,
                            "expiresAt": expires_at
                        }
                    },
                    upsert=False,
                    return_document=True
                )

                if not res:
                    # Try to insert as new if it doesn't exist
                    try:
                        col.insert_one({
                            "checksum": checksum,
                            "hash": checksum,
                            "status": "RESERVED",
                            "owner": worker_id,
                            "expiresAt": expires_at
                        })
                    except Exception: # DuplicateKeyError
                        pass

                # Fetch back the record to see who won the race
                doc = col.find_one({"checksum": checksum})
                if doc:
                    status = doc.get("status", "UPLOADING")
                    if status == "COMPLETED":
                        asset = RegisteredAsset(
                            checksum=doc["checksum"],
                            s3_key=doc["s3_key"],
                            cdn_url=doc["cdn_url"],
                            size_bytes=doc.get("size_bytes", 0),
                            mime_type=doc.get("mime_type", "application/octet-stream")
                        )
                        self._cache[checksum] = asset
                        return "COMPLETED", asset
                    elif doc.get("owner") == worker_id:
                        return status, None
                    else:
                        return status, None
            except Exception:
                pass

        # Local fallback if DB is not present
        if checksum in self._cache:
            return "COMPLETED", self._cache[checksum]
        
        # Simulate local reservation success
        return "RESERVED", None

    def start_upload(self, checksum: str, worker_id: str, lease_secs: int = 300) -> None:
        """Transitions the asset status from RESERVED to UPLOADING."""
        if self._db_client:
            try:
                db = self._db_client.get_database()
                now = datetime.datetime.utcnow()
                expires_at = now + datetime.timedelta(seconds=lease_secs)
                db.assets.update_one(
                    {
                        "checksum": checksum, 
                        "owner": worker_id, 
                        "status": "RESERVED"
                    },
                    {
                        "$set": {
                            "status": "UPLOADING",
                            "expiresAt": expires_at
                        }
                    }
                )
            except Exception:
                pass

    def start_verification(self, checksum: str, worker_id: str, lease_secs: int = 60) -> None:
        """Transitions the asset status to VERIFYING."""
        if self._db_client:
            try:
                db = self._db_client.get_database()
                now = datetime.datetime.utcnow()
                expires_at = now + datetime.timedelta(seconds=lease_secs)
                db.assets.update_one(
                    {
                        "checksum": checksum, 
                        "owner": worker_id, 
                        "status": {"$in": ["RESERVED", "UPLOADING"]}
                    },
                    {
                        "$set": {
                            "status": "VERIFYING",
                            "expiresAt": expires_at
                        }
                    }
                )
            except Exception:
                pass

    def complete_upload(
        self, 
        checksum: str, 
        s3_key: str, 
        cdn_url: str, 
        size_bytes: int, 
        mime_type: str
    ) -> RegisteredAsset:
        """
        Updates the reserved asset document, marking it as COMPLETED and storing URLs.
        """
        asset = RegisteredAsset(
            checksum=checksum,
            s3_key=s3_key,
            cdn_url=cdn_url,
            size_bytes=size_bytes,
            mime_type=mime_type
        )
        
        if self._db_client:
            try:
                db = self._db_client.get_database()
                db.assets.update_one(
                    {"checksum": checksum},
                    {
                        "$set": {
                            "status": "COMPLETED",
                            "s3_key": s3_key,
                            "cdn_url": cdn_url,
                            "size_bytes": size_bytes,
                            "mime_type": mime_type
                        },
                        "$unset": {
                            "expiresAt": "",
                            "owner": ""
                        }
                    },
                    upsert=True
                )
            except Exception:
                pass

        # Sync local cache
        self._cache[checksum] = asset
        return asset
