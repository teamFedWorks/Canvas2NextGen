import datetime
import threading
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

class MongoLockManager:
    """
    Manages distributed locks inside MongoDB with compare-and-swap (CAS) semantics.
    Supports processing locks (keyed by jobId) and publication locks (keyed by contentFingerprint).
    """
    def __init__(self, db_client):
        self.db_client = db_client

    def _get_collection(self):
        db = self.db_client.get_database()
        return db.locks

    def acquire_lock(self, lock_id: str, worker_id: str, lease_secs: int) -> bool:
        """
        Atomically acquires a lock. Returns True if acquired, False otherwise.
        Acquisition succeeds if:
        1. Lock document does not exist (inserted).
        2. Lock document exists but is expired.
        3. Lock document exists and is owned by the current worker (renewal).
        """
        col = self._get_collection()
        now = datetime.datetime.utcnow()
        expires_at = now + datetime.timedelta(seconds=lease_secs)

        # Attempt to renew existing expired lock or renew our own lock
        try:
            res = col.update_one(
                {
                    "_id": lock_id,
                    "$or": [
                        {"expires_at": {"$lt": now}},
                        {"owner": worker_id}
                    ]
                },
                {
                    "$set": {
                        "owner": worker_id,
                        "expires_at": expires_at
                    }
                }
            )
            if res.modified_count > 0:
                return True
        except Exception as e:
            logger.warning(f"Error during lock renewal attempt for {lock_id}: {e}")

        # If update did not match (no document or not expired/owned by others), attempt insert
        try:
            col.insert_one({
                "_id": lock_id,
                "owner": worker_id,
                "expires_at": expires_at
            })
            return True
        except Exception: # DuplicateKeyError
            return False

    def release_lock(self, lock_id: str, worker_id: str) -> None:
        """Atomically releases the lock if owned by the current worker."""
        col = self._get_collection()
        try:
            col.delete_one({"_id": lock_id, "owner": worker_id})
        except Exception as e:
            logger.error(f"Failed to release lock {lock_id}: {e}")


class LockHeartbeat(threading.Thread):
    """
    Background daemon thread that periodically renews lease duration
    for a MongoDB lock using compare-and-swap (CAS) ownership validation.
    """
    def __init__(self, db_client, lock_id: str, worker_id: str, interval_secs: int = 30, lease_secs: int = 60):
        super().__init__(daemon=True)
        self.db_client = db_client
        self.lock_id = lock_id
        self.worker_id = worker_id
        self.interval_secs = interval_secs
        self.lease_secs = lease_secs
        self._stop_event = threading.Event()
        self.lock_lost = False

    def stop(self):
        """Signals the heartbeat loop to terminate."""
        self._stop_event.set()

    def run(self):
        db = self.db_client.get_database()
        col = db.locks
        
        while not self._stop_event.wait(self.interval_secs):
            try:
                now = datetime.datetime.utcnow()
                expires_at = now + datetime.timedelta(seconds=self.lease_secs)
                
                # CAS renewal: only update if we still own this lock
                res = col.update_one(
                    {"_id": self.lock_id, "owner": self.worker_id},
                    {"$set": {"expires_at": expires_at}}
                )
                
                if res.matched_count == 0:
                    logger.error(f"Lock ownership lost for {self.lock_id}! Heartbeat renewal failed.")
                    self.lock_lost = True
                    self.stop()
            except Exception as e:
                logger.warning(f"Failed to renew lock heartbeat for {self.lock_id}: {e}")
