import json
import logging
import pickle
import typing

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class PersistentStateStore:
    """
    Redis-backed persistent storage for ExecutionContext snapshots.

    Key Schema:
        volnux:workflow:{workflow_id}:contexts -> Set of context IDs
        volnux:context:{state_id}:snapshot -> Hash of snapshot data
        volnux:context:{state_id}:lock -> Distributed lock key
        volnux:workflow:{workflow_id}:metadata -> Workflow-level metadata
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: typing.Optional[aioredis.Redis] = None
        self._serializer = StateSerializer()

    async def connect(self):
        """Initialize Redis connection pool"""
        self.redis = await aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=False,  # Handle binary data
        )

    async def disconnect(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()

    async def save_snapshot(self, snapshot: ContextSnapshot) -> None:
        """
        Persist a context snapshot to Redis.
        Uses Redis Hash for efficient field updates.
        """
        key = f"volnux:context:{snapshot.state_id}:snapshot"

        # Serialize to JSON
        data = json.dumps(snapshot.to_dict())

        async with self.redis.pipeline(transaction=True) as pipe:
            # Store snapshot
            await pipe.set(key, data)

            # Add to workflow's context set
            await pipe.sadd(
                f"volnux:workflow:{snapshot.workflow_id}:contexts", snapshot.state_id
            )

            # Set TTL (optional - for automatic cleanup)
            await pipe.expire(key, 86400 * 7)  # 7 days

            await pipe.execute()

        logger.debug(f"Saved snapshot for context {snapshot.state_id}")

    async def get_snapshot(self, state_id: str) -> typing.Optional[ContextSnapshot]:
        """Retrieve a context snapshot"""
        key = f"volnux:context:{state_id}:snapshot"
        data = await self.redis.get(key)

        if not data:
            return None

        snapshot_dict = json.loads(data)
        return ContextSnapshot.from_dict(snapshot_dict)

    async def get_active_snapshots(
        self, workflow_id: str, statuses: typing.Optional[typing.List[str]] = None
    ) -> typing.List[ContextSnapshot]:
        """
        Retrieve all context snapshots for a workflow, ordered by depth.

        Args:
            workflow_id: The workflow identifier
            statuses: Optional filter for execution statuses

        Returns:
            List of snapshots sorted by depth (parents before children)
        """
        # Get all context IDs for this workflow
        context_ids = await self.redis.smembers(
            f"volnux:workflow:{workflow_id}:contexts"
        )

        snapshots = []
        for context_id in context_ids:
            snapshot = await self.get_snapshot(
                context_id.decode() if isinstance(context_id, bytes) else context_id
            )
            if snapshot:
                if statuses is None or snapshot.status in statuses:
                    snapshots.append(snapshot)

        # Sort by depth to ensure parents are processed before children
        snapshots.sort(key=lambda s: s.depth)

        return snapshots

    async def update_snapshot_field(
        self, state_id: str, field_path: str, value: typing.Any
    ) -> None:
        """
        Update a specific field in a snapshot without full rewrite.

        Args:
            state_id: Context state ID
            field_path: Dot-notation path (e.g., "status", "metrics.end_time")
            value: New value
        """
        snapshot = await self.get_snapshot(state_id)
        if not snapshot:
            logger.warning(f"Snapshot {state_id} not found for update")
            return

        # Navigate to nested field
        snapshot_dict = snapshot.to_dict()
        keys = field_path.split(".")
        target = snapshot_dict

        for key in keys[:-1]:
            target = target[key]

        target[keys[-1]] = value

        # Save updated snapshot
        await self.save_snapshot(ContextSnapshot.from_dict(snapshot_dict))

    async def delete_snapshot(self, state_id: str, workflow_id: str) -> None:
        """Remove a snapshot from Redis"""
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.delete(f"volnux:context:{state_id}:snapshot")
            await pipe.srem(f"volnux:workflow:{workflow_id}:contexts", state_id)
            await pipe.execute()

    async def cleanup_workflow(self, workflow_id: str) -> int:
        """
        Delete all snapshots for a completed workflow.

        Returns:
            Number of contexts cleaned up
        """
        context_ids = await self.redis.smembers(
            f"volnux:workflow:{workflow_id}:contexts"
        )

        count = 0
        for context_id in context_ids:
            cid = context_id.decode() if isinstance(context_id, bytes) else context_id
            await self.delete_snapshot(cid, workflow_id)
            count += 1

        # Remove workflow metadata
        await self.redis.delete(f"volnux:workflow:{workflow_id}:metadata")

        return count
