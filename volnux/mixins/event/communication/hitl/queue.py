class HITLQueue:
    """
    Per-workflow HITL queue backed by Redis pub/sub.

    Each workflow instance has its own queue namespace:
        volnux:hitl:queue:{workflow_name}:{entry_id}

    Multiple workflow instances sharing a Redis cluster share
    infrastructure without sharing queue entries — namespacing
    provides isolation.

    The bootloader subscribes to all response channels matching
    the pattern volnux:hitl:response:* and routes responses to
    the correct queue entry by request_id.
    """

    def __init__(self, redis_client, workflow_name: str):
        self._redis = redis_client
        self._workflow_name = workflow_name
        self._queue_prefix = f"volnux:hitl:queue:{workflow_name}"
        self._response_prefix = "volnux:hitl:response"

    async def enqueue(self, entry: HITLQueueEntry) -> None:
        key = f"{self._queue_prefix}:{entry.entry_id}"
        await self._redis.set(
            key,
            json.dumps(asdict(entry)),
            ex=self._compute_ttl(entry.timeout_at),
        )
        logger.info(
            "HITLQueue '%s': enqueued entry %s for task %s",
            self._workflow_name,
            entry.entry_id,
            entry.task_id,
        )

    async def get_entry_by_request_id(
        self, request_id: str
    ) -> Optional[HITLQueueEntry]:
        """Scan queue for entry matching request_id."""
        pattern = f"{self._queue_prefix}:*"
        async for key in self._redis.scan_iter(pattern):
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                if data.get("request_id") == request_id:
                    return HITLQueueEntry(**data)
        return None

    async def dequeue(self, entry_id: str) -> None:
        """Remove entry after successful resumption."""
        await self._redis.delete(f"{self._queue_prefix}:{entry_id}")

    async def get_expired_entries(self) -> List[HITLQueueEntry]:
        """Return entries whose timeout_at has passed."""
        now = datetime.now(timezone.utc)
        expired = []
        async for key in self._redis.scan_iter(f"{self._queue_prefix}:*"):
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                timeout_at = data.get("timeout_at")
                if timeout_at:
                    deadline = datetime.fromisoformat(timeout_at)
                    if now >= deadline:
                        expired.append(HITLQueueEntry(**data))
        return expired

    async def publish_response(
        self,
        request_id: str,
        response: HITLResponse,
    ) -> None:
        """
        Publish a human response to the request's response channel.
        The bootloader's subscriber picks this up and routes it.
        """
        channel = f"{self._response_prefix}:{request_id}"
        await self._redis.publish(
            channel,
            json.dumps(asdict(response)),
        )
