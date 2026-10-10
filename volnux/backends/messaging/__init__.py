"""
Architectural Comparison
------------------------
+--------------------------+-----------------------+-----------------------+-----------------------+
| Attribute                | PushPopCapability     | PubSubCapability      | StreamCapability      |
+--------------------------+-----------------------+-----------------------+-----------------------+
| Persistence              | Ephemeral / In-Memory | Ephemeral (In-Flight) | Durable / Persistent  |
| Read Mechanics           | Destructive (Pop)     | Broadcast (Ephemeral) | Non-Destructive (Log) |
| Offsets                  | None                  | None                  | Independent / Tracked |
| Replayability            | No                    | No                    | Yes (From Offset/0/$) |
| Delivery Guarantees      | At-Most-Once          | Fire-and-Forget       | At-Least-Once (ACK)   |
+--------------------------+-----------------------+-----------------------+-----------------------+
"""
