"""Home of the event and pipeline classes the hydration tests generate.

Checkpoints store classes by import path, so a class made with ``type(...)`` is only
resumable if it can be imported again. The test factories register what they build
here.
"""
