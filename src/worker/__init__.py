"""Standalone worker: consumes job messages and runs them.

The pipeline's execution end. A message is a pure pointer (``job_id`` /
``job_type``); the worker claims the run with a compare-and-set, hands it to a
handler, and records ``completed`` or ``failed``.

The handler shipped here is deliberately generic: it does no processing and
always succeeds, so this repo is a working skeleton to graft real work onto —
see ``worker.handlers``.
"""
