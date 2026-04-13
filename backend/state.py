# Simple in-memory store for job state
# For production scale, swap this out for Redis
import asyncio

job_store: dict = {}

# Per-job SSE event queues -- shared between pipeline.py and main.py
job_event_queues: dict = {}

def get_job_queue(job_id: str) -> asyncio.Queue:
    if job_id not in job_event_queues:
        job_event_queues[job_id] = asyncio.Queue(maxsize=500)
    return job_event_queues[job_id]

def push_job_event(job_id: str, event: dict):
    """Push an event to the job's SSE queue (non-blocking, best-effort)."""
    if job_id in job_event_queues:
        try:
            job_event_queues[job_id].put_nowait(event)
        except asyncio.QueueFull:
            pass
        except Exception:
            pass
