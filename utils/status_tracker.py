"""Status tracking utility for onboarding jobs with SSE support"""

import asyncio
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Job status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Step status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class StatusTracker:
    """Track onboarding job status and progress with SSE event support"""

    def __init__(self):
        """Initialize status tracker"""
        self._jobs: Dict[str, Dict] = {}
        # SSE subscribers: merchant_id -> list of asyncio.Queue
        self._sse_subscribers: Dict[str, List[asyncio.Queue]] = {}

    def create_job(self, merchant_id: str, user_id: str) -> str:
        """
        Create a new onboarding job

        Args:
            merchant_id: Merchant identifier
            user_id: User identifier

        Returns:
            Job ID
        """
        job_id = f"{merchant_id}_{int(datetime.utcnow().timestamp())}"

        self._jobs[merchant_id] = {
            "job_id": job_id,
            "merchant_id": merchant_id,
            "user_id": user_id,
            "status": JobStatus.PENDING,
            "progress": 0,
            "total_steps": 8,
            "current_step": None,
            "steps": {
                "create_merchant_record": {
                    "status": StepStatus.PENDING,
                    "message": "Creating merchant record in database",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "create_folders": {
                    "status": StepStatus.PENDING,
                    "message": "Creating folder structure",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "process_products": {
                    "status": StepStatus.PENDING,
                    "message": "Processing product files",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "process_categories": {
                    "status": StepStatus.PENDING,
                    "message": "Processing category files",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "import_products_db": {
                    "status": StepStatus.PENDING,
                    "message": "Importing products to database with embeddings",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "convert_documents": {
                    "status": StepStatus.PENDING,
                    "message": "Converting documents to NDJSON",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "setup_vertex": {
                    "status": StepStatus.PENDING,
                    "message": "Setting up Vertex AI Search (with website crawling if URL provided)",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "generate_config": {
                    "status": StepStatus.PENDING,
                    "message": "Generating merchant configuration",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                },
                "finalize": {
                    "status": StepStatus.PENDING,
                    "message": "Finalizing onboarding",
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                }
            },
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "error": None
        }

        logger.info(f"Created job {job_id} for merchant {merchant_id}")
        return job_id

    def update_step_status(
        self,
        merchant_id: str,
        step_name: str,
        status: StepStatus,
        message: Optional[str] = None,
        error: Optional[str] = None
    ):
        """
        Update status of a specific step and notify SSE subscribers.

        Args:
            merchant_id: Merchant identifier
            step_name: Name of the step
            status: New status
            message: Optional status message
            error: Optional error message
        """
        if merchant_id not in self._jobs:
            logger.warning(f"Job not found for merchant: {merchant_id}")
            return

        job = self._jobs[merchant_id]

        if step_name not in job["steps"]:
            # Step not in predefined list — create it dynamically
            job["steps"][step_name] = {
                "status": StepStatus.PENDING,
                "message": step_name,
                "started_at": None,
                "completed_at": None,
                "error": None
            }

        step = job["steps"][step_name]
        step["status"] = status
        step["updated_at"] = datetime.utcnow().isoformat()

        if status == StepStatus.IN_PROGRESS:
            step["started_at"] = datetime.utcnow().isoformat()
            job["current_step"] = step_name
            job["status"] = JobStatus.IN_PROGRESS
        elif status == StepStatus.COMPLETED:
            step["completed_at"] = datetime.utcnow().isoformat()
            step["error"] = None
        elif status == StepStatus.FAILED:
            step["error"] = error
            job["status"] = JobStatus.FAILED
            job["error"] = error

        if message:
            step["message"] = message

        # Update overall progress
        completed_steps = sum(
            1 for s in job["steps"].values()
            if s["status"] in [StepStatus.COMPLETED, StepStatus.SKIPPED]
        )
        job["progress"] = int((completed_steps / job["total_steps"]) * 100)

        # Check if all steps are completed
        if all(
            s["status"] in [StepStatus.COMPLETED, StepStatus.SKIPPED]
            for s in job["steps"].values()
        ):
            job["status"] = JobStatus.COMPLETED
            job["progress"] = 100

        job["updated_at"] = datetime.utcnow().isoformat()
        logger.info(f"Updated step {step_name} for merchant {merchant_id}: {status}")

        # Push SSE event to all subscribers for this merchant
        self._push_sse_event(merchant_id, {
            "step": step_name,
            "step_status": status.value,
            "message": message or step.get("message", ""),
            "error": error,
            "progress": job["progress"],
            "job_status": job["status"].value,
            "current_step": job["current_step"],
            "timestamp": datetime.utcnow().isoformat(),
        })

    def _push_sse_event(self, merchant_id: str, event: dict):
        """Push event to all SSE subscribers for a merchant (thread-safe)."""
        subscribers = self._sse_subscribers.get(merchant_id, [])
        if not subscribers:
            return

        # Remove dead queues and push to active ones
        active = []
        for queue in subscribers:
            try:
                queue.put_nowait(event)
                active.append(queue)
            except Exception:
                # Queue full or closed — drop it
                pass
        self._sse_subscribers[merchant_id] = active

    def subscribe(self, merchant_id: str) -> asyncio.Queue:
        """
        Subscribe to SSE events for a merchant.
        Returns an asyncio.Queue that receives events.
        """
        queue = asyncio.Queue(maxsize=100)
        if merchant_id not in self._sse_subscribers:
            self._sse_subscribers[merchant_id] = []
        self._sse_subscribers[merchant_id].append(queue)
        logger.info(f"SSE subscriber added for merchant {merchant_id} (total: {len(self._sse_subscribers[merchant_id])})")
        return queue

    def unsubscribe(self, merchant_id: str, queue: asyncio.Queue):
        """Remove an SSE subscriber."""
        subscribers = self._sse_subscribers.get(merchant_id, [])
        try:
            subscribers.remove(queue)
        except ValueError:
            pass
        if not subscribers:
            self._sse_subscribers.pop(merchant_id, None)
        logger.info(f"SSE subscriber removed for merchant {merchant_id}")

    def get_status(self, merchant_id: str) -> Optional[Dict]:
        """
        Get current status of a job

        Args:
            merchant_id: Merchant identifier

        Returns:
            Job status dictionary or None if not found
        """
        return self._jobs.get(merchant_id)

    def get_all_jobs(self) -> Dict[str, Dict]:
        """Get all jobs"""
        return self._jobs

    def delete_job(self, merchant_id: str):
        """Delete a job from tracking"""
        if merchant_id in self._jobs:
            del self._jobs[merchant_id]
            logger.info(f"Deleted job for merchant: {merchant_id}")
