"""
パイプライン管理モジュール

ジョブの実行、進捗管理、エラーハンドリングを担当
"""
import asyncio
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Callable, List
from enum import Enum

from src.pipeline.stages import AVAILABLE_STAGES, get_stage_by_number

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """ジョブステータス"""
    PENDING = "pending"
    IN_PROGRESS = "in-progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobCancelledError(Exception):
    """ジョブがキャンセルされた時の例外"""
    pass


class Job:
    """パイプラインジョブ"""

    def __init__(self, job_id: str, start_stage: int = 1):
        self.job_id = job_id
        self.start_stage = start_stage
        self.status = JobStatus.PENDING
        self.current_stage: Optional[int] = None
        self.progress_message = ""
        self.error_message: Optional[str] = None
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.cancelled = False

    def to_dict(self) -> Dict:
        """ジョブ情報を辞書形式で返す"""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "start_stage": self.start_stage,
            "current_stage": self.current_stage,
            "progress_message": self.progress_message,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class PipelineManager:
    """パイプライン管理クラス"""

    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.lock = threading.Lock()

    def create_job(self, start_stage: int = 1) -> str:
        """
        新しいジョブを作成

        Args:
            start_stage: 開始ステージ番号（1-4）

        Returns:
            ジョブID
        """
        job_id = str(uuid.uuid4())
        job = Job(job_id, start_stage)

        with self.lock:
            self.jobs[job_id] = job

        logger.info(f"Created job {job_id} (start_stage={start_stage})")
        return job_id

    def get_job(self, job_id: str) -> Optional[Job]:
        """ジョブ情報を取得"""
        with self.lock:
            return self.jobs.get(job_id)

    def get_all_jobs(self) -> List[Job]:
        """全ジョブ情報を取得"""
        with self.lock:
            return list(self.jobs.values())

    def cancel_job(self, job_id: str) -> bool:
        """
        ジョブをキャンセル

        Args:
            job_id: ジョブID

        Returns:
            キャンセル成功した場合True
        """
        job = self.get_job(job_id)
        if not job:
            return False

        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return False

        with self.lock:
            job.cancelled = True
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()

        logger.info(f"Cancelled job {job_id}")
        return True

    def run_pipeline(self, job_id: str) -> bool:
        """
        パイプラインを同期実行

        Args:
            job_id: ジョブID

        Returns:
            成功した場合True
        """
        job = self.get_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return False

        # ジョブ開始
        with self.lock:
            job.status = JobStatus.IN_PROGRESS
            job.started_at = datetime.now()

        try:
            # 指定されたステージから実行
            for stage_num in range(job.start_stage, len(AVAILABLE_STAGES) + 1):
                # キャンセルチェック
                if job.cancelled:
                    raise JobCancelledError(f"Job {job_id} was cancelled")

                stage = get_stage_by_number(stage_num)
                if not stage:
                    break

                with self.lock:
                    job.current_stage = stage_num
                    job.progress_message = f"Running {stage.name}"

                logger.info(f"Job {job_id}: {stage.name}")

                # ステージ実行
                def update_callback(message: str):
                    """進捗更新コールバック"""
                    with self.lock:
                        job.progress_message = message

                success = stage.run(update_callback)

                if not success:
                    raise Exception(f"Stage {stage_num} failed")

            # 成功
            with self.lock:
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now()
                job.progress_message = "Pipeline completed successfully"

            logger.info(f"Job {job_id} completed successfully")
            return True

        except JobCancelledError as e:
            logger.warning(str(e))
            return False

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)

            with self.lock:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.now()
                job.error_message = str(e)

            return False

    async def run_pipeline_async(self, job_id: str) -> bool:
        """
        パイプラインを非同期実行

        Args:
            job_id: ジョブID

        Returns:
            成功した場合True
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_pipeline, job_id)


# グローバルマネージャーインスタンス
pipeline_manager = PipelineManager()


def create_and_run_job(start_stage: int = 1) -> str:
    """
    ジョブを作成して実行

    Args:
        start_stage: 開始ステージ番号

    Returns:
        ジョブID
    """
    job_id = pipeline_manager.create_job(start_stage)

    # バックグラウンドで実行
    def run_job():
        pipeline_manager.run_pipeline(job_id)

    thread = threading.Thread(target=run_job, daemon=True)
    thread.start()

    return job_id
