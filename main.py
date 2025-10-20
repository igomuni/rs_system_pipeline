"""
メインエントリーポイント

FastAPI アプリケーション、またはCLIとして実行可能
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

from config import (
    DATA_DIR,
    DOWNLOAD_DIR,
    OUTPUT_DIR,
    RAW_DIR,
    NORMALIZED_DIR,
    PROCESSED_DIR,
    SCHEMA_DIR,
)
from src.pipeline.manager import pipeline_manager, create_and_run_job

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log', encoding='utf-8'),
    ]
)

logger = logging.getLogger(__name__)


def _ensure_directories():
    """必要なディレクトリを作成"""
    for directory in [DATA_DIR, DOWNLOAD_DIR, OUTPUT_DIR, RAW_DIR,
                     NORMALIZED_DIR, PROCESSED_DIR, SCHEMA_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


# FastAPIアプリ
app = FastAPI(
    title="RS System Pipeline API",
    description="行政事業レビュー過去データをRSシステム形式に変換するパイプライン",
    version="0.1.0",
)


class PipelineRequest(BaseModel):
    """パイプライン実行リクエスト"""
    start_stage: int = 1


@app.on_event("startup")
async def startup_event():
    """起動時の処理"""
    _ensure_directories()


@app.get("/")
async def root():
    """ルートエンドポイント"""
    return {
        "message": "RS System Pipeline API",
        "version": "0.1.0",
        "endpoints": {
            "run": "/api/pipeline/run",
            "status": "/api/pipeline/status/{job_id}",
            "jobs": "/api/pipeline/jobs",
            "cancel": "/api/pipeline/cancel/{job_id}",
        }
    }


@app.post("/api/pipeline/run")
async def run_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    """
    パイプラインを実行

    Args:
        request: パイプライン実行リクエスト
        background_tasks: バックグラウンドタスク

    Returns:
        ジョブID
    """
    if not 1 <= request.start_stage <= 4:
        raise HTTPException(status_code=400, detail="start_stage must be between 1 and 4")

    job_id = create_and_run_job(request.start_stage)

    return JSONResponse({
        "job_id": job_id,
        "message": f"Pipeline started from stage {request.start_stage}",
    })


@app.get("/api/pipeline/status/{job_id}")
async def get_job_status(job_id: str):
    """
    ジョブのステータスを取得

    Args:
        job_id: ジョブID

    Returns:
        ジョブステータス
    """
    job = pipeline_manager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JSONResponse(job.to_dict())


@app.get("/api/pipeline/jobs")
async def list_jobs():
    """
    全ジョブのリストを取得

    Returns:
        ジョブリスト
    """
    jobs = pipeline_manager.get_all_jobs()
    return JSONResponse({
        "jobs": [job.to_dict() for job in jobs],
        "total": len(jobs),
    })


@app.post("/api/pipeline/cancel/{job_id}")
async def cancel_job(job_id: str):
    """
    ジョブをキャンセル

    Args:
        job_id: ジョブID

    Returns:
        キャンセル結果
    """
    success = pipeline_manager.cancel_job(job_id)

    if not success:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job {job_id}")

    return JSONResponse({
        "job_id": job_id,
        "message": "Job cancelled successfully",
    })


@app.get("/api/results/{filename:path}")
async def download_result(filename: str):
    """
    処理済みファイルをダウンロード

    Args:
        filename: ファイル名（パス区切りを含む場合もあり）

    Returns:
        ファイルレスポンス
    """
    # パストラバーサル攻撃を防ぐため、絶対パス化して親ディレクトリをチェック
    file_path = (PROCESSED_DIR / filename).resolve()

    if not file_path.is_relative_to(PROCESSED_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file path")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {filename} not found")

    return FileResponse(file_path, filename=file_path.name)


def cli_main():
    """CLI実行"""
    import argparse

    parser = argparse.ArgumentParser(description="RS System Pipeline CLI")
    parser.add_argument(
        "--stage",
        type=int,
        default=1,
        choices=[1, 2, 3, 4],
        help="Start stage (1-4)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Process only specific year (e.g., 2014). If not specified, process all years.",
    )
    parser.add_argument(
        "--server",
        action="store_true",
        help="Run as API server",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="API server host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API server port",
    )

    args = parser.parse_args()

    if args.server:
        # APIサーバーとして起動
        import uvicorn
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        # CLI実行: 必要なディレクトリを作成
        _ensure_directories()

        logger.info(f"Starting pipeline from stage {args.stage}")
        if args.year:
            logger.info(f"Processing only year {args.year}")
        job_id = pipeline_manager.create_job(args.stage, target_year=args.year)
        success = pipeline_manager.run_pipeline(job_id)

        if success:
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)


if __name__ == "__main__":
    cli_main()
