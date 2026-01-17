#!/usr/bin/env python3
"""
FastAPI server for pipeline execution.

Endpoint: POST /run
Request body: {"source_url": "https://..."}
Response: CSV file content (text/csv)
"""

import logging
import os
import subprocess
import sys
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, HttpUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Pipeline API",
    description="HTTP API for running the enrichment pipeline",
    version="1.0.0"
)

# Pipeline constants
OUTPUT_DIR = 'outputs'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'enriched_yes_companies.csv')
TIMEOUT_SECONDS = 600  # 10 minutes


class RunRequest(BaseModel):
    """Request model for /run endpoint."""
    source_url: HttpUrl


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Pipeline API",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/run")
async def run_pipeline(request: RunRequest):
    """
    Run the pipeline and return the enriched CSV file.
    
    Args:
        request: Request containing source_url
        
    Returns:
        CSV file content as text/csv response
        
    Raises:
        HTTPException: If pipeline execution fails or times out
    """
    source_url = str(request.source_url)
    logger.info(f"Received pipeline run request for URL: {source_url}")
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Build command
    cmd = [sys.executable, "pipeline.py", "--source-url", source_url]
    logger.info(f"Executing command: {' '.join(cmd)}")
    
    # Collect logs
    logs = []
    
    def log_line(line: str):
        """Capture log line and print it."""
        line = line.rstrip()
        if line:
            logs.append(line)
            logger.info(line)
    
    try:
        # Run pipeline with timeout
        # Use communicate() with timeout for cross-platform compatibility
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Capture output with timeout
            stdout, _ = process.communicate(timeout=TIMEOUT_SECONDS)
            
            # Log all output
            for line in stdout.splitlines():
                log_line(line)
            
            return_code = process.returncode
            
        except subprocess.TimeoutExpired:
            logger.error(f"Pipeline execution timed out after {TIMEOUT_SECONDS} seconds")
            process.kill()
            process.wait()
            raise HTTPException(
                status_code=504,
                detail={
                    "error": "Pipeline execution timed out",
                    "timeout_seconds": TIMEOUT_SECONDS,
                    "logs": logs[-100:]  # Last 100 lines
                }
            )
        
        if return_code != 0:
            logger.error(f"Pipeline failed with return code {return_code}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Pipeline execution failed",
                    "return_code": return_code,
                    "logs": logs[-100:]  # Last 100 lines
                }
            )
        
        logger.info("Pipeline completed successfully")
        
        # Check if output file exists
        if not os.path.exists(OUTPUT_FILE):
            logger.error(f"Output file not found: {OUTPUT_FILE}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Output file not generated",
                    "expected_path": OUTPUT_FILE,
                    "logs": logs[-100:]
                }
            )
        
        # Read and return CSV file
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                csv_content = f.read()
            
            logger.info(f"Returning CSV file with {len(csv_content)} characters")
            
            return Response(
                content=csv_content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=enriched_yes_companies.csv"
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to read output file: {e}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Failed to read output file",
                    "message": str(e),
                    "logs": logs[-100:]
                }
            )
    
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during pipeline execution: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unexpected error",
                "message": str(e),
                "logs": logs[-100:] if logs else []
            }
        )


if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment variable (default: 8000)
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
