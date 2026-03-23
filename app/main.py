from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.v1.reports import router as report_router
from app.schemas.common import ApiResponse

app = FastAPI(
    title="Report Platform Backend", 
    version="0.1.0",
    docs_url="/consultant/docs",
    openapi_url="/consultant/openapi.json",
    )
app.include_router(report_router, prefix="/consultant/api")


@app.get("/health", response_model=ApiResponse[dict])
def health():
    return ApiResponse(data={"status": "ok"})


@app.exception_handler(Exception)
async def fallback_exception_handler(request: Request, exc: Exception):  # noqa: ARG001
    if hasattr(exc, "status_code") and hasattr(exc, "detail"):
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail:
            return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(
        status_code=500,
        content=ApiResponse(
            code=5000,
            message="internal error",
            error={"detail": str(exc)},
        ).model_dump(),
    )
