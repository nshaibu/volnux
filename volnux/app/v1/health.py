import time
from ..app import get_current_app

app = get_current_app()


@app.get("/api/v1/status")
async def health_check():
    """Runtime health check. No authentication required."""

    uptime_seconds = time.time() - app.state.start_time
    uptime_str = f"{uptime_seconds:.2f} seconds"

    return {"status": "success", "data": {"version": "1.0.0", "uptime": uptime_str}}
