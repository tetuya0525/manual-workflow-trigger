# ==============================================================================
# Dockerfile for Manual Workflow Trigger Service (Corrected)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# 1. Install dependencies as the root user first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Create the non-root user for the application
RUN adduser --system --group appuser

# 3. Copy application code and set ownership
COPY --chown=appuser:appuser . .

# 4. Switch to the non-root user
USER appuser

# Cloud Run PORT environment variable
ENV PORT 8080

# 5. Run the application as the non-root user
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "main:create_app()"]
