# This is the NEW Dockerfile for your main API (in the root directory)
# It's a simple Python server.

FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .
COPY constants.py .

# --- NEW: Copy the new API directory ---
COPY chessism_api/ ./chessism_api

# Expose the port the app runs on
EXPOSE 8000

# Run the Uvicorn server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]