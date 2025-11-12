# ----------------------------------------------------------------------------------
# STAGE 1: BUILDER - Compiles the lc0 binary (Cached after first successful run)
# ----------------------------------------------------------------------------------
FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS builder

# Install build dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    git \
    build-essential \
    clang \
    wget \
    pkg-config \
    libprotobuf-dev \
    libboost-all-dev \
    python3-pip \
    ninja-build \
    && rm -rf /var/lib/apt/lists/*

# Install meson (lc0's required build system)
RUN pip3 install meson

# Clone the lc0 repository
WORKDIR /lc0-src
RUN git clone https://github.com/LeelaChessZero/lc0.git .
RUN git checkout release/0.32 

# **HEAVY STEP:** Build lc0 with CUDA/cuDNN support. 
RUN ./build.sh

# ----------------------------------------------------------------------------------
# STAGE 2: FINAL - Sets up the runtime environment and FastAPI
# ----------------------------------------------------------------------------------
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# --- INSTALL PYTHON & FASTAPI ---
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install FastAPI, Uvicorn (ASGI server), python-chess, and pydantic
RUN pip3 install fastapi uvicorn python-chess pydantic

# --- COPY Lc0 ARTIFACTS AND DOWNLOAD WEIGHTS (AS ROOT) ---
# The binary and weights will be available at /usr/local/bin/
COPY --from=builder /lc0-src/build/release/lc0 /usr/local/bin/lc0

# ==============================================================================
# UPDATED: Downloading the specified T1-256x10-distilled network (ID 2432500.pb.gz)
# ==============================================================================
ENV STABLE_NETWORK_URL="https://storage.lczero.org/files/networks-contrib/t1-256x10-distilled-swa-2432500.pb.gz"
RUN wget -O /usr/local/bin/network.gz ${STABLE_NETWORK_URL}

# --- SETUP APP USER AND COPY API SCRIPT ---
RUN useradd -m juser
RUN mkdir /home/juser/app
RUN chown -R juser:juser /home/juser

# Copy the API script from your local machine into the container's app directory
# Assumes 'main.py' is the new local file name
COPY main.py /home/juser/app/main.py

USER juser
WORKDIR /home/juser/app

# Define the FastAPI port
EXPOSE 9999

# Default command: Start Uvicorn to serve the FastAPI application.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9999"]