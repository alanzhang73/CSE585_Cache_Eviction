#!/bin/bash

# Exit on error
set -e

echo "=== Step 1: Run dependencies ==="
sudo bash dependencies.sh

echo "=== Step 2: Ensure build directory exists ==="
mkdir -p build
cd build

echo "=== Step 3: Check for CUDA (nvcc) ==="
if ! command -v nvcc &> /dev/null
then
    echo "CUDA not found. Installing CUDA toolkit..."

    # Detect OS and install accordingly (Ubuntu/Debian assumed)
    if [ -f /etc/debian_version ]; then
        sudo apt update
        sudo apt install -y nvidia-cuda-toolkit
    else
        echo "Unsupported OS for automatic CUDA install."
        echo "Please install CUDA manually: https://developer.nvidia.com/cuda-downloads"
        exit 1
    fi
else
    echo "CUDA found: $(nvcc --version)"
fi

echo "=== Step 4: Build project ==="
cmake ..
make -j
sudo make install

echo "=== Step 5: Setup Python virtual environment ==="

# Go back to project root (assumes build/ is one level deep)
cd ..

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    sudo apt install python3.12-venv
    python3 -m venv .venv
fi

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing mooncake..."
pip install mooncake
pip install mooncake-transfer-engine

echo "=== Done ==="
echo "Virtual environment is active. To reactivate later, run: source venv/bin/activate"