#!/bin/bash

# GPU discovery script for Apache Spark
# Returns JSON with GPU resources or empty array when no GPUs found

# Check for nvidia-smi existence and execute permission
if command -v nvidia-smi &> /dev/null && [ -x "$(command -v nvidia-smi)" ]; then
    # Safely get GPU count
    NUM_GPUS=$(nvidia-smi -L 2>/dev/null | grep -c "^GPU [0-9]\+:" || echo 0)
else
    NUM_GPUS=0
fi

# Generate addresses array
ADDRESSES=""
if [ "$NUM_GPUS" -gt 0 ]; then
    ADDRESSES=$(seq 0 $((NUM_GPUS-1)) | sed 's/^/"/; s/$/"/' | paste -sd ',' -)
fi

# Output JSON result
cat <<EOF
{
  "name": "gpu",
  "addresses": [$ADDRESSES]
}
EOF