#!/bin/bash
# Updated run script for Aurora Alert System

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if we're in a virtual environment, if not try to activate one
if [[ -z "$VIRTUAL_ENV" && -f "$SCRIPT_DIR/venv/bin/activate" ]]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Run the NOAA alert script with any passed arguments
python "$SCRIPT_DIR/noaa alert.py" "$@"
