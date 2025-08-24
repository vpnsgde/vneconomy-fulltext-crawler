#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Log file
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/run_log.txt"
mkdir -p "$LOG_DIR"

# Function to deactivate virtual environment on exit
cleanup() {
    if [[ -n "$VIRTUAL_ENV" ]]; then
        echo "Deactivating virtual environment..." | tee -a "$LOG_FILE"
        deactivate
    fi
}
trap cleanup EXIT

# Create necessary folders
echo "Creating project folders if not exist..." | tee -a "$LOG_FILE"
mkdir -p tmp paper_links content_data

# Create virtual environment if it does not exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..." | tee -a "$LOG_FILE"
    python3 -m venv .venv
fi

# Activate virtual environment
echo "Activating virtual environment..." | tee -a "$LOG_FILE"
source .venv/bin/activate

# Install requirements
if [ -f "requirements.txt" ]; then
    echo "Installing packages from requirements.txt..." | tee -a "$LOG_FILE"
    while IFS= read -r package || [[ -n "$package" ]]; do
        if [[ -n "$package" ]]; then
            echo "Installing $package..." | tee -a "$LOG_FILE"
            pip install "$package" | tee -a "$LOG_FILE"
        fi
    done < requirements.txt
else
    echo "requirements.txt not found, skipping installation." | tee -a "$LOG_FILE"
fi

# Run scripts in order
SCRIPTS=("pre_database.py" "pages_processing.py" "content_processing.py" "post_database.py")

for script in "${SCRIPTS[@]}"; do
    if [ -f "scripts/$script" ]; then
        echo "Running $script..." | tee -a "$LOG_FILE"
        python3 "scripts/$script" >> "$LOG_FILE" 2>&1
        echo "$script finished." | tee -a "$LOG_FILE"
    else
        echo "[WARNING] Script $script not found, skipping." | tee -a "$LOG_FILE"
    fi
done

echo "All tasks completed successfully." | tee -a "$LOG_FILE"
