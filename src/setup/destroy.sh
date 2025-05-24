#!/bin/bash

# destroy.sh: Script to destroy the Google Cloud infrastructure deployed by init.sh.
# Runs 'terraform destroy' and offers options for local cleanup.
#
# Usage: ./destroy.sh [--delete-zips <true|false>] [--delete-tf-files <true|false>] [--help]
#
# Arguments:
#   --delete-zips <true|false>: Optional. Whether to delete the local Cloud Functions source zip files.
#                               Defaults to 'true'.
#   --delete-tf-files <true|false>: Optional. Whether to delete local Terraform state and configuration files
#                                   (all files/directories in the 'terraform' folder EXCEPT 'main.tf').
#                                   Defaults to 'false'.
#   --help: Display this help message and exit. Must be used exclusively.
#
# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error.
set -euo pipefail

usage="🧨 Usage: $(basename "$0") [--delete-zips <true|false>] [--delete-tf-files <true|false>]
🧨 Usage: $(basename "$0") [--help]

Arguments:
  📦 --delete-zips <true|false>: Optional flag. Whether to delete the local Cloud Functions source zip files after destroy.
                                  Defaults to 'true'.
  🧹 --delete-tf-files <true|false>: Optional flag. Whether to delete local Terraform state and configuration files.
                                     This will remove ALL files and directories inside the 'terraform' folder, EXCEPT for 'main.tf'.
                                     Use with extreme caution, as this removes your local Terraform state and configuration.
                                     Defaults to 'false'.
  ℹ️  --help: Optional flag. Display this help message and exit. This flag must be used exclusively."

# --- Script Setup ---
SCRIPT_START_DIR="$(pwd)"
echo "📂 Starting directory: $SCRIPT_START_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
echo "📁 Script directory: $SCRIPT_DIR"
echo "📂 Current directory: $(pwd)"

# --- Default Parameter Values ---
DELETE_ZIPS="true"
DELETE_TF_FILES="false"

# --- Argument Parsing ---
echo "🔬 Parsing command-line arguments: $@"

for arg in "$@"; do
    if [ "$arg" == "--help" ]; then
        if [ "$#" -gt 1 ]; then
             echo "❌ Error: --help flag must be used exclusively."
             echo "$usage"
             exit 1
        fi
        echo "$usage"
        exit 0
    fi
done

while [ "$#" -gt 0 ]; do
    case "$1" in
        --delete-zips)
            if [ -z "${2:-}" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --delete-zips requires a boolean value (true or false)."
                echo "$usage"
                exit 1
            fi
            DELETE_ZIPS="$2"
            if [ "$DELETE_ZIPS" != "true" ] && [ "$DELETE_ZIPS" != "false" ]; then
                 echo "❌ Error: Invalid value for --delete-zips: '$DELETE_ZIPS'. Must be 'true' or 'false'."
                 echo "$usage"
                 exit 1
            fi
            echo "📦 Flag '--delete-zips' set to: $DELETE_ZIPS"
            shift 2
            ;;
        --delete-tf-files)
            if [ -z "${2:-}" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --delete-tf-files requires a boolean value (true or false)."
                echo "$usage"
                exit 1
            fi
            DELETE_TF_FILES="$2"
             if [ "$DELETE_TF_FILES" != "true" ] && [ "$DELETE_TF_FILES" != "false" ]; then
                 echo "❌ Error: Invalid value for --delete-tf-files: '$DELETE_TF_FILES'. Must be 'true' or 'false'."
                 echo "$usage"
                 exit 1
            fi
            echo "🧹 Flag '--delete-tf-files' set to: $DELETE_TF_FILES"
            shift 2
            ;;
        *)
            echo "❌ Error: Unknown argument '$1'."
            echo "$usage"
            exit 1
            ;;
    esac
done

echo "⚙️ Final parameters: DELETE_ZIPS=$DELETE_ZIPS, DELETE_TF_FILES=$DELETE_TF_FILES"

# --- Load Environment Variables ---
ENV_FILE="$SCRIPT_DIR/../../.env"

if [ -f "$ENV_FILE" ]; then
  echo "📄 Loading environment variables from $ENV_FILE..."
  set -a
  . "$ENV_FILE"
  set +a
else
  echo "❌ Error: .env file not found at $ENV_FILE."
  echo "This file is required to get the PROJECT_ID for terraform destroy."
  exit 1
fi

# --- Validate Required Environment Variables ---
# PROJECT_ID is strictly required for terraform destroy
if [ -z "${PROJECT_ID:-}" ]; then
    echo "❌ Error: Required environment variable 'PROJECT_ID' is not set or is empty in the .env file."
    echo "This variable is needed to identify the project targeted for destruction."
    exit 1
fi
echo "✅ PROJECT_ID is set: $PROJECT_ID"

# --- Define Paths ---
TF_DIR="$SCRIPT_DIR/../../terraform"
TFVARS_FILENAME="terraform.tfvars"
# No need for TFVARS_FILE full path here, as we delete everything except main.tf

# --- Navigate to Terraform Directory ---
echo "📂 Navigating to Terraform directory: $TF_DIR"
cd "$TF_DIR" || { echo "❌ Failed to navigate to Terraform directory '$TF_DIR'. Please ensure the path is correct."; exit 1; }
echo "📂 Current directory is now: $(pwd)"

# --- Check for Terraform Variables File (needed for destroy) ---
# Note: We check for terraform.tfvars specifically because terraform destroy -var-file needs it.
# If --delete-tf-files is true, this file WILL be deleted AFTER the destroy.
echo "🔎 Checking for Terraform variables file: $TFVARS_FILENAME (required for destroy)"
if [ ! -f "$TFVARS_FILENAME" ]; then
    echo "❌ Error: The Terraform variables file '$TFVARS_FILENAME' was not found in the Terraform directory ($(pwd))."
    echo "This file is typically generated by running the 'init.sh' script and contains necessary project-specific variables required for destroy."
    echo "Please ensure it exists before running destroy."
    exit 1
fi
echo "✅ Terraform variables file '$TFVARS_FILENAME' found."

# --- Execute Terraform Destroy ---
echo "🧨 Initiating Terraform destroy operation for project: $PROJECT_ID using '$TFVARS_FILENAME'..."
echo "This will destroy ALL infrastructure resources managed by this Terraform configuration in project '$PROJECT_ID'."
terraform destroy -var-file="$TFVARS_FILENAME" -auto-approve -lock=false || { echo "❌ Terraform destroy failed. Some resources may still exist."; exit 1; }
echo "✅ Terraform destroy operation finished."

# --- Optional Local Cleanup of Zip Files ---
if [ "$DELETE_ZIPS" == "true" ]; then
    echo "🧹 Starting local cleanup: removing generated zip files..."

    LOCAL_ZIP_NAME="source.zip"
    # Construct paths relative to the SCRIPT_DIR, not the current directory (which is TF_DIR)
    TABLE_STATS_LOCAL_ZIP="$SCRIPT_DIR/../cloud-functions/table-stats-builder/$LOCAL_ZIP_NAME"
    QUERY_SIMULATOR_LOCAL_ZIP="$SCRIPT_DIR/../cloud-functions/query-simulator/$LOCAL_ZIP_NAME"
    MAIL_NOTIFIER_LOCAL_ZIP="$SCRIPT_DIR/../cloud-functions/mail-notifier/$LOCAL_ZIP_NAME"

    echo "Checking for table-stats-builder local zip at: $TABLE_STATS_LOCAL_ZIP"
    if [ -f "$TABLE_STATS_LOCAL_ZIP" ]; then
      rm -f "$TABLE_STATS_LOCAL_ZIP"
      echo "🗑️ Deleted local zip: $TABLE_STATS_LOCAL_ZIP"
    else
      echo "⚪ Table-stats-builder local zip not found, skipping cleanup."
    fi

    echo "Checking for query-simulator local zip at: $QUERY_SIMULATOR_LOCAL_ZIP"
    if [ -f "$QUERY_SIMULATOR_LOCAL_ZIP" ]; then
      rm -f "$QUERY_SIMULATOR_LOCAL_ZIP"
      echo "🗑️ Deleted local zip: $QUERY_SIMULATOR_LOCAL_ZIP"
    else
      echo "⚪ Query-simulator local zip not found, skipping cleanup."
    fi

    echo "Checking for mail-notifier local zip at: $MAIL_NOTIFIER_LOCAL_ZIP"
    if [ -f "$MAIL_NOTIFIER_LOCAL_ZIP" ]; then
      rm -f "$MAIL_NOTIFIER_LOCAL_ZIP"
      echo "🗑️ Deleted local zip: $MAIL_NOTIFIER_LOCAL_ZIP"
    else
      echo "⚪ Mail-notifier local zip not found, skipping cleanup."
    fi
    echo "✅ Local zip file cleanup finished."
else
    echo "⚪ Skipping local zip file cleanup as requested."
fi

# --- Optional Local Cleanup of Terraform Files (excluding main.tf) ---
if [ "$DELETE_TF_FILES" == "true" ]; then
    echo "🧹 Starting local cleanup: removing all files and directories in $(pwd) EXCEPT 'main.tf'..."

    CLEANUP_COUNT=0
    # Iterate over all items (files and directories) in the current directory (TF_DIR)
    # Using find is safer than `rm -rf *` because it handles filenames with spaces/special chars better
    # and allows explicit exclusion.
    # -mindepth 1: Start processing from the current directory down (don't list '.')
    # -maxdepth 1: Only process items directly within the current directory
    # ! -name "main.tf": Exclude any item named "main.tf"
    # -exec rm -rf {} +: Execute rm -rf on all matched items in batches
    find . -mindepth 1 -maxdepth 1 ! -name "main.tf" -exec rm -rf {} +

    # Check if the find command was successful (return code 0).
    # Note: Checking individual removals within the loop is harder with find -exec +
    # A simpler approach after is to just check the exit code of find.
    if [ $? -eq 0 ]; then
        echo "✅ Local Terraform file cleanup finished. All items except 'main.tf' in $(pwd) have been removed."
    else
        echo "❌ Local Terraform file cleanup failed. Could not remove all items except 'main.tf' in $(pwd)."
        # Note: You might want to exit here if cleanup failure is critical
    fi

else
    echo "⚪ Skipping local Terraform file cleanup as requested (--delete-tf-files set to false)."
fi


echo "✅ Destroy script finished successfully."

# --- Return to starting directory (optional, but good practice) ---
echo "📁 Returning to starting directory: $SCRIPT_START_DIR"
# Check if we are already there before attempting to cd
if [ "$(pwd)" != "$SCRIPT_START_DIR" ]; then
  cd "$SCRIPT_START_DIR" || { echo "❌ Failed to return to starting directory!"; exit 1; }
fi
echo "✅ Returned to starting directory."