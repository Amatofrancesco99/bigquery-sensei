#!/bin/bash

# init.sh: Script to initialize and deploy the BigQuery Job Analysis infrastructure using Terraform.
# Handles project creation, service enablement, IAM roles, BigQuery tables, Cloud Functions,
# Cloud Scheduler, and optional one-off Cloud Tasks.
#
# Usage: ./init.sh [--simulate-queries] [--run-initial-backfill [<start_time_iso8601> [<end_time_iso8601>]]]
# Usage: ./init.sh [--help]
#
# Arguments:
#   --simulate-queries: Optional flag. Deploy query simulator CF and one-off task.
#   --run-initial-backfill: Optional flag. Deploy one-off task for historical backfill.
#                           Accepts optional arguments:
#                           <start_time_iso8601>: Explicit start time (ISO 8601).
#                           <end_time_iso8601>: Explicit end time (ISO 8601).
#                           - 0 args: Calculate 1 year ago to (now - TIMEDELTA_CONSIDERED_SEC).
#                           - 1 arg: End time is (now - TIMEDELTA_CONSIDERED_SEC).
#                           - 2 args: Use explicit time range.
#   --help: Display this help message and exit. Must be used exclusively.
#
# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error.
set -euo pipefail

usage="🚀 Usage: $(basename "$0") [--simulate-queries] [--run-initial-backfill [<start_time_iso8601> [<end_time_iso8601>]]]
🚀 Usage: $(basename "$0") [--help]

Arguments:
  🔬 --simulate-queries: Optional flag. Deploy query simulator Cloud Function and its one-off Cloud Task trigger.
  📅 --run-initial-backfill: Optional flag. Deploy a one-off Cloud Task for historical backfill.
                      Accepts optional arguments after this flag:
                      <start_time_iso8601>: The explicit start time for the backfill window (ISO 8601 format, e.g., 2023-01-01T00:00:00Z).
                      <end_time_iso8601>: The explicit end time for the backfill window (ISO 8601 format, e.g., 2024-01-01T00:00:00Z).
                      - If no times are provided (0 arguments): The window is calculated as 1 year ago from (now - TIMEDELTA_CONSIDERED_SEC) to (now - TIMEDELTA_CONSIDERED_SEC) (UTC).
                      - If only <start_time_iso8601> is provided (1 argument): The end time is calculated as (now - TIMEDELTA_CONSIDERED_SEC) (UTC).
                      - If both <start_time_iso8601> and <end_time_iso8601> are provided (2 arguments): The explicit time range is used.
  ℹ️  --help: Optional flag. Display this help message and exit. This flag must be used exclusively."

SCRIPT_START_DIR="$(pwd)"
echo "📂 Starting directory: $SCRIPT_START_DIR"
echo "📂 Current directory: $(pwd)"

DEPLOY_QUERY_SIMULATOR="false"
DEPLOY_INITIAL_BACKFILL_TASK="false"
BACKFILL_START_TIME=""
BACKFILL_END_TIME=""
CALCULATE_BACKFILL_TIMES="false"
CALCULATE_BACKFILL_END_TIME="false"

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
        --simulate-queries)
            DEPLOY_QUERY_SIMULATOR="true"
            echo "🟢 Flag '--simulate-queries' set. Query simulator function and trigger will be deployed."
            shift
            ;;
        --run-initial-backfill)
            DEPLOY_INITIAL_BACKFILL_TASK="true"
            echo "🟢 Flag '--run-initial-backfill' set."
            shift

            if [ "$#" -eq 0 ] || [[ "$1" == --* ]]; then
                CALCULATE_BACKFILL_TIMES="true"
                echo "📅 No explicit time range provided for backfill. Will calculate 1 year ago from (now - TIMEDELTA_CONSIDERED_SEC) to (now - TIMEDELTA_CONSIDERED_SEC) (UTC)."
            elif [ "$#" -eq 1 ] && [[ "$1" != --* ]]; then
                BACKFILL_START_TIME="$1"
                CALCULATE_BACKFILL_END_TIME="true"
                echo "📅 Explicit start time provided for backfill: $BACKFILL_START_TIME. End time will be calculated as now minus TIMEDELTA_CONSIDERED_SEC (UTC)."
                shift
            elif [ "$#" -ge 2 ] && [[ "$1" != --* ]] && [[ "$2" != --* ]]; then
                BACKFILL_START_TIME="$1"
                BACKFILL_END_TIME="$2"
                echo "📅 Explicit backfill time range specified: $BACKFILL_START_TIME to $BACKFILL_END_TIME"
                shift 2
            else
                 echo "❌ Error: --run-initial-backfill expects either no arguments, one argument (<start_time_iso8601>), or two arguments (<start_time_iso8601> <end_time_iso8601>)."
                 echo "$usage"
                 exit 1
            fi
            ;;
        *)
            echo "❌ Error: Unknown argument '$1'."
            echo "$usage"
            exit 1
            ;;
    esac
done

ENV_FILE="../../.env"

if [ -f "$ENV_FILE" ]; then
  echo "📄 Loading environment variables from $ENV_FILE..."
  set -a
  . "$ENV_FILE"
  set +a
else
  echo "❌ .env file not found at $ENV_FILE. Please ensure it's in the project root."
  exit 1
fi

always_required_vars=(PROJECT_ID REGION CLOUD_FUNCTIONS_SOURCE_BUCKET BILLING_ACCOUNT_ID LLM_MODEL_STATSBUILDER TIMEDELTA_CONSIDERED_SEC)

echo "🔎 Checking for always required environment variables..."
for var_name in "${always_required_vars[@]}"; do
  if [ -z "${!var_name+x}" ] || [ -z "${!var_name}" ]; then
    echo "❌ Required environment variable '$var_name' is not set or is empty in the .env file."
    echo "Please ensure all core required variables are defined and have values."
    exit 1
  fi
done
echo "✅ All always required environment variables are set."

conditional_mail_vars=(MAILERSEND_API_TOKEN MAILERSEND_DOMAIN LLM_MODEL_MAILNOTIFIER MAIL_REPORT_FREQUENCY MIN_STATS_JOBS_SEND_EMAIL)
MAILERSEND_ENABLED="false"

if [ -n "${MAILERSEND_API_TOKEN:-}" ]; then
    MAILERSEND_ENABLED="true"
    echo "✉️ MailerSend API Token is set. Checking for related variables..."

    for var_name in "${conditional_mail_vars[@]}"; do
        if [ -z "${!var_name+x}" ] || [ -z "${!var_name}" ]; then
            echo "❌ MailerSend is enabled, but required variable '$var_name' is not set or is empty in the .env file."
            echo "Please ensure all MailerSend related variables are defined if MAILERSEND_API_TOKEN is present."
            exit 1
        fi
    done
     echo "✅ All MailerSend-related environment variables are set."
else
    echo "⚪ MailerSend API Token is not set. Skipping checks for MailerSend related variables and their deployment."
fi

if ! [[ "$TIMEDELTA_CONSIDERED_SEC" =~ ^[0-9]+$ ]] || [ "$TIMEDELTA_CONSIDERED_SEC" -le 0 ]; then
    echo "❌ Validation Error: TIMEDELTA_CONSIDERED_SEC must be a positive integer. Received: '$TIMEDELTA_CONSIDERED_SEC'"
    echo "Please update TIMEDELTA_CONSIDERED_SEC in your .env file."
    exit 1
fi
echo "✅ TIMEDELTA_CONSIDERED_SEC is a valid positive integer: $TIMEDELTA_CONSIDERED_SEC"

if [ "$MAILERSEND_ENABLED" = "true" ]; then
    if ! [[ "$MIN_STATS_JOBS_SEND_EMAIL" =~ ^[0-9]+$ ]] || [ "$MIN_STATS_JOBS_SEND_EMAIL" -lt 0 ]; then
        echo "❌ Validation Error: MIN_STATS_JOBS_SEND_EMAIL must be a non-negative integer when MailerSend is configured. Received: '$MIN_STATS_JOBS_SEND_EMAIL'"
        echo "Please update MIN_STATS_JOBS_SEND_EMAIL in your .env file."
        exit 1
    fi
    echo "✅ MIN_STATS_JOBS_SEND_EMAIL is a valid non-negative integer: $MIN_STATS_JOBS_SEND_EMAIL"
fi

NOW_EPOCH=$(date +%s)

if [[ "$DEPLOY_INITIAL_BACKFILL_TASK" == "true" ]]; then
    if [[ "$CALCULATE_BACKFILL_TIMES" == "true" ]]; then
        BACKFILL_END_EPOCH=$((NOW_EPOCH - TIMEDELTA_CONSIDERED_SEC))
        BACKFILL_START_EPOCH=$((BACKFILL_END_EPOCH - 31536000))
        BACKFILL_START_TIME=$(date -u -d "@$BACKFILL_START_EPOCH" +"%Y-%m-%dT%H:%M:%SZ")
        BACKFILL_END_TIME=$(date -u -d "@$BACKFILL_END_EPOCH" +"%Y-%m-%dT%H:%M:%SZ")
        echo "📅 Calculated backfill time range: $BACKFILL_START_TIME to $BACKFILL_END_TIME (1 year before end time to end time UTC)."

    elif [[ "$CALCULATE_BACKFILL_END_TIME" == "true" ]]; then
        BACKFILL_END_EPOCH=$((NOW_EPOCH - TIMEDELTA_CONSIDERED_SEC))
        BACKFILL_END_TIME=$(date -u -d "@$BACKFILL_END_EPOCH" +"%Y-%m-%dT%H:%M:%SZ")
        echo "📅 Calculated backfill end time: $BACKFILL_END_TIME (now minus TIMEDELTA_CONSIDERED_SEC UTC)."
        echo "   Using provided start time: $BACKFILL_START_TIME"
    fi
fi

if [[ "$DEPLOY_INITIAL_BACKFILL_TASK" == "true" ]]; then
    if [[ -z "$BACKFILL_START_TIME" || -z "$BACKFILL_END_TIME" ]]; then
         echo "❌ Error: --run-initial-backfill was specified but the start or end time is missing after argument processing."
         echo "Please provide a valid time range or let the script calculate it."
         exit 1
    fi
    if ! date -Iseconds -d "$(printf %q "$BACKFILL_START_TIME")" >/dev/null 2>&1; then
        echo "❌ Error: Invalid start_time format '$BACKFILL_START_TIME'. Please use ISO 8601 format (e.g., 2023-01-01T00:00:00Z)."
        echo "$usage"
        exit 1
    fi
     if ! date -Iseconds -d "$(printf %q "$BACKFILL_END_TIME")" >/dev/null 2>&1; then
        echo "❌ Error: Invalid end_time format '$BACKFILL_END_TIME'. Please use ISO 8601 format (e.g., 2024-01-01T00:00:00Z)."
        echo "$usage"
        exit 1
    fi

    START_EPOCH_VALIDATE=$(date -u -d "$(printf %q "$BACKFILL_START_TIME")" +%s)
    END_EPOCH_VALIDATE=$(date -u -d "$(printf %q "$BACKFILL_END_TIME")" +%s)

    if [ "$START_EPOCH_VALIDATE" -ge "$END_EPOCH_VALIDATE" ]; then
        echo "❌ Validation Error: Backfill start time ('$BACKFILL_START_TIME') must be strictly before the end time ('$BACKFILL_END_TIME')."
        exit 1
    fi

    echo "✅ Backfill times seem valid and ordered correctly (basic format and order check)."
fi

TFVARS_PROJECT_NAME_TO_CREATE=""
PROJECT_NAME_TO_CREATE_VAL="${PROJECT_NAME_TO_CREATE:-}"
if [ -n "$PROJECT_NAME_TO_CREATE_VAL" ]; then
  echo "Project display name specified: $PROJECT_NAME_TO_CREATE_VAL"
  TFVARS_PROJECT_NAME_TO_CREATE="project_name_to_create = \"$PROJECT_NAME_TO_CREATE_VAL\""
fi

TFVARS_DEVELOPER_USERS="[]"
DEVELOPER_USERS_VAL="${DEVELOPER_USERS:-}"

if [ -n "$DEVELOPER_USERS_VAL" ]; then
    TFVARS_DEVELOPER_USERS="["
    IFS=',' read -r -a user_array <<< "${DEVELOPER_USERS_VAL// /}"
    first_email=true
    for email in "${user_array[@]}"; do
        trimmed_email=$(echo "$email" | xargs)
        if [ -n "$trimmed_email" ]; then
            if [ "$first_email" = false ]; then
                TFVARS_DEVELOPER_USERS+=", "
            fi
            TFVARS_DEVELOPER_USERS+="\"$trimmed_email\""
            first_email=false
        fi
    done
    TFVARS_DEVELOPER_USERS+="]"
fi
echo "Generated TFVARS developer_users HCL list: $TFVARS_DEVELOPER_USERS"

TABLE_STATS_SOURCE_DIR="../../src/cloud-functions/table-stats-builder"
QUERY_SIMULATOR_SOURCE_DIR="../../src/cloud-functions/query-simulator"
MAIL_NOTIFIER_SOURCE_DIR="../../src/cloud-functions/mail-notifier"

LOCAL_ZIP_NAME="source.zip"
GCS_OBJECT_NAME_TABLE_STATS="table-stats-source.zip"
GCS_OBJECT_NAME_QUERY_SIMULATOR="query-simulator-source.zip"
GCS_OBJECT_NAME_MAIL_NOTIFIER="mail-notifier-source.zip"

TF_DIR="../../terraform"
TFVARS="$TF_DIR/terraform.tfvars"

echo "📦 Zipping Cloud Function source from $TABLE_STATS_SOURCE_DIR into $LOCAL_ZIP_NAME..."
if [ ! -d "$TABLE_STATS_SOURCE_DIR" ]; then
  echo "❌ Error: Source directory not found for table-stats-builder: $TABLE_STATS_SOURCE_DIR"
  exit 1
fi
(cd "$TABLE_STATS_SOURCE_DIR" && rm -f "$LOCAL_ZIP_NAME" && zip -q -r "$LOCAL_ZIP_NAME" .) || { echo "❌ Failed to create zip archive for table-stats-builder."; exit 1; }
echo "✅ Zipping table-stats-builder completed."

if [[ "$DEPLOY_QUERY_SIMULATOR" == "true" ]]; then
  echo "📦 Zipping Cloud Function source from $QUERY_SIMULATOR_SOURCE_DIR into $LOCAL_ZIP_NAME..."
  if [ ! -d "$QUERY_SIMULATOR_SOURCE_DIR" ]; then
    echo "❌ Error: Source directory not found for query-simulator: $QUERY_SIMULATOR_SOURCE_DIR"
    exit 1
  fi
  (cd "$QUERY_SIMULATOR_SOURCE_DIR" && rm -f "$LOCAL_ZIP_NAME" && zip -q -r "$LOCAL_ZIP_NAME" .) || { echo "❌ Failed to create zip archive for query-simulator."; exit 1; }
  echo "✅ Zipping query-simulator completed."
else
  echo "⚪ Skipping zipping query simulator source as deployment is not requested."
  if [ -f "$QUERY_SIMULATOR_SOURCE_DIR/$LOCAL_ZIP_NAME" ]; then
    echo "🗑️ Removing existing query simulator local zip file."
    rm -f "$QUERY_SIMULATOR_SOURCE_DIR/$LOCAL_ZIP_NAME"
  fi
fi

if [ "$MAILERSEND_ENABLED" = "true" ]; then
    echo "📦 Zipping Cloud Function source from $MAIL_NOTIFIER_SOURCE_DIR into $LOCAL_ZIP_NAME..."
    if [ ! -d "$MAIL_NOTIFIER_SOURCE_DIR" ]; then
      echo "❌ Error: Source directory not found for mail-notifier: $MAIL_NOTIFIER_SOURCE_DIR"
      exit 1
    fi
    (cd "$MAIL_NOTIFIER_SOURCE_DIR" && rm -f "$LOCAL_ZIP_NAME" && zip -q -r "$LOCAL_ZIP_NAME" .) || { echo "❌ Failed to create zip archive for mail-notifier."; exit 1; }
    echo "✅ Zipping mail-notifier completed."
else
    echo "⚪ Skipping zipping mail notifier source as MailerSend is not configured."
    if [ -f "$MAIL_NOTIFIER_SOURCE_DIR/$LOCAL_ZIP_NAME" ]; then
      echo "🗑️ Removing existing mail notifier local zip file."
      rm -f "$MAIL_NOTIFIER_SOURCE_DIR/$LOCAL_ZIP_NAME"
    fi
fi

echo "✍️ Writing variables to $TFVARS..."
cat > "$TFVARS" <<EOF
gcp_project_id = "$PROJECT_ID"
billing_account_id = "$BILLING_ACCOUNT_ID"
$TFVARS_PROJECT_NAME_TO_CREATE
gcp_region = "$REGION"
cloud_functions_source_bucket = "$CLOUD_FUNCTIONS_SOURCE_BUCKET"
cloud_function_source_object_table_stats = "$GCS_OBJECT_NAME_TABLE_STATS"
cloud_function_source_object_mail_notifier = "${GCS_OBJECT_NAME_MAIL_NOTIFIER}"
deploy_query_simulator = $DEPLOY_QUERY_SIMULATOR
deploy_initial_backfill_task = $DEPLOY_INITIAL_BACKFILL_TASK
developer_users = $TFVARS_DEVELOPER_USERS
llm_model_statsbuilder = "$LLM_MODEL_STATSBUILDER"
timedelta_considered_sec = $TIMEDELTA_CONSIDERED_SEC
EOF

if [ "$MAILERSEND_ENABLED" = "true" ]; then
cat >> "$TFVARS" <<EOF
# MailerSend variables (configured)
mailersend_api_token = "$MAILERSEND_API_TOKEN"
mail_report_frequency = "$MAIL_REPORT_FREQUENCY"
mailersend_domain = "$MAILERSEND_DOMAIN"
llm_model_mailnotifier = "$LLM_MODEL_MAILNOTIFIER"
min_stats_jobs_send_email = $MIN_STATS_JOBS_SEND_EMAIL
EOF
else
cat >> "$TFVARS" <<EOF
# MailerSend variables (not configured)
mailersend_api_token = ""
mail_report_frequency = "monthly"
mailersend_domain = ""
llm_model_mailnotifier = ""
min_stats_jobs_send_email = 10
EOF
fi


if [[ "$DEPLOY_QUERY_SIMULATOR" == "true" ]]; then
  echo "cloud_function_source_object_query_simulator = \"$GCS_OBJECT_NAME_QUERY_SIMULATOR\"" >> "$TFVARS"
else
  echo "cloud_function_source_object_query_simulator = null" >> "$TFVARS"
fi

if [[ "$DEPLOY_INITIAL_BACKFILL_TASK" == "true" ]]; then
  echo "initial_backfill_start_time = \"$BACKFILL_START_TIME\"" >> "$TFVARS"
  echo "initial_backfill_end_time = \"$BACKFILL_END_TIME\"" >> "$TFVARS"
else
  echo "initial_backfill_start_time = null" >> "$TFVARS"
  echo "initial_backfill_end_time = null" >> "$TFVARS"
fi

echo "✅ terraform.tfvars generated successfully."

echo "📁 Changing directory to $TF_DIR..."
cd "$TF_DIR" || { echo "❌ Failed to change directory to $TF_DIR"; exit 1; }

echo "✅ Running terraform init -upgrade..."
terraform init -upgrade || { echo "❌ Terraform initialization failed."; exit 1; }
echo "✅ Terraform init completed successfully."

PROJECT_EXISTS_GCP=false
echo "🔎 Checking if project '$PROJECT_ID' already exists in Google Cloud..."
if gcloud projects describe "$PROJECT_ID" &> /dev/null; then
  PROJECT_EXISTS_GCP=true
  echo "✅ Project '$PROJECT_ID' found in Google Cloud."
else
  echo "⚪ Project '$PROJECT_ID' not found in Google Cloud."
fi

PROJECT_IN_TFSTATE=false
echo "🔎 Checking if resource 'google_project.new_gcp_project' is tracked in Terraform state..."
if terraform state list 2>/dev/null | grep -q "^google_project.new_gcp_project$"; then
  PROJECT_IN_TFSTATE=true
  echo "✅ Resource 'google_project.new_gcp_project' found in Terraform state."
else
  echo "⚪ Resource 'google_project.new_gcp_project' not found in Terraform state."
fi

if $PROJECT_EXISTS_GCP && ! $PROJECT_IN_TFSTATE; then
  echo "⚠️ Project '$PROJECT_ID' exists in Google Cloud but is NOT tracked in Terraform state."
  echo "   Attempting to import the existing project into Terraform state automatically."
  echo ""

  IMPORT_COMMAND="terraform import google_project.new_gcp_project $PROJECT_ID"
  echo "🤖 Running: $IMPORT_COMMAND"

  if $IMPORT_COMMAND; then
    echo ""
    echo "✅ Automatic import successful!"
    echo "   The existing Google Cloud project '$PROJECT_ID' is now tracked by Terraform state."
    echo "   Proceeding with 'terraform apply' to manage the imported project and deploy/manage its resources."
  else
    echo ""
    echo "❌ Automatic import failed."
    echo ""
    echo "⚠️ Manual Intervention Required: Project '$PROJECT_ID' exists in Google Cloud but is NOT tracked in Terraform state."
    echo "   The automatic import attempt failed."
    echo "   To import manually, run the following command from the '$TF_DIR' directory:"
    echo ""
    echo "       $IMPORT_COMMAND"
    echo ""
    echo "   After successfully importing, run this script or 'terraform apply' again."
    exit 1
  fi

elif $PROJECT_IN_TFSTATE && !$PROJECT_EXISTS_GCP; then
  echo "❌ Found an unexpected and inconsistent state:"
  echo "   Terraform state indicates the project resource exists, but Google Cloud reports it does NOT exist."
  echo "   Your Terraform state is likely inconsistent with the real infrastructure."
  echo ""
  echo "   Manual Intervention Required: Reconcile your Terraform state with the actual infrastructure."
  echo "   Options:"
  echo "   - If the project was deleted intentionally: Remove the project resource from state using 'terraform state rm google_project.new_gcp_project'."
  echo "   - If the project should exist: Investigate why 'gcloud projects describe' fails or restore a correct state file."
  echo ""
  echo "   After reconciling, run this script or 'terraform apply' again."
  exit 1
fi

if $PROJECT_EXISTS_GCP && $PROJECT_IN_TFSTATE; then
    echo "🤝 Project '$PROJECT_ID' exists in Google Cloud and IS tracked in Terraform state."
    echo "   Proceeding with 'terraform apply' to manage the existing project and deploy/update its resources."
elif ! $PROJECT_EXISTS_GCP && ! $PROJECT_IN_TFSTATE; then
     echo "✨ Project '$PROJECT_ID' does NOT exist in Google Cloud, and IS NOT tracked in Terraform state."
     echo "   Proceeding with 'terraform apply' to create the new project and deploy all its resources."
fi

echo "🚀 Running Terraform apply..."
terraform apply -var-file="terraform.tfvars" -auto-approve -lock=false || { echo "❌ Terraform apply failed."; exit 1; }
echo "✅ Terraform apply completed successfully."

echo "📁 Returning to starting directory: $SCRIPT_START_DIR"
cd "$SCRIPT_START_DIR" || { echo "❌ Failed to return to starting directory!"; exit 1; }

echo "✅ Script finished successfully."