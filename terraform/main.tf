terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.41.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = google_project.new_gcp_project.project_id
  region  = var.gcp_region
}

provider "google" {
  alias = "creator"
}

variable "gcp_project_id" {
  description = "The desired unique identifier for the Google Cloud Project. This ID will be used as the project ID and must be globally unique across all Google Cloud projects."
  type          = string
}

variable "project_name_to_create" {
  description = "The human-readable display name for the Google Cloud Project. If set to null, the gcp_project_id will be used as the display name as well."
  type          = string
  default       = null
}

variable "billing_account_id" {
  description = "The alphanumeric ID of the billing account that the Google Cloud Project will be linked to. This is required for billing project usage."
  type          = string
}

variable "gcp_region" {
  description = "The Google Cloud region where regional resources (like BigQuery datasets, Cloud Functions, Cloud Tasks queues) will be deployed within the project."
  type          = string
}

variable "cloud_functions_source_bucket" {
  description = "The desired base name for the Google Cloud Storage bucket that will store the source code archives for the Cloud Functions. The project ID will be prepended to this name to ensure uniqueness."
  type          = string
}

variable "cloud_function_source_object_table_stats" {
  description = "The object path and filename within the Cloud Functions source bucket for the zipped source code of the table-stats-builder Cloud Function. E.g., 'table-stats-builder/source.zip'."
  type          = string
}

variable "cloud_function_source_object_query_simulator" {
  description = "The object path and filename within the Cloud Functions source bucket for the zipped source code of the query-simulator Cloud Function. Set to null if the query simulator function is not being deployed."
  type          = string
  nullable    = true
}

variable "cloud_function_source_object_mail_notifier" {
  description = "The object path and filename within the Cloud Functions source bucket for the zipped source code of the mail-notifier Cloud Function."
  type          = string
}

variable "developer_users" {
  description = "A list of email addresses for users who should be granted a custom 'Developers' IAM role and to whom aggregated reports are sent to. This role includes permissions like accessing Secret Manager secrets and potentially impersonating service accounts."
  type          = list(string)
  default       = []
}

variable "deploy_query_simulator" {
  description = "A boolean flag. Set to true to deploy the query simulator Cloud Function and configure a Cloud Task to trigger it exactly once after the initial deployment."
  type          = bool
  default       = false
}

variable "deploy_initial_backfill_task" {
  description = "A boolean flag. Set to true to configure and trigger a one-off Cloud Task to run the table stats builder Cloud Function for an initial historical backfill period."
  type          = bool
  default       = false
}

variable "initial_backfill_start_time" {
  description = "The start time (ISO 8601 string) for the initial backfill Cloud Task payload. Used only if deploy_initial_backfill_task is true. If null, the Cloud Function's default time window will be used."
  type          = string
  default       = null
}

variable "initial_backfill_end_time" {
  description = "The end time (ISO 8601 string) for the initial backfill Cloud Task payload. Used only if deploy_initial_backfill_task is true. If null, the Cloud Function's default time window will be used."
  type          = string
  default       = null
}

variable "llm_model_statsbuilder" {
  description = "The name or identifier of the Large Language Model (LLM) that the table-stats-builder Cloud Function should use for performing query analysis."
  type          = string
}

variable "llm_model_mailnotifier" {
  description = "The name or identifier of the Large Language Model (LLM) that the mail-notifier Cloud Function might use (e.g., for generating report summaries)."
  type          = string
}

variable "timedelta_considered_sec" {
  description = "The default duration, in seconds, of the time window for BigQuery job logs that the table-stats-builder Cloud Function will query and analyze during each scheduled run when no specific time range is provided in the request body. Also used to calculate the Cloud Scheduler frequency."
  type          = number
}

variable "mail_notifier_jobs_header" {
  description = "A fixed string comment expected at the beginning of BigQuery queries executed by the mail-notifier Cloud Function. This string is used to identify and exclude these specific jobs from the main job analysis process, ensuring that internal mail-notifier queries do not trigger unnecessary analysis or reporting."
  type          = string
  default       = "-- Jobs executed for mail notifier extractions"
}

variable "mailersend_api_token" {
  description = "The token used for authentication with the MailerSend service. This is required for sending emails from the Cloud Functions. Leave empty or null to skip deploying the mail notification service."
  type          = string
  sensitive     = true
  default       = ""
}

variable "mailersend_domain" {
  description = "The verified domain used for sending emails via MailerSend. This domain must be configured and verified in your MailerSend account."
  type          = string
  default       = ""
}

variable "mail_report_frequency" {
  description = "The frequency of the email reports. Options: 'daily', 'weekly', 'bi-weekly', 'monthly', 'quarterly', 'semesterly', 'yearly'. Used only if mailersend_api_token is provided."
  type          = string
  default       = "monthly"
  validation {
    condition     = contains(["daily", "weekly", "bi-weekly", "monthly", "quarterly", "semesterly", "yearly"], var.mail_report_frequency)
    error_message = "The mail_report_frequency must be one of: 'daily', 'weekly', 'bi-weekly', 'monthly', 'quarterly', 'semesterly', 'yearly' when mailersend_api_token is provided."
  }
}

variable "min_stats_jobs_send_email" {
  description = "The minimum number of jobs that must have been analyzed in a time window before an email report is sent. This is used to avoid sending empty or low-value reports."
  type          = number
  default       = 10
  validation {
    condition     = var.min_stats_jobs_send_email > 0
    error_message = "The min_stats_jobs_send_email must be greater than 0."
  }
}

locals {
  mail_frequency_cron_schedule = {
    "daily"      = "0 7 * * *"     # Every day at 7:00 AM UTC
    "weekly"     = "0 7 * * 1"     # Every Monday at 7:00 AM UTC
    "bi-weekly"  = "0 7 * * 1"     # Every Monday at 7:00 AM UTC (standard cron limitation for "every other")
    "monthly"    = "0 7 1 * *"     # On the 1st of every month at 7:00 AM UTC
    "quarterly"  = "0 7 1 */3 *"   # On the 1st of the month, every 3 months, at 7:00 AM UTC
    "semesterly" = "0 7 1 */6 *"   # On the 1st of the month, every 6 months, at 7:00 AM UTC
    "yearly"     = "0 7 1 1 *"     # On the 1st of January at 7:00 AM UTC
  }
}

data "archive_file" "cloud_function_source_zip_table_stats" {
  type        = "zip"
  source_dir  = "../src/cloud-functions/table-stats-builder"
  output_path = "../src/cloud-functions/table-stats-builder/source.zip"
}

data "archive_file" "cloud_function_source_zip_query_simulator" {
  count       = var.deploy_query_simulator ? 1 : 0
  type        = "zip"
  source_dir  = "../src/cloud-functions/query-simulator"
  output_path = "../src/cloud-functions/query-simulator/source.zip"
}

data "archive_file" "cloud_function_source_zip_mail_notifier" {
  count       = var.mailersend_api_token != "" ? 1 : 0
  type        = "zip"
  source_dir  = "../src/cloud-functions/mail-notifier"
  output_path = "../src/cloud-functions/mail-notifier/source.zip"
}

resource "random_string" "queue_name_suffix" {
  count       = (var.deploy_query_simulator || var.deploy_initial_backfill_task) ? 1 : 0
  length      = 8
  special     = false
  upper       = false
}

resource "google_project" "new_gcp_project" {
  provider      = google.creator
  project_id    = var.gcp_project_id
  name          = var.project_name_to_create != null ? var.project_name_to_create : var.gcp_project_id
  billing_account = var.billing_account_id
  deletion_policy = "ABANDON"
}

resource "google_project_service" "enabled_services" {
  project = google_project.new_gcp_project.project_id

  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "cloudapis.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudtasks.googleapis.com",
    "aiplatform.googleapis.com",
    "secretmanager.googleapis.com",
    "compute.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com"
  ])
  service = each.value

  disable_dependent_services = true
  disable_on_destroy         = false

  depends_on = [
    google_project.new_gcp_project,
  ]
}

resource "time_sleep" "wait_for_api_enablement" {
  depends_on = [google_project_service.enabled_services]
  create_duration = "60s"
}

data "google_compute_default_service_account" "default" {
  project = google_project.new_gcp_project.project_id
  depends_on = [google_project_service.enabled_services]
}

resource "google_project_iam_member" "default_compute_sa_roles" {
  for_each = toset([
    # "roles/editor",
    "roles/bigquery.jobUser",
    "roles/bigquery.dataViewer",
    "roles/bigquery.resourceViewer",
    "roles/bigquery.metadataViewer",
    "roles/bigquery.dataEditor",
    "roles/storage.objectViewer",
    "roles/storage.objectCreator",
    "roles/cloudfunctions.invoker",
    "roles/aiplatform.user"
  ])
  project = google_project.new_gcp_project.project_id
  role    = each.value
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

resource "google_project_iam_custom_role" "developers" {
  description = "Defines a custom IAM role for developers with specific permissions including access to Secret Manager secrets."
  role_id     = "Developers"
  title       = "Developers"
  project     = google_project.new_gcp_project.project_id
  permissions = [
    "secretmanager.versions.access",
    "secretmanager.secrets.get",
    "secretmanager.secrets.list",
  ]
}

resource "google_project_iam_binding" "developers_secret_manager_access" {
  project = google_project.new_gcp_project.project_id
  role    = google_project_iam_custom_role.developers.id
  members = concat(
    [for user_email in var.developer_users : "user:${user_email}"],
    ["serviceAccount:${data.google_compute_default_service_account.default.email}"]
  )
}

resource "google_secret_manager_secret" "mailersend_api_token_secret" {
  count     = var.mailersend_api_token != "" ? 1 : 0
  project   = google_project.new_gcp_project.project_id
  secret_id = "mailersend-api-token"

  replication {
    auto {}
  }

  depends_on = [
    google_project_service.enabled_services["secretmanager.googleapis.com"]
  ]
}

resource "google_secret_manager_secret_version" "mailersend_api_token_secret_version" {
  count     = var.mailersend_api_token != "" ? 1 : 0
  secret    = google_secret_manager_secret.mailersend_api_token_secret[0].id
  secret_data = var.mailersend_api_token

  depends_on = [
    google_secret_manager_secret.mailersend_api_token_secret
  ]
}

resource "google_secret_manager_secret_iam_member" "mailersend_secret_access" {
  count   = var.mailersend_api_token != "" ? 1 : 0
  secret_id = google_secret_manager_secret.mailersend_api_token_secret[0].id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"

  depends_on = [
    google_secret_manager_secret.mailersend_api_token_secret,
    google_secret_manager_secret_version.mailersend_api_token_secret_version,
    data.google_compute_default_service_account.default
  ]
}

resource "google_bigquery_dataset" "query_results_dataset" {
  description = "Creates the BigQuery dataset for storing query analysis results and related metadata tables."
  dataset_id = "stats_builder_results"
  project    = google_project.new_gcp_project.project_id
  location   = var.gcp_region
  depends_on = [
    google_project_service.enabled_services["bigquery.googleapis.com"]
  ]
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "query_results_table" {
  description = "This table stores the detailed results of automated SQL query analysis for jobs that were successfully analyzed. Each row represents a single analysis run for a specific query, capturing metrics, risk assessments, recommendations, and associated metadata like the agent and LLM version used. The table is partitioned by the analysis timestamp for efficient time-based querying and clustered by LLM version and risk level to optimize performance for filtering and aggregation on these dimensions."
  dataset_id          = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id            = "successful_analysis"
  project             = google_project.new_gcp_project.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "analysis_timestamp"
  }
  clustering = ["job_id", "user_email", "llm_version", "risk_level"]
  schema = jsonencode([
    {
      name        = "analysis_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The precise date and time (UTC) when the analysis of the SQL query was successfully completed. This field is used for time-based partitioning of the table."
    },
    {
      name        = "job_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "A globally unique identifier assigned to each individual query analysis job or run. This allows tracking the lineage of a specific analysis process."
    },
    {
      name        = "user_email"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The email address of the user who submitted the SQL query for analysis. This field is used for user-specific reporting and notifications."
    },
    {
      name        = "total_bytes_billed"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The total number of bytes billed for the user query. This metric provides insight into the resource consumption associated with the user's query activity."
    },
    {
      name        = "agent_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The name or identifier of the software agent, service, or user that initiated and performed this specific query analysis."
    },
    {
      name        = "llm_version"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Specifies the exact version or identifier of the Large Language Model (LLM) used by the agent to perform the analysis and generate scores and recommendations. This is a clustering field."
    },
    {
      name        = "query"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The full text of the SQL query that was submitted for analysis."
    },
    {
      name        = "statement_type"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Indicates the primary type of SQL statement analyzed (e.g., 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE TABLE', 'MERGE')."
    },
    {
      name        = "average_score"
      type        = "FLOAT"
      mode        = "NULLABLE"
      description = "A calculated average score across all applicable scoring categories for the analyzed query. Represents a general quality or risk indicator."
    },
    {
      name        = "score"
      type        = "RECORD"
      mode        = "NULLABLE"
      description = "A nested structure containing a breakdown of scores for the query across various predefined assessment dimensions."
      fields = [
        {
          name        = "security"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score indicating the potential security risks associated with the query (e.g., SQL injection vulnerabilities, data exposure). Higher scores represent lower risk."
        },
        {
          name        = "readability"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score assessing how easy the query is for a human to read and understand, considering formatting, naming conventions, etc. Higher scores indicate better readability."
        },
        {
          name        = "cost_efficiency"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score evaluating the query's potential cost in terms of resource consumption (e.g., bytes processed, slot usage). Higher scores indicate better cost efficiency."
        },
        {
          name        = "scalability"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score predicting how well the query will perform or how its cost will grow as data volume increases. Higher scores suggest better scalability."
        },
        {
          name        = "performance"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score estimating the execution speed and efficiency of the query on the target data platform. Higher scores indicate better performance."
        },
        {
          name        = "complexity"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score representing the inherent logical or structural complexity of the query. Note: This score might be inverse (higher means less complex) or direct (higher means more complex) depending on the analysis model."
        },
        {
          name        = "modularity"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score related to how well the query is structured or how easily parts of it could be reused or understood independently. Higher scores suggest better modularity."
        },
        {
          name        = "maintainability"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score combining aspects like readability, modularity, and complexity to assess how easy the query would be to modify or debug in the future. Higher scores indicate better maintainability."
        },
        {
          name        = "commenting"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score based on the presence, quality, and relevance of comments within the SQL query. Higher scores indicate better commenting practices."
        },
        {
          name        = "data_volume_efficiency"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score specifically evaluating how efficiently the query handles or reduces the volume of data it needs to process (e.g., using appropriate WHERE clauses, avoiding SELECT *). Higher scores are better."
        },
        {
          name        = "partition_pruning"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score indicating how effectively the query can utilize partition pruning to reduce the amount of data scanned in partitioned tables. Higher scores mean more effective pruning."
        },
        {
          name        = "sensitive_data_exposure"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score focused specifically on the potential for the query to access or expose sensitive data inappropriately. Higher scores indicate lower risk of exposure."
        },
        {
          name        = "null_handling"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score evaluating how correctly and robustly the query handles potential NULL values in its logic and calculations. Higher scores indicate better NULL handling."
        },
        {
          name        = "joins_quality"
          type        = "FLOAT"
          mode        = "NULLABLE"
          description = "A score assessing the appropriateness and efficiency of the JOIN clauses used in the query. Higher scores suggest well-optimized and correct joins."
        }
      ]
    },
    {
      name        = "recommendations"
      type        = "STRING"
      mode        = "REPEATED"
      description = "A list of specific, actionable recommendations or suggestions generated by the analysis to improve the query based on the scores and identified issues. Each element in the list is a separate recommendation string."
    },
    {
      name        = "risk_level"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "An overall categorical assessment of the query's risk level based on the analysis (e.g., 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'). This is a clustering field."
    },
    {
      name        = "is_safe_to_run"
      type        = "BOOLEAN"
      mode        = "NULLABLE"
      description = "A boolean flag indicating whether the analysis determined the query to be safe to execute in a production or target environment, based on predefined criteria and thresholds."
    },
    {
      name        = "score_confidence"
      type        = "FLOAT"
      mode        = "NULLABLE"
      description = "A metric representing the confidence level or certainty of the analysis system in the scores and assessments provided for this query. Ranges typically from 0 to 1."
    }
  ])
}

resource "google_bigquery_table" "query_results_view_clean" {
  description = "Creates a view on the successful analysis results table, selecting only the latest analysis record for each unique job identifier based on the analysis timestamp. This view helps in accessing the most recent assessment for any given query job."
  dataset_id  = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id    = "${google_bigquery_table.query_results_table.table_id}_clean"
  project     = google_project.new_gcp_project.project_id
  deletion_protection = false

  view {
    query = <<-EOT
      WITH MaxTimestamp AS (
          SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY analysis_timestamp DESC) AS rn
          FROM
              `${google_bigquery_table.query_results_table.project}.${google_bigquery_table.query_results_table.dataset_id}.${google_bigquery_table.query_results_table.table_id}`
      )

      SELECT *
      FROM MaxTimestamp
      WHERE rn = 1
    EOT
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_table.query_results_table
  ]
}

resource "google_bigquery_table" "query_failed_results_table" {
  description = "Stores records of SQL query analysis jobs that failed, including details about the failure source and reason. This table helps in monitoring and debugging issues with the analysis pipeline itself."
  dataset_id          = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id            = "failed_analysis"
  project             = google_project.new_gcp_project.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "analysis_timestamp"
  }
  clustering = ["job_id", "llm_version", "error_code", "failure_reason"]
  schema = jsonencode([
    {
      name        = "analysis_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The precise date and time (UTC) when the analysis of the SQL query was attempted but failed. This field is used for time-based partitioning of the table."
    },
    {
      name        = "job_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "A globally unique identifier assigned to each individual query analysis job or run. This allows tracking the lineage of a specific analysis process."
    },
    {
      name        = "user_email"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The email address of the user who submitted the SQL query for analysis. This field is used for user-specific reporting and notifications."
    },
    {
      name        = "total_bytes_billed"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The total number of bytes billed for the user query. This metric provides insight into the resource consumption associated with the user's query activity."
    },
    {
      name        = "agent_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The name or identifier of the software agent, service, or user that initiated the query analysis that subsequently failed."
    },
    {
      name        = "llm_version"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Specifies the exact version or identifier of the Large Language Model (LLM) that was intended to be used for the analysis before it failed."
    },
    {
      name        = "query"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "The full text of the SQL query that was submitted for analysis but resulted in a failure."
    },
    {
      name        = "statement_type"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Indicates the primary type of SQL statement that was being analyzed when the failure occurred (e.g., 'SELECT', 'INSERT')."
    },
    {
      name        = "error_code"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "A specific code representing the type of error or failure encountered during the analysis job. This code helps in categorizing and diagnosing issues, potentially derived from BigQuery job error reasons or internal analysis errors."
    },
    {
      name        = "failure_reason"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "A detailed description or the specific error message providing more context on why the analysis failed. This information is crucial for debugging and understanding the nature of the issue encountered during processing."
    }
  ])
}

resource "google_bigquery_table" "query_failed_results_view_clean" {
  description = "Creates a view on the failed analysis table, selecting only the latest failure record for each unique job identifier. Useful for easily accessing the most recent failure details for any given query job that failed analysis."
  dataset_id  = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id    = "${google_bigquery_table.query_failed_results_table.table_id}_clean"
  project     = google_project.new_gcp_project.project_id
  deletion_protection = false

  view {
    query = <<-EOT
      WITH MaxTimestamp AS (
          SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY analysis_timestamp DESC) AS rn
          FROM
              `${google_bigquery_table.query_failed_results_table.project}.${google_bigquery_table.query_failed_results_table.dataset_id}.${google_bigquery_table.query_failed_results_table.table_id}`
      )

      SELECT *
      FROM MaxTimestamp
      WHERE rn = 1
    EOT
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_table.query_failed_results_table
  ]
}

resource "google_bigquery_table" "runned_extractions_details_table" {
  description = "Stores summary records for each execution run of the BigQuery job analysis process. Each row represents the outcome for a specific time window, including the number of jobs fetched, filtered, successfully analyzed, and failures, along with the time window boundaries and run metadata."
  dataset_id          = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id            = "runned_extractions_details"
  project             = google_project.new_gcp_project.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "analysis_timestamp"
  }
  clustering = ["window_start_time", "window_end_time"]
  schema = jsonencode([
    {
      name        = "analysis_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The precise date and time (UTC) when the analysis process for this specific time window was successfully completed and recorded. This field is used for time-based partitioning of the table to improve query performance and manage data lifecycle."
    },
    {
      name        = "window_start_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The inclusive starting timestamp (UTC) of the data time window covered by this analysis run. This marks the beginning of the period for which data was processed from BigQuery INFORMATION_SCHEMA."
    },
    {
      name        = "window_end_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The exclusive ending timestamp (UTC) of the data time window covered by this analysis run. This marks the end of the period for which data was processed (the window includes data up to, but not including, this timestamp)."
    },
    {
      name        = "is_initial_backfill_run"
      type        = "BOOLEAN"
      mode        = "NULLABLE"
      description = "A flag indicating whether this analysis run was explicitly triggered as part of an initial backfill process to populate historical data, rather than a regular incremental run. TRUE if it was a backfill run, FALSE or NULL otherwise."
    },
    {
      name        = "total_analysis_jobs_count"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The total number of BigQuery jobs initially fetched from INFORMATION_SCHEMA within the specified time window, before any filtering was applied."
    },
    {
      name        = "successful_analysis_jobs_count"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The number of jobs from the filtered set for which the analysis agent successfully processed the job details and produced a structured analysis result."
    },
    {
      name        = "failed_analysis_jobs_count"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The number of jobs from the filtered set for which the analysis process failed to produce a valid structured output, leading to a failed analysis record being logged."
    }
  ])
}

resource "google_bigquery_table" "missing_windows" {
  description = "A view that calculates and returns the start and end timestamps for time windows that appear to be missing between consecutive processed windows recorded in the run details table. This is useful for identifying gaps in the historical analysis coverage."
  dataset_id  = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id    = "missing_windows"
  project     = google_project.new_gcp_project.project_id
  deletion_protection = false

  view {
    query = <<-EOF
      SELECT
          TIMESTAMP_ADD(t.window_end_time, INTERVAL 1 SECOND) AS missing_window_start_time,
          TIMESTAMP_SUB(t.next_window_start_time, INTERVAL 1 SECOND) AS missing_window_end_time
      FROM (
          SELECT
              window_start_time,
              window_end_time,
              LEAD(window_start_time) OVER (ORDER BY window_start_time) as next_window_start_time
          FROM
              `${google_bigquery_table.runned_extractions_details_table.project}.${google_bigquery_table.runned_extractions_details_table.dataset_id}.${google_bigquery_table.runned_extractions_details_table.table_id}`
          WHERE
              window_start_time IS NOT NULL AND window_end_time IS NOT NULL
      ) AS t
      WHERE
          t.next_window_start_time IS NOT NULL
          AND TIMESTAMP_DIFF(t.next_window_start_time, t.window_end_time, MICROSECOND) > 1000000
          AND TIMESTAMP_ADD(t.window_end_time, INTERVAL 1 SECOND) < TIMESTAMP_SUB(t.next_window_start_time, INTERVAL 1 SECOND)
    EOF
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_table.runned_extractions_details_table
  ]
}

resource "google_bigquery_table" "mail_notifier_suggestions_table" {
  description = "Stores summarized analysis results for specific users over defined time windows, intended for email notification. Each record represents a summary report for a user, including overall grade, key statistics, improvement areas, and relevant study resources."
  dataset_id          = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id            = "mail_notifier_suggestions"
  project             = google_project.new_gcp_project.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "analysis_timestamp"
  }
  clustering = ["user_email", "grade"]
  schema = jsonencode([
    {
      name        = "analysis_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The precise date and time (UTC) when this specific summary report was generated. This field is used for time-based partitioning."
    },
    {
      name        = "user_email"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "The email address of the user whose queries were analyzed and summarized in this report."
    },
    {
      name        = "window_start_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The inclusive starting timestamp (UTC) of the time window of user queries included in this summary report."
    },
    {
      name        = "window_end_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "The exclusive ending timestamp (UTC) of the time window of user queries included in this summary report."
    },
    {
      name        = "window_days"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The number of days in the time window covered by the single summary report. This is calculated as the difference in days between window_end_time and window_start_time."
    },
    {
      name        = "total_bytes_billed"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The total number of bytes billed for the user queries analyzed within the specified time window. This metric provides insight into the resource consumption associated with the user's query activity."
    },
    {
      name        = "analyzed_jobs_count"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "The total number of user queries analyzed within the specified time window for this report. Used as a tie-breaker for selecting the most granular analysis."
    },
    {
      name        = "grade"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "An overall grade or score assigned to the user's queries within the window, based on the analysis results. Higher grades indicate better query practices."
    },
    {
      name        = "stats_summary_extraction"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "A textual summary of key statistics and findings from the analysis of the user's queries in the window, potentially generated by an LLM."
    },
    {
      name        = "suggested_areas_of_improvement"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "A textual summary of suggested areas where the user could improve their query writing based on the analysis, potentially generated by an LLM."
    },
    {
      name        = "online_study_urls"
      type        = "STRING"
      mode        = "REPEATED"
      description = "A list of URLs pointing to online resources (documentation, tutorials, etc.) relevant to the suggested areas of improvement for the user."
    }
  ])
}

resource "google_bigquery_table" "mail_notifier_suggestions_view_clean" {
  description = "Creates a view on the mail_notifier_suggestions table, selecting only the latest and most granular analysis summary for each unique user email. 'Latest' is determined by the analysis_timestamp, and 'most granular' by the analyzed_jobs_count in case of timestamp ties."
  dataset_id  = google_bigquery_dataset.query_results_dataset.dataset_id
  table_id    = "${google_bigquery_table.mail_notifier_suggestions_table.table_id}_clean"
  project     = google_project.new_gcp_project.project_id
  deletion_protection = false

  view {
    query = <<-EOF
      WITH RankedSummaries AS (
          SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY user_email ORDER BY analysis_timestamp DESC, analyzed_jobs_count DESC) AS rn
          FROM
              `${google_bigquery_table.mail_notifier_suggestions_table.project}.${google_bigquery_table.mail_notifier_suggestions_table.dataset_id}.${google_bigquery_table.mail_notifier_suggestions_table.table_id}`
      )
      SELECT *
      FROM RankedSummaries
      WHERE rn = 1
    EOF
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_table.mail_notifier_suggestions_table
  ]
}

resource "google_storage_bucket" "cloud_function_source_bucket" {
  name          = "${var.gcp_project_id}-${var.cloud_functions_source_bucket}"
  location      = var.gcp_region
  storage_class = "STANDARD"
  project       = google_project.new_gcp_project.project_id

  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true
  force_destroy = true
}

resource "google_storage_bucket_object" "cloud_function_source_object_table_stats" {
  name    = var.cloud_function_source_object_table_stats
  bucket  = google_storage_bucket.cloud_function_source_bucket.name
  source  = data.archive_file.cloud_function_source_zip_table_stats.output_path
  depends_on = [data.archive_file.cloud_function_source_zip_table_stats]
}

resource "google_storage_bucket_object" "cloud_function_source_object_query_simulator" {
  count   = var.deploy_query_simulator ? 1 : 0
  name    = var.cloud_function_source_object_query_simulator
  bucket  = google_storage_bucket.cloud_function_source_bucket.name
  source  = data.archive_file.cloud_function_source_zip_query_simulator[0].output_path
  depends_on = [data.archive_file.cloud_function_source_zip_query_simulator]
}

resource "google_storage_bucket_object" "cloud_function_source_object_mail_notifier" {
  count   = var.mailersend_api_token != "" ? 1 : 0
  name    = var.cloud_function_source_object_mail_notifier
  bucket  = google_storage_bucket.cloud_function_source_bucket.name
  source  = data.archive_file.cloud_function_source_zip_mail_notifier[0].output_path
  depends_on = [data.archive_file.cloud_function_source_zip_mail_notifier]
}

resource "google_cloudfunctions2_function" "table_stats_builder_function" {
  description = "Deploys the table-stats-builder Cloud Function (Gen 2), responsible for fetching and analyzing BigQuery job logs."
  name        = "table-stats-builder-function"
  location    = var.gcp_region
  project     = google_project.new_gcp_project.project_id

  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function_source_bucket.name
        object = google_storage_bucket_object.cloud_function_source_object_table_stats.name
      }
    }
  }

  service_config {
    available_memory    = "512Mi"
    timeout_seconds     = 540
    service_account_email = data.google_compute_default_service_account.default.email
    max_instance_count  = 4
    environment_variables = {
      LOG_LEVEL = "INFO"
      PROJECT_ID = var.gcp_project_id
      REGION = var.gcp_region
      EXEC_SERVICE_ACCOUNT = data.google_compute_default_service_account.default.email
      MAILNOTIFIER_JOBS_HEADER = var.mail_notifier_jobs_header
      RESULTS_DATASET_ID = google_bigquery_dataset.query_results_dataset.dataset_id
      RESULTS_TABLE_ID = google_bigquery_table.query_results_table.table_id
      FAILED_RESULTS_TABLE_ID = google_bigquery_table.query_failed_results_table.table_id
      RUNNED_EXTRACTIONS_TABLE_ID = google_bigquery_table.runned_extractions_details_table.table_id
      AGENT_CONFIG_FILE = "agent_config.json"
      LLM_MODEL = var.llm_model_statsbuilder
      GOOGLE_GENAI_USE_VERTEXAI = "TRUE"
      TIMEDELTA_CONSIDERED_SEC = var.timedelta_considered_sec
    }
  }

  depends_on = [
    time_sleep.wait_for_api_enablement,
    google_project_service.enabled_services,
    data.google_compute_default_service_account.default,
    google_project_iam_member.default_compute_sa_roles,
    google_bigquery_table.query_results_table,
    google_bigquery_table.query_failed_results_table,
    google_bigquery_table.runned_extractions_details_table,
    google_storage_bucket_object.cloud_function_source_object_table_stats
  ]
}

resource "google_cloudfunctions2_function" "query_simulator_once_function" {
  description = "Deploys the query-simulator Cloud Function (Gen 2) intended to be executed once via Cloud Tasks to populate initial data."
  count       = var.deploy_query_simulator ? 1 : 0
  name        = "query-simulator-execute-once-function"
  location    = var.gcp_region
  project     = google_project.new_gcp_project.project_id

  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function_source_bucket.name
        object = google_storage_bucket_object.cloud_function_source_object_query_simulator[0].name
      }
    }
  }

  service_config {
    available_memory    = "512Mi"
    timeout_seconds     = 540
    service_account_email = data.google_compute_default_service_account.default.email
    max_instance_count = 1
    environment_variables = {
      LOG_LEVEL = "INFO"
      PROJECT_ID = var.gcp_project_id
      REGION = var.gcp_region
      COUNTER_BUCKET_NAME = google_storage_bucket.cloud_function_source_bucket.name
      COUNTER_FILE_NAME = "query_simulator_run_counter.txt"
      QUERIES_FOLDER = "queries"
    }
  }

  depends_on = [
    time_sleep.wait_for_api_enablement,
    google_project_service.enabled_services,
    data.google_compute_default_service_account.default,
    google_project_iam_member.default_compute_sa_roles,
    google_storage_bucket_object.cloud_function_source_object_query_simulator
  ]
}

resource "google_cloudfunctions2_function" "mail_notifier_function" {
  description = "Deploys the mail-notifier Cloud Function (Gen 2), responsible for sending email reports."
  count       = var.mailersend_api_token != "" ? 1 : 0
  name        = "mail-notifier-function"
  location    = var.gcp_region
  project     = google_project.new_gcp_project.project_id

  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function_source_bucket.name
        object = google_storage_bucket_object.cloud_function_source_object_mail_notifier[0].name
      }
    }
  }

  service_config {
    available_memory    = "2Gi"
    timeout_seconds     = 300  
    service_account_email = data.google_compute_default_service_account.default.email
    max_instance_count  = 2
    environment_variables = {
      LOG_LEVEL = "INFO"
      PROJECT_ID = var.gcp_project_id
      REGION = var.gcp_region
      MAILNOTIFIER_JOBS_HEADER = var.mail_notifier_jobs_header
      MAILERSEND_API_TOKEN_SECRET_PATH = google_secret_manager_secret.mailersend_api_token_secret[0].id
      MAILERSEND_DOMAIN = var.mailersend_domain
      MAIL_REPORT_FREQUENCY = var.mail_report_frequency
      AGENT_CONFIG_FILE = "agent_config.json"
      LLM_MODEL = var.llm_model_mailnotifier
      GOOGLE_GENAI_USE_VERTEXAI = "TRUE"
      MIN_STATS_JOBS_SEND_EMAIL = var.min_stats_jobs_send_email
      DEVELOPER_USERS_MAILLIST = join(",", [for user_email in var.developer_users : "user:${user_email}"])
      STATSTABLE_DATASET_ID = google_bigquery_table.query_results_view_clean.dataset_id
      STATSTABLE_TABLE_ID = google_bigquery_table.query_results_view_clean.table_id
      RESULTS_DATASET_ID = google_bigquery_table.mail_notifier_suggestions_table.dataset_id
      RESULTS_TABLE_ID = google_bigquery_table.mail_notifier_suggestions_table.table_id
    }
  }

  depends_on = [
    time_sleep.wait_for_api_enablement,
    google_project_service.enabled_services,
    google_cloud_scheduler_job.table_stats_builder_schedule,
    data.google_compute_default_service_account.default,
    google_project_iam_member.default_compute_sa_roles,
    google_storage_bucket_object.cloud_function_source_object_mail_notifier,
    google_secret_manager_secret.mailersend_api_token_secret,
    google_secret_manager_secret_iam_member.mailersend_secret_access
  ]
}

resource "google_cloud_scheduler_job" "table_stats_builder_schedule" {
  description = "Schedules the table-stats-builder Cloud Function to run periodically, typically every timedelta_considered_sec minutes, to process recent BigQuery job logs."
  name        = "table-stats-builder-schedule"
  schedule    = format("*/%d * * * *", var.timedelta_considered_sec / 60)
  time_zone   = "UTC"
  project     = google_project.new_gcp_project.project_id

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.table_stats_builder_function.service_config[0].uri
    oidc_token {
      service_account_email = data.google_compute_default_service_account.default.email
    }
  }

  attempt_deadline = "320s"

  depends_on = [
    google_cloudfunctions2_function.table_stats_builder_function,
    data.google_compute_default_service_account.default,
    google_project_service.enabled_services["cloudscheduler.googleapis.com"]
  ]
}

resource "google_cloud_scheduler_job" "mail_notifier_schedule" {
  description = "Schedules the mail-notifier Cloud Function to send email reports based on the configured frequency."
  count       = var.mailersend_api_token != "" ? 1 : 0
  name        = "mail-notifier-schedule-${var.mail_report_frequency}"
  schedule    = lookup(local.mail_frequency_cron_schedule, var.mail_report_frequency, "0 7 * * 1") # Default to weekly on Mondays at 7:00 AM UTC
  time_zone   = "UTC"
  project     = google_project.new_gcp_project.project_id
  region      = var.gcp_region

  http_target {
    uri = google_cloudfunctions2_function.mail_notifier_function[0].service_config[0].uri
    oidc_token {
      service_account_email = data.google_compute_default_service_account.default.email
    }
  }

  depends_on = [
    google_cloudfunctions2_function.mail_notifier_function,
    google_project_service.enabled_services["cloudscheduler.googleapis.com"]
  ]
}

resource "google_cloud_tasks_queue" "query_simulator_once_queue" {
  count    = var.deploy_query_simulator ? 1 : 0
  name     = "query-sim-once${(var.deploy_query_simulator || var.deploy_initial_backfill_task) ? "-${random_string.queue_name_suffix[0].result}" : ""}"
  location = var.gcp_region
  project  = google_project.new_gcp_project.project_id

  depends_on = [
    google_project_service.enabled_services["cloudtasks.googleapis.com"],
    random_string.queue_name_suffix
  ]
}

resource "google_cloud_tasks_queue" "table_stats_builder_once_queue" {
  count    = var.deploy_initial_backfill_task ? 1 : 0
  name     = "stats-backfill-once${(var.deploy_query_simulator || var.deploy_initial_backfill_task) ? "-${random_string.queue_name_suffix[0].result}" : ""}"
  location = var.gcp_region
  project  = google_project.new_gcp_project.project_id

  depends_on = [
    google_project_service.enabled_services["cloudtasks.googleapis.com"],
    random_string.queue_name_suffix
  ]
}

resource "null_resource" "trigger_query_simulator_once" {
  count = var.deploy_query_simulator ? 1 : 0

  triggers = {
    function_url = google_cloudfunctions2_function.query_simulator_once_function[0].service_config[0].uri
    queue_name   = google_cloud_tasks_queue.query_simulator_once_queue[0].name
    project_id   = google_project.new_gcp_project.project_id
    location     = var.gcp_region
    sa_email     = data.google_compute_default_service_account.default.email
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -euo pipefail
      if ! command -v gcloud &> /dev/null; then
          echo "gcloud command not found. Cannot trigger Cloud Task." >&2
          exit 1
      fi
      echo "Creating one-off Cloud Task for query simulator..."
      gcloud tasks create-http-task \
        --queue=${self.triggers.queue_name} \
        --location=${self.triggers.location} \
        --url="${self.triggers.function_url}" \
        --method=POST \
        --project=${self.triggers.project_id} \
        --oidc-service-account-email=${self.triggers.sa_email} \
        --header="Content-Type: application/json" \
        --body-content='{}' 
    EOT
    interpreter = ["bash", "-c"]
  }

  depends_on = [
    google_cloud_tasks_queue.query_simulator_once_queue,
    google_cloudfunctions2_function.query_simulator_once_function,
    google_project_iam_member.default_compute_sa_roles
  ]
}

resource "null_resource" "trigger_initial_backfill" {
  count = var.deploy_initial_backfill_task ? 1 : 0

  triggers = {
    function_url = google_cloudfunctions2_function.table_stats_builder_function.service_config[0].uri
    queue_name   = google_cloud_tasks_queue.table_stats_builder_once_queue[0].name
    project_id   = google_project.new_gcp_project.project_id
    location     = var.gcp_region
    sa_email     = data.google_compute_default_service_account.default.email
    start_time   = var.initial_backfill_start_time
    end_time     = var.initial_backfill_end_time
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -euo pipefail
      if ! command -v gcloud &> /dev/null; then
          echo "gcloud command not found. Cannot trigger Cloud Task." >&2
          exit 1
      fi
      if ! command -v jq &> /dev/null; then
          echo "jq command not found. Cannot build Cloud Task body. Please install jq." >&2
          exit 1
      fi

      TASK_BODY='{"is_initial_backfill": "TRUE"}'

      if [[ "${self.triggers.start_time}" != "<nil>" && -n "${self.triggers.start_time}" ]]; then
        TASK_BODY=$(echo "$TASK_BODY" | jq --arg start_time "${self.triggers.start_time}" '. + {"start_time": $start_time}')
      fi

      if [[ "${self.triggers.end_time}" != "<nil>" && -n "${self.triggers.end_time}" ]]; then
        TASK_BODY=$(echo "$TASK_BODY" | jq --arg end_time "${self.triggers.end_time}" '. + {"end_time": $end_time}')
      fi

      echo "Creating one-off Cloud Task for initial backfill..."
      gcloud tasks create-http-task \
        --queue=${self.triggers.queue_name} \
        --location=${self.triggers.location} \
        --url="${self.triggers.function_url}" \
        --method=POST \
        --project=${self.triggers.project_id} \
        --oidc-service-account-email=${self.triggers.sa_email} \
        --header="Content-Type: application/json" \
        --body-content="$TASK_BODY"
    EOT
    interpreter = ["bash", "-c"]
  }

  depends_on = [
    google_cloud_tasks_queue.table_stats_builder_once_queue,
    google_cloudfunctions2_function.table_stats_builder_function,
    google_project_iam_member.default_compute_sa_roles
  ]
}

output "new_project_id" {
  description = "The ID of the managed Google Cloud Project."
  value       = google_project.new_gcp_project.project_id
}