import os
import logging
import concurrent.futures
import threading
import re
import markdown
from typing import List, Dict, Any, Optional, Tuple
from google.cloud import secretmanager, bigquery
from datetime import datetime, timedelta, timezone
import numpy as np
from agent_builder import build_analysis_agent, GraphState
from graph_utils import format_bytes, generate_spider_graph, generate_trendlines_graph, get_image_base64
from mailersend import emails
from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser


# --- Environment Variables ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

GOOGLE_CLOUD_PROJECT = os.getenv("PROJECT_ID")
GOOGLE_CLOUD_LOCATION = os.getenv("REGION")
MAILNOTIFIER_JOBS_HEADER = os.getenv("MAILNOTIFIER_JOBS_HEADER")
MAILERSEND_API_TOKEN_SECRET_PATH = os.getenv("MAILERSEND_API_TOKEN_SECRET_PATH")
MAILERSEND_DOMAIN = os.getenv("MAILERSEND_DOMAIN")
MAIL_REPORT_FREQUENCY = os.getenv("MAIL_REPORT_FREQUENCY")
AGENT_CONFIG_FILE = os.getenv("AGENT_CONFIG_FILE")
LLM_MODEL = os.getenv("LLM_MODEL")
MIN_STATS_JOBS_SEND_EMAIL = int(os.getenv("MIN_STATS_JOBS_SEND_EMAIL"))
DEVELOPER_USERS_MAILLIST_STR = os.getenv("DEVELOPER_USERS_MAILLIST")
STATSTABLE_DATASET_ID = os.getenv("STATSTABLE_DATASET_ID")
STATSTABLE_TABLE_ID = os.getenv("STATSTABLE_TABLE_ID")
RESULTS_DATASET_ID = os.getenv("RESULTS_DATASET_ID")
RESULTS_TABLE_ID = os.getenv("RESULTS_TABLE_ID")

# --- Constants ---
BIGQUERY_COST_PER_TB_USD = 6.25
BYTES_IN_TB = 1024**4

SERVICE_ACCOUNT_REGEX = re.compile(r'.+@.+?\.gserviceaccount\.com$')

SEASONAL_COLORS = {
    "spring": {"primary": "#4CAF50", "secondary": "#8BC34A", "background": "#E8F5E9", "text": "#333333", "graph_line": "#388E3C", "graph_fill": "#C8E6C9"},
    "summer": {"primary": "#FFC107", "secondary": "#FF9800", "background": "#FFF8E1", "text": "#333333", "graph_line": "#FFA000", "graph_fill": "#FFECB3"},
    "autumn": {"primary": "#FF5722", "secondary": "#E64A19", "background": "#FBE9E7", "text": "#333333", "graph_line": "#F4511E", "graph_fill": "#FFCCBC"},
    "winter": {"primary": "#2196F3", "secondary": "#1976D2", "background": "#E3F2FD", "text": "#333333", "graph_line": "#1E88E5", "graph_fill": "#BBDEFB"},
}

# --- GCP Clients ---
local_gcp_clients = threading.local()


def get_bigquery_client():
    """
    Gets or initializes a ThreadLocal BigQuery client instance.

    Ensures that each thread processing email reports has its own BigQuery
    client instance to avoid potential thread-safety issues with the client.
    This is particularly important in a multi-threaded environment like
    using ThreadPoolExecutor in a Cloud Function.

    Returns:
        An initialized Google Cloud BigQuery client instance.

    Raises:
        Exception: If BigQuery client initialization fails due to
                   configuration issues or network problems.
    """
    if not hasattr(local_gcp_clients, "bigquery_client") or local_gcp_clients.bigquery_client is None:
        logging.info(f"Initializing BigQuery Client for thread {threading.current_thread().name}...")
        try:
            local_gcp_clients.bigquery_client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)
        except Exception as e:
             logging.error(f"Failed to initialize BigQuery client for thread {threading.current_thread().name}: {e}", exc_info=True)
             raise
    return local_gcp_clients.bigquery_client


def get_secret_client():
     """
     Gets or initializes a ThreadLocal Secret Manager client instance.

     Ensures that each thread has its own Secret Manager client for fetching
     secrets like API keys securely. This prevents potential conflicts when
     multiple threads attempt to use the same client instance concurrently.

     Returns:
         An initialized Google Cloud Secret Manager client instance.

     Raises:
         Exception: If Secret Manager client initialization fails due to
                    authentication issues or network problems.
     """
     if not hasattr(local_gcp_clients, "secret_client") or local_gcp_clients.secret_client is None:
         logging.info(f"Initializing Secret Manager Client for thread {threading.current_thread().name}...")
         try:
            local_gcp_clients.secret_client = secretmanager.SecretManagerServiceClient()
         except Exception as e:
             logging.error(f"Failed to initialize Secret Manager client for thread {threading.current_thread().name}: {e}", exc_info=True)
             raise
     return local_gcp_clients.secret_client


# --- Helper Functions ---
def is_service_account(email: str) -> bool:
    """
    Checks if an email address matches the pattern for a Google Cloud service account.

    Args:
        email: The email address string to check.

    Returns:
        True if the email appears to be a service account, False otherwise.
    """
    if not email or not isinstance(email, str):
        return False
    return bool(SERVICE_ACCOUNT_REGEX.match(email))


def get_secret_value(secret_resource_name: str) -> Optional[str]:
    """
    Fetches the latest version of a secret from Google Cloud Secret Manager.

    Uses a ThreadLocal Secret Manager client to access the secret value securely.
    The secret resource name should be in the format projects/*/secrets/*.
    This function is used to retrieve API keys like the MailerSend token.

    Args:
        secret_resource_name: The full resource name of the secret in Secret Manager
                              (e.g., projects/YOUR_PROJECT_NUMBER/secrets/YOUR_SECRET_ID).

    Returns:
        The secret value as a string, or None if the secret name is not provided
        or an error occurs during access (e.g., permission denied, secret not found).
    """
    client = get_secret_client()
    if not secret_resource_name:
        logging.warning("Secret resource name is not provided.")
        return None
    try:
        response = client.access_secret_version(request={"name": f"{secret_resource_name}/versions/latest"})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Error accessing secret {secret_resource_name}: {e}", exc_info=True)
        return None


def get_season(date: datetime) -> str:
    """
    Determines the current season based on the month (Northern Hemisphere).

    Used to select appropriate seasonal colors for the email report and graphs.
    This provides a simple way to add visual variation to the reports throughout the year.

    Args:
        date: A datetime object representing the current date.

    Returns:
        A string representing the current season ('spring', 'summer', 'autumn', 'winter').
    """
    month = date.month
    if month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"
    else:
        return "winter"


def calculate_report_window(frequency: str, end_time: datetime) -> Tuple[datetime, datetime]:
    """
    Calculates the start timestamp for the report based on frequency and end time.

    Determines the time window for which user query data should be aggregated
    and reported, based on the configured report frequency (daily, weekly, monthly, etc.).
    The end time is provided (typically the current time in UTC). The start time is calculated
    by subtracting the appropriate duration based on the frequency.

    Args:
        frequency: A string indicating the report frequency ('daily', 'weekly',
                   'bi-weekly', 'monthly', 'quarterly', 'semesterly', 'yearly').
        end_time: The end timestamp (UTC) of the report window.

    Returns:
        A tuple containing the start and end datetime objects for the report window
        in UTC.
    """
    if frequency == "daily":
        window_start = end_time - timedelta(days=1)
    elif frequency == "weekly":
        window_start = end_time - timedelta(weeks=1)
    elif frequency == "bi-weekly":
         window_start = end_time - timedelta(weeks=2)
    elif frequency == "monthly":
        first_day_of_current_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        window_start = last_day_of_previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif frequency == "quarterly":
         current_quarter_month_start = [1, 4, 7, 10][(end_time.month - 1) // 3]
         first_day_of_current_quarter = end_time.replace(month=current_quarter_month_start, day=1, hour=0, minute=0, second=0, microsecond=0)
         last_day_of_previous_quarter = first_day_of_current_quarter - timedelta(days=1)
         window_start = last_day_of_previous_quarter.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif frequency == "semesterly":
         current_semester_month_start = 1 if end_time.month <= 6 else 7
         first_day_of_current_semester = end_time.replace(month=current_semester_month_start, day=1, hour=0, minute=0, second=0, microsecond=0)
         last_day_of_previous_semester = first_day_of_current_semester - timedelta(days=1)
         window_start = last_day_of_previous_semester.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif frequency == "yearly":
        window_start = end_time.replace(year=end_time.year - 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        logging.warning(f"Unknown frequency '{frequency}'. Using daily.")
        window_start = end_time - timedelta(days=1)

    window_end = end_time

    return window_start, window_end


def calculate_analysis_window(report_window_start: datetime, report_window_end: datetime, frequency: str) -> Tuple[datetime, datetime]:
    """
    Calculates the time window for fetching data to be analyzed by the LLM.

    For monthly reports, this is the previous calendar month. For other frequencies,
    it is the same as the report window.

    Args:
        report_window_start: The start timestamp (UTC) of the report window.
        report_window_end: The end timestamp (UTC) of the report window.
        frequency: A string indicating the report frequency.

    Returns:
        A tuple containing the start and end datetime objects for the analysis window
        in UTC.
    """
    if frequency.lower() == "monthly":
        first_day_of_report_month = report_window_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_of_previous_month = first_day_of_report_month - timedelta(days=1)
        analysis_window_start = last_day_of_previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        analysis_window_end = first_day_of_report_month
    else:
        analysis_window_start = report_window_start
        analysis_window_end = report_window_end

    return analysis_window_start, analysis_window_end


def fetch_users_for_reporting(report_window_start: datetime, report_window_end: datetime) -> List[str]:
    """
    Fetches a list of distinct user emails who have job analysis data
    within the report window, excluding service accounts.

    Queries the `query_results_table` to find unique `user_email` values
    within the specified time frame and filters out emails matching the
    service account pattern. Uses a ThreadLocal BigQuery client.

    Args:
        report_window_start: The start timestamp (UTC) of the report window.
        report_window_end: The end timestamp (UTC) of the report window.

    Returns:
        A list of distinct user email strings. Returns an empty list if no
        eligible users are found or an error occurs.
    """
    client = get_bigquery_client()
    logging.info(f"Fetching distinct user emails between {report_window_start} and {report_window_end}, excluding service accounts.")

    query = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT DISTINCT user_email
    FROM `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE analysis_timestamp BETWEEN @report_window_start AND @report_window_end
    AND user_email IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_window_start", "TIMESTAMP", report_window_start),
            bigquery.ScalarQueryParameter("report_window_end", "TIMESTAMP", report_window_end),
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        rows = list(query_job.result())
        all_emails = [row['user_email'] for row in rows]

        human_users = [email for email in all_emails if not is_service_account(email)]

        logging.info(f"Found {len(all_emails)} distinct emails, {len(human_users)} are human users.")
        return human_users

    except Exception as e:
        logging.error(f"Error fetching distinct user emails: {e}", exc_info=True)
        return []


def fetch_user_job_analysis_data(user_email: str, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
    """
    Fetches detailed job analysis records for a specific user within a time window.

    Retrieves rows from the `query_results_table` (STATSTABLE_TABLE_ID) that
    correspond to the given user and fall within the specified time window.
    This data is used as input for the LLM agent to generate the summarized report.
    Uses a ThreadLocal BigQuery client for thread safety.

    Args:
        user_email: The email address of the user.
        window_start: The start timestamp (UTC) of the time window.
        window_end: The end timestamp (UTC) of the time window.

    Returns:
        A list of dictionaries, where each dictionary is a row from the
        `query_results_table` for the specified user and time window. Returns
        an empty list if no data is found or an error occurs during the query.
    """
    client = get_bigquery_client()
    logging.info(f"Fetching job analysis data for user: {user_email} between {window_start} and {window_end}")

    query = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT
        analysis_timestamp,
        job_id,
        user_email,
        total_bytes_billed,
        agent_name,
        llm_version,
        query,
        statement_type,
        average_score,
        score, -- Include the nested score structure
        recommendations,
        risk_level,
        is_safe_to_run,
        score_confidence
    FROM
        `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE
        user_email = @user_email
        AND analysis_timestamp BETWEEN @window_start AND @window_end
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
            bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        rows = list(query_job.result())
        job_analysis_data = [dict(row) for row in rows]
        logging.info(f"Fetched {len(job_analysis_data)} job analysis records for {user_email} in analysis window.")
        return job_analysis_data

    except Exception as e:
        logging.error(f"Error fetching user job analysis data for {user_email}: {e}", exc_info=True)
        return []


def fetch_historical_user_reports(user_email: str, end_time: datetime, historical_window_days: int = 365) -> List[Dict[str, Any]]:
    """
    Fetches historical summarized user reports for trendline generation and comparison.

    Retrieves rows from the `mail_notifier_suggestions_table` (RESULTS_TABLE_ID)
    for a specific user within a historical time window ending at `end_time`.
    This data is used to plot trends over time and calculate period-over-period
    changes in the email report. Uses a ThreadLocal BigQuery client.

    Args:
        user_email: The email address of the user.
        end_time: The end timestamp (UTC) of the historical window (typically the
                  end of the current report window).
        historical_window_days: The number of days to go back for historical data.

    Returns:
        A list of dictionaries, where each dictionary is a historical report
        summary for the user, sorted by `analysis_timestamp` ASC. Returns an
        empty list if no data is found or an error occurs during the query.
    """
    client = get_bigquery_client()
    historical_window_start = end_time - timedelta(days=historical_window_days)
    logging.info(f"Fetching historical reports for user: {user_email} between {historical_window_start} and {end_time}")

    query = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT
        user_email,
        analysis_timestamp,
        window_start_time,
        window_end_time,
        window_days,
        total_bytes_billed,
        analyzed_jobs_count,
        grade,
        stats_summary_extraction,
        suggested_areas_of_improvement,
        online_study_urls
    FROM
        `{GOOGLE_CLOUD_PROJECT}.{RESULTS_DATASET_ID}.{RESULTS_TABLE_ID}`
    WHERE
        user_email = @user_email
        AND analysis_timestamp BETWEEN @historical_window_start AND @end_time
    ORDER BY
        analysis_timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
            bigquery.ScalarQueryParameter("historical_window_start", "TIMESTAMP", historical_window_start),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        rows = list(query_job.result())
        historical_reports = [dict(row) for row in rows]
        logging.info(f"Fetched {len(historical_reports)} historical reports for {user_email}.")
        return historical_reports

    except Exception as e:
        logging.error(f"Error fetching historical user reports for {user_email}: {e}", exc_info=True)
        return []


def aggregate_job_data(job_analysis_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregates detailed job analysis data for a user over a time window.

    Calculates summary statistics like total bytes billed, total jobs analyzed,
    the average grade across all jobs, and average scores per dimension (for spider graph)
    in the provided list of job analysis records. The overall grade is calculated
    as the average of the 'average_score' field from each job, scaled to 0-100.

    Args:
        job_analysis_data: A list of dictionaries, where each dictionary is a
                           job analysis record from the `query_results_table`.

    Returns:
        A dictionary containing aggregated metrics: 'total_bytes_billed',
        'analyzed_jobs_count', 'grade', and 'average_spider_scores'.
    """
    total_bytes_billed = 0
    analyzed_jobs_count = 0
    average_scores = []
    spider_scores_sum: Dict[str, float] = {}
    spider_scores_count: Dict[str, int] = {}

    for job in job_analysis_data:
        analyzed_jobs_count += 1
        if job.get('total_bytes_billed') is not None:
            total_bytes_billed += job['total_bytes_billed']

        if job.get('average_score') is not None:
            average_scores.append(job['average_score'])

        if isinstance(job.get('score'), dict):
            for dimension, score in job['score'].items():
                if isinstance(score, (int, float)):
                    spider_scores_sum[dimension] = spider_scores_sum.get(dimension, 0.0) + score
                    spider_scores_count[dimension] = spider_scores_count.get(dimension, 0) + 1
                else:
                    logging.debug(f"Skipping non-numeric score for dimension '{dimension}': {score}")


    overall_grade = int(np.mean(average_scores) * 100) if average_scores else None

    average_spider_scores = {
        dimension: spider_scores_sum[dimension] / spider_scores_count[dimension]
        for dimension in spider_scores_sum if spider_scores_count[dimension] > 0 
    }

    return {
        "total_bytes_billed": total_bytes_billed,
        "analyzed_jobs_count": analyzed_jobs_count,
        "grade": overall_grade,
        "average_spider_scores": average_spider_scores
    }


def aggregate_service_account_activity(report_window_start: datetime, report_window_end: datetime) -> Dict[str, Any]:
    """
    Aggregates activity statistics across all service account analyzed jobs
    within a time window.

    Queries the `query_results_table` to calculate total jobs analyzed, total bytes
    billed, average score, and counts of high/critical risk jobs for all
    service accounts combined. This data is used for the developer report.
    Uses a ThreadLocal BigQuery client.

    Args:
        report_window_start: The start timestamp (UTC) of the report window.
        report_window_end: The end timestamp (UTC) of the report window.

    Returns:
        A dictionary containing aggregated stats for all service accounts.
        Returns a dictionary with zero/None values if no data is found or an
        error occurs.
    """
    client = get_bigquery_client()
    logging.info(f"Fetching and aggregating service account activity stats between {report_window_start} and {report_window_end}.")

    query_service_accounts = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT DISTINCT user_email
    FROM `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE analysis_timestamp BETWEEN @report_window_start AND @report_window_end
    AND user_email IS NOT NULL
    """
    job_config_sa = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_window_start", "TIMESTAMP", report_window_start),
            bigquery.ScalarQueryParameter("report_window_end", "TIMESTAMP", report_window_end),
        ]
    )

    service_account_emails = []
    try:
        query_job_sa = client.query(query_service_accounts, job_config=job_config_sa)
        rows_sa = list(query_job_sa.result())
        all_emails_in_window = [row['user_email'] for row in rows_sa]
        service_account_emails = [email for email in all_emails_in_window if is_service_account(email)]
        logging.info(f"Found {len(service_account_emails)} distinct service account emails with activity.")
    except Exception as e:
        logging.error(f"Error fetching distinct service account emails: {e}", exc_info=True)


    if not service_account_emails:
        logging.info("No service account emails found in the window. Returning zero stats.")
        return {
            "total_jobs": 0,
            "total_bytes_billed": 0,
            "average_score": None,
            "high_risk_jobs": 0,
            "critical_risk_jobs": 0
        }

    service_account_list_str = ", ".join([f"'{email}'" for email in service_account_emails])

    query_aggregate = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT
        COUNT(job_id) as total_jobs,
        SUM(total_bytes_billed) as total_bytes_billed,
        AVG(average_score) as average_score,
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_risk_jobs,
        SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) as critical_risk_jobs
    FROM
        `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE
        analysis_timestamp BETWEEN @report_window_start AND @report_window_end
        AND user_email IN ({service_account_list_str})
    """
    job_config_agg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_window_start", "TIMESTAMP", report_window_start),
            bigquery.ScalarQueryParameter("report_window_end", "TIMESTAMP", report_window_end),
        ]
    )

    try:
        query_job_agg = client.query(query_aggregate, job_config=job_config_agg)
        row = list(query_job_agg.result())
        if row:
            stats = dict(row[0])
            stats['total_jobs'] = int(stats['total_jobs']) if stats['total_jobs'] is not None else 0
            stats['total_bytes_billed'] = int(stats['total_bytes_billed']) if stats['total_bytes_billed'] is not None else 0
            stats['average_score'] = float(stats['average_score']) if stats['average_score'] is not None else None
            stats['high_risk_jobs'] = int(stats['high_risk_jobs']) if stats['high_risk_jobs'] is not None else 0
            stats['critical_risk_jobs'] = int(stats['critical_risk_jobs']) if stats['critical_risk_jobs'] is not None else 0
            logging.info(f"Aggregated service account stats: {stats}")
            return stats
        else:
            logging.warning("Aggregate query for service accounts returned no rows.")
            return {
                "total_jobs": 0,
                "total_bytes_billed": 0,
                "average_score": None,
                "high_risk_jobs": 0,
                "critical_risk_jobs": 0
            }

    except Exception as e:
        logging.error(f"Error aggregating service account activity stats: {e}", exc_info=True)
        return {
            "total_jobs": 0,
            "total_bytes_billed": 0,
            "average_score": None,
            "high_risk_jobs": 0,
            "critical_risk_jobs": 0
        }


def aggregate_human_user_activity_stats(report_window_start: datetime, report_window_end: datetime) -> Dict[str, Any]:
    """
    Aggregates activity statistics across all human user analyzed jobs
    within a time window.

    Queries the `query_results_table` to calculate total jobs analyzed, total bytes
    billed, average score, and counts of high/critical risk jobs for all
    human users combined. This data is used for the developer report to show
    overall human user activity. Uses a ThreadLocal BigQuery client.

    Args:
        report_window_start: The start timestamp (UTC) of the report window.
        report_window_end: The end timestamp (UTC) of the report window.

    Returns:
        A dictionary containing aggregated stats for all human users.
        Returns a dictionary with zero/None values if no data is found or an
        error occurs.
    """
    client = get_bigquery_client()
    logging.info(f"Fetching and aggregating human user activity stats between {report_window_start} and {report_window_end}.")

    query_human_users = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT DISTINCT user_email
    FROM `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE analysis_timestamp BETWEEN @report_window_start AND @report_window_end
    AND user_email IS NOT NULL
    """
    job_config_hu = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_window_start", "TIMESTAMP", report_window_start),
            bigquery.ScalarQueryParameter("report_window_end", "TIMESTAMP", report_window_end),
        ]
    )

    human_user_emails = []
    try:
        query_job_hu = client.query(query_human_users, job_config=job_config_hu)
        rows_hu = list(query_job_hu.result())
        all_emails_in_window = [row['user_email'] for row in rows_hu]
        human_user_emails = [email for email in all_emails_in_window if not is_service_account(email)]
        logging.info(f"Found {len(human_user_emails)} distinct human user emails with activity.")
    except Exception as e:
        logging.error(f"Error fetching distinct human user emails: {e}", exc_info=True)


    if not human_user_emails:
        logging.info("No human user emails found in the window. Returning zero stats.")
        return {
            "total_jobs": 0,
            "total_bytes_billed": 0,
            "average_score": None,
            "high_risk_jobs": 0,
            "critical_risk_jobs": 0
        }

    human_user_list_str = ", ".join([f"'{email}'" for email in human_user_emails])

    query_aggregate = f"""
    {MAILNOTIFIER_JOBS_HEADER}
    SELECT
        COUNT(job_id) as total_jobs,
        SUM(total_bytes_billed) as total_bytes_billed,
        AVG(average_score) as average_score,
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_risk_jobs,
        SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) as critical_risk_jobs
    FROM
        `{GOOGLE_CLOUD_PROJECT}.{STATSTABLE_DATASET_ID}.{STATSTABLE_TABLE_ID}`
    WHERE
        analysis_timestamp BETWEEN @report_window_start AND @report_window_end
        AND user_email IN ({human_user_list_str})
    """
    job_config_agg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_window_start", "TIMESTAMP", report_window_start),
            bigquery.ScalarQueryParameter("report_window_end", "TIMESTAMP", report_window_end),
        ]
    )

    try:
        query_job_agg = client.query(query_aggregate, job_config=job_config_agg)
        row = list(query_job_agg.result())
        if row:
            stats = dict(row[0])
            stats['total_jobs'] = int(stats['total_jobs']) if stats['total_jobs'] is not None else 0
            stats['total_bytes_billed'] = int(stats['total_bytes_billed']) if stats['total_bytes_billed'] is not None else 0
            stats['average_score'] = float(stats['average_score']) if stats['average_score'] is not None else None
            stats['high_risk_jobs'] = int(stats['high_risk_jobs']) if stats['high_risk_jobs'] is not None else 0
            stats['critical_risk_jobs'] = int(stats['critical_risk_jobs']) if stats['critical_risk_jobs'] is not None else 0
            logging.info(f"Aggregated human user stats: {stats}")
            return stats
        else:
            logging.warning("Aggregate query for human users returned no rows.")
            return {
                "total_jobs": 0,
                "total_bytes_billed": 0,
                "average_score": None,
                "high_risk_jobs": 0,
                "critical_risk_jobs": 0
            }

    except Exception as e:
        logging.error(f"Error aggregating human user activity stats: {e}", exc_info=True)
        return {
            "total_jobs": 0,
            "total_bytes_billed": 0,
            "average_score": None,
            "high_risk_jobs": 0,
            "critical_risk_jobs": 0
        }


def build_user_email_html(user_data: Dict[str, Any], historical_data: List[Dict[str, Any]], season_colors: Dict[str, str], mail_report_frequency: str, project_id: str, spider_graph_base64: Optional[str], trendlines_graph_base64: Optional[str]) -> str:
    """
    Builds the HTML content for a user-specific report email.

    This function takes the summarized report data for the current period,
    historical data for comparisons and trendlines, seasonal color scheme,
    report frequency, project ID, and Base64 encoded graph images (spider
    and trendlines) and formats them into a single HTML string for the email body.
    It includes the overall stats summary, period-over-period comparisons,
    embedded graphs, brief summary from the LLM, areas for improvement,
    relevant online resources, and a disclaimer. It also parses Markdown
    content within the summary and suggested areas fields into HTML.

    Args:
        user_data: A dictionary containing the summarized report data for the
                   current time window (from mail_notifier_suggestions_table schema).
        historical_data: A list of dictionaries containing historical report data
                         for trendline generation (from mail_notifier_suggestions_table),
                         sorted by analysis_timestamp ASC. This is used to calculate
                         period-over-period changes and provide data for the trendlines graph.
        season_colors: A dictionary containing color codes for email elements,
                       based on the current season, ensuring a visually consistent look.
        mail_report_frequency: A string indicating the frequency of this report (e.g., 'monthly').
                               Used in the email subject and header.
        project_id: The Google Cloud project ID. Used in the email header.
        spider_graph_base64: An optional Base64 encoded string of the spider graph image,
                             prefixed with the data URI scheme (e.g., "data:image/png;base64,...").
                             This image is embedded directly into the HTML.
        trendlines_graph_base64: An optional Base64 encoded string of the trendlines
                                 graph image, prefixed with the data URI scheme.
                                 This image is embedded directly into the HTML.

    Returns:
        A string containing the complete HTML content for the user email. Returns
        a simple message if the primary user_data is missing.
    """
    if not user_data:
        logging.error("No user data provided to build email HTML.")
        return "<p>No data available for this user in the specified period.</p>"

    user_email = user_data.get('user_email', 'User')
    window_start_time = user_data.get('window_start_time')
    window_end_time = user_data.get('window_end_time')
    overall_grade = user_data.get('grade', 'N/A')
    total_bytes_billed = user_data.get('total_bytes_billed', 0)
    analyzed_jobs_count = user_data.get('analyzed_jobs_count', 0)
    brief_summary_md = user_data.get('stats_summary_extraction', 'No summary available.')
    areas_of_improvement_md = user_data.get('suggested_areas_of_improvement', 'No specific suggestions at this time.')
    online_study_urls = user_data.get('online_study_urls', [])

    brief_summary_html = markdown.markdown(brief_summary_md or '')
    areas_of_improvement_html = markdown.markdown(areas_of_improvement_md or '')

    window_start_str = window_start_time.strftime('%Y-%m-%d') if isinstance(window_start_time, datetime) else 'N/A'
    window_end_str = window_end_time.strftime('%Y-%m-%d') if isinstance(window_end_time, datetime) else 'N/A'
    formatted_bytes_billed = format_bytes(total_bytes_billed)

    grade_change = "N/A"
    bytes_change = "N/A"
    jobs_change = "N/A"

    prev_period_report = None
    current_report_analysis_timestamp = user_data.get('analysis_timestamp')
    if isinstance(current_report_analysis_timestamp, datetime):
        previous_reports = [
            report for report in historical_data
            if isinstance(report.get('analysis_timestamp'), datetime) and report['analysis_timestamp'] < current_report_analysis_timestamp
        ]
        previous_reports.sort(key=lambda x: x['analysis_timestamp'], reverse=True)

        if previous_reports:
            prev_period_report = previous_reports[0]


    if prev_period_report:
         prev_grade = prev_period_report.get('grade')
         prev_bytes = prev_period_report.get('total_bytes_billed', 0)
         prev_jobs = prev_period_report.get('analyzed_jobs_count', 0)

         if isinstance(overall_grade, (int, float)) and isinstance(prev_grade, (int, float)):
              if prev_grade != 0:
                  change_percent = ((overall_grade - prev_grade) / prev_grade) * 100
                  color = 'green' if change_percent >= 0 else 'red'
                  arrow = '&#9650;' if change_percent >= 0 else '&#9660;'
                  grade_change = f"{change_percent:.1f}% <span style='color: {color}; font-weight: bold;'>{arrow}</span>"
              else:
                  color = 'green' if isinstance(overall_grade, (int, float)) and overall_grade > 0 else 'red' if isinstance(overall_grade, (int, float)) and overall_grade < 0 else '#555'
                  arrow = '&#9650;' if isinstance(overall_grade, (int, float)) and overall_grade > 0 else '&#9660;' if isinstance(overall_grade, (int, float)) and overall_grade < 0 else ''
                  grade_change = f"{overall_grade} (Prev Grade 0) <span style='color: {color}; font-weight: bold;'>{arrow}</span>" if isinstance(overall_grade, (int, float)) else "N/A"
         elif isinstance(overall_grade, (int, float)) and prev_grade is None:
             grade_change = f"{overall_grade} (No Previous Data)"


         if isinstance(total_bytes_billed, (int, float)) and isinstance(prev_bytes, (int, float)):
              if prev_bytes != 0:
                  change_percent = ((total_bytes_billed - prev_bytes) / prev_bytes) * 100
                  color = 'red' if change_percent >= 0 else 'green'
                  arrow = '&#9650;' if change_percent >= 0 else '&#9660;'
                  bytes_change = f"{change_percent:.1f}% <span style='color: {color}; font-weight: bold;'>{arrow}</span>"
              else:
                   color = 'red' if isinstance(total_bytes_billed, (int, float)) and total_bytes_billed > 0 else 'green' if isinstance(total_bytes_billed, (int, float)) and total_bytes_billed < 0 else '#555'
                   arrow = '&#9650;' if isinstance(total_bytes_billed, (int, float)) and total_bytes_billed > 0 else '&#9660;' if isinstance(total_bytes_billed, (int, float)) and total_bytes_billed < 0 else ''
                   bytes_change = f"{format_bytes(total_bytes_billed)} (Prev Bytes 0) <span style='color: {color}; font-weight: bold;'>{arrow}</span>" if isinstance(total_bytes_billed, (int, float)) else "N/A"
         elif isinstance(total_bytes_billed, (int, float)) and prev_bytes is None:
              bytes_change = f"{format_bytes(total_bytes_billed)} (No Previous Data)"


         if isinstance(analyzed_jobs_count, int) and isinstance(prev_jobs, int):
              if prev_jobs != 0:
                  change_percent = ((analyzed_jobs_count - prev_jobs) / prev_jobs) * 100
                  color = 'red' if change_percent >= 0 else 'green'
                  arrow = '&#9650;' if change_percent >= 0 else '&#9660;'
                  jobs_change = f"{change_percent:.1f}% <span style='color: {color}; font-weight: bold;'>{arrow}</span>"
              else:
                  color = 'red' if isinstance(analyzed_jobs_count, int) and analyzed_jobs_count > 0 else 'green' if isinstance(analyzed_jobs_count, int) and analyzed_jobs_count < 0 else '#555'
                  arrow = '&#9650;' if isinstance(analyzed_jobs_count, int) and analyzed_jobs_count > 0 else '&#9660;' if isinstance(analyzed_jobs_count, int) and analyzed_jobs_count < 0 else ''
                  jobs_change = f"{analyzed_jobs_count} (Prev Jobs 0) <span style='color: {color}; font-weight: bold;'>{arrow}</span>" if isinstance(analyzed_jobs_count, int) else "N/A"
         elif isinstance(analyzed_jobs_count, int) and prev_jobs is None:
              jobs_change = f"{analyzed_jobs_count} (No Previous Data)" 


    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Your BigQuery Report</title>
        <style>
            body {{ font-family: sans-serif; line-height: 1.6; color: {season_colors['text']}; background-color: {season_colors['background']}; margin: 0; padding: 20px; }}
            .container {{ max-width: 700px; margin: 0 auto; background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border: 1px solid #dddddd; }}
            h1, h2, h3 {{ color: {season_colors['primary']}; }}
            .header {{ background-color: {season_colors['primary']}; color: #ffffff; padding: 15px; text-align: center; border-top-left-radius: 8px; border-top-right-radius: 8px; }}
            .section {{ margin-bottom: 25px; padding-bottom: 15px; border-bottom: 1px solid #eeeeee; }}
            .section:last-child {{ border-bottom: none; }}
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-top: 15px;
            }}
            .stat-card {{
                background-color: #f9f9f9;
                padding: 15px;
                border-radius: 4px;
                text-align: center;
                border: 1px solid #eeeeee;
            }}
            .stat-card strong {{
                display: block;
                font-size: 1.3em;
                color: {season_colors['secondary']};
                margin-bottom: 5px;
            }}
             .stat-card span {{
                font-size: 0.9em;
                color: #555555;
             }}
             .stat-card .change {{
                 font-weight: bold;
             }}
            .grade-box {{
                background-color: #e8f5e9;
                color: #2e7d32;
                padding: 15px;
                border-radius: 4px;
                text-align: center;
                font-size: 1.5em;
                font-weight: bold;
                margin-top: 10px;
                 border: 1px solid #c8e6c9;
            }}
             .improvement-list ul {{ list-style: none; padding: 0; }}
             .improvement-list li {{ margin-bottom: 10px; padding-left: 25px; position: relative; }}
             .improvement-list li:before {{ content: '\\2022'; color: {season_colors['secondary']}; font-weight: bold; display: inline-block; width: 1em; margin-left: -1em; }}

            .resource-list ul {{ list-style: none; padding: 0; }}
            .resource-list li {{ margin-bottom: 8px; }}
            .resource-list li a {{ color: {season_colors['primary']}; text-decoration: none; }}
            .resource-list li a:hover {{ text-decoration: underline; }}

            .infographic {{
                text-align: center;
                margin-top: 20px;
                margin-bottom: 20px;
            }}
            .infographic img {{ max-width: 100%; height: auto; border: 1px solid #eeeeee; border-radius: 4px; }}

            /* Styles for rendered Markdown */
            .summary-content, .improvement-content {{
                /* Add some basic styling for rendered markdown like lists */
                margin-top: 10px;
            }}
             .summary-content ul, .improvement-content ul {{
                 padding-left: 20px; /* Standard list indentation */
                 margin-top: 0;
                 margin-bottom: 10px;
             }}
             .summary-content li, .improvement-content li {{
                 margin-bottom: 5px; /* Space between list items */
             }}
             .summary-content p:last-child, .improvement-content p:last-child {{
                 margin-bottom: 0; /* Remove bottom margin for last paragraph */
             }}


            .disclaimer {{ margin-top: 30px; padding: 15px; background-color: #ffebee; color: #c62828; border-left: 4px solid #ef5350; font-size: 0.9em; }}
            .footer {{ margin-top: 30px; text-align: center; font-size: 0.9em; color: #aaaaaa; }}

            @media only screen and (max-width: 600px) {{
                body {{ padding: 10px; }}
                .container {{ padding: 15px; }}
                .stats-grid {{ grid-template-columns: 1fr; }}
                 .stat-card {{ margin: 5px 0; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>Hello {user_email.split('@')[0]},</h2>
                <p>This is your {mail_report_frequency.capitalize()} updates for the queries performed over the project &lt;{project_id}&gt;.</p>
            </div>

            <div class="section">
                <h3>Overall Stats Summary ({window_start_str} to {window_end_str})</h3>

                <div class="stats-grid">
                    <div class="stat-card">
                        <strong>{overall_grade if overall_grade is not None else 'N/A'}</strong>
                        <span>Average grade of this period</span>
                    </div>
                     <div class="stat-card">
                        <strong class="change">{grade_change}</strong>
                        <span>% change in Grade vs previous period</span>
                    </div>
                     <div class="stat-card">
                        <strong>{formatted_bytes_billed}</strong>
                        <span>Total Bytes Billed this period</span>
                    </div>
                    <div class="stat-card">
                        <strong class="change">{bytes_change}</strong>
                        <span>% change in Bytes Billed vs previous period</span>
                    </div>
                     <div class="stat-card">
                        <strong>{analyzed_jobs_count if analyzed_jobs_count is not None else 'N/A'}</strong>
                        <span># jobs executed this period</span>
                    </div>
                    <div class="stat-card">
                         <strong class="change">{jobs_change}</strong>
                         <span>% change in Jobs vs previous period</span>
                    </div>
                </div>
            </div>

            <div class="section infographic">
                <h3>Performance Overview (This Period)</h3>
                {f'<img src="{spider_graph_base64}" alt="Spider Graph of this period performance scores">' if spider_graph_base64 else '<p>Spider graph data not available.</p>'}
                <p style="font-size:0.9em; color:#555;">Spider Graph showing average performance scores across different dimensions for this report period.</p>
            </div>

            <div class="section infographic">
                <h3>Grade and Cost Per Job Trendlines (Historical)</h3>
                 {f'<img src="{trendlines_graph_base64}" alt="Grade and costs/jobs trendlines over time">' if trendlines_graph_base64 else '<p>Trendlines graph data not available.</p>'}
                <p style="font-size:0.9em; color:#555;">Trendlines showing your historical grade and cost per job metrics.</p>
            </div>


            <div class="section">
                <h3>Brief summary</h3>
                {brief_summary_html}
            </div>

            <div class="section">
                <h3>Area of improvement</h3>
                {areas_of_improvement_html}
            </div>

             <div class="section resource-list">
                <h3>Relevant resources found</h3>
                {'<ul>' + ''.join(f'<li><a href="{url}" target="_blank">{url}</a></li>' for url in online_study_urls) + '</ul>' if online_study_urls else '<p>No specific online resources found at this time.</p>'}
            </div>

            <div class="disclaimer">
                <strong>Disclaimer:</strong>
                Notice that all those stats are automatically performed by AI agents, all your information are kept private and useful only for you.
            </div>
            <div class="footer">
                This report is generated based on your BigQuery job analysis data.
            </div>
        </div>
    </body>
    </html>
    """
    return html_content


def build_developer_email_html(human_user_stats: Dict[str, Any], service_account_stats: Dict[str, Any], season_colors: Dict[str, str], mail_report_frequency: str, project_id: str, report_window_start: datetime, report_window_end: datetime) -> str:
    """
    Builds the HTML content for the developer report email using a card layout.

    Summarizes the activity and performance of the analysis across human users
    and aggregated service account activity within the reporting window for the
    development team. This provides insights into the operational status and
    effectiveness of the analysis pipeline and overall project activity. Uses a
    grid of cards similar to the user report for visual consistency.

    Args:
        human_user_stats: A dictionary containing aggregated statistics for all
                          human users combined (e.g., total_jobs, total_bytes_billed,
                          average_score, high_risk_jobs, critical_risk_jobs).
        service_account_stats: A dictionary containing aggregated statistics
                               for all service accounts combined.
        season_colors: A dictionary containing color codes for email elements.
        mail_report_frequency: A string indicating the frequency of this report.
        project_id: The Google Cloud project ID.
        report_window_start: The start timestamp (UTC) of the report window.
        report_window_end: The end timestamp (UTC) of the report window.

    Returns:
        A string containing the complete HTML content for the developer email.
    """
    window_start_str = report_window_start.strftime('%Y-%m-%d') if report_window_start else 'N/A'
    window_end_str = report_window_end.strftime('%Y-%m-%d') if report_window_end else 'N/A'

    stats_list = {
        "Service Accounts": [
            {"label": "Total Jobs Analyzed", "value": service_account_stats.get('total_jobs', 0), "risk": None},
            {"label": "Total Bytes Billed", "value": format_bytes(service_account_stats.get('total_bytes_billed')), "risk": None},
            {"label": "Average Score", "value": f"{service_account_stats.get('average_score', 'N/A'):.1f}" if isinstance(service_account_stats.get('average_score'), (int, float)) else 'N/A', "risk": None},
            {"label": "High Risk Jobs", "value": service_account_stats.get('high_risk_jobs', 0), "risk": "high"},
            {"label": "Critical Risk Jobs", "value": service_account_stats.get('critical_risk_jobs', 0), "risk": "critical"}
        ],
        "Human Users": [
            {"label": "Total Jobs Analyzed", "value": human_user_stats.get('total_jobs', 0), "risk": None},
            {"label": "Total Bytes Billed", "value": format_bytes(human_user_stats.get('total_bytes_billed')), "risk": None},
            {"label": "Average Score", "value": f"{human_user_stats.get('average_score', 'N/A'):.1f}" if isinstance(human_user_stats.get('average_score'), (int, float)) else 'N/A', "risk": None},
            {"label": "High Risk Jobs", "value": human_user_stats.get('high_risk_jobs', 0), "risk": "high"},
            {"label": "Critical Risk Jobs", "value": human_user_stats.get('critical_risk_jobs', 0), "risk": "critical"}
        ]
    }

    def generate_stat_cards_html(stats_data: List[Dict[str, Any]], columns: int = 3) -> str:
        card_html = ""

        card_style = f"background-color: #f9f9f9; border: 1px solid #eeeeee; border-radius: 4px; text-align: center; padding: 10px; height: 100%;"
        value_style = f"display: block; font-size: 1.3em; color: {season_colors['secondary']}; margin-bottom: 5px; word-wrap: break-word; font-weight: bold;"
        label_style = f"display: block; font-size: 0.9em; color: #555555;"
        high_risk_style = f"color: #ffa726;"
        critical_risk_style = f"color: #ef5350;"

        card_html += '<table border="0" cellpadding="0" cellspacing="0" width="100%"><tr>'

        for i, stat in enumerate(stats_data):
            current_value_style = value_style
            if stat['risk'] == 'high':
                current_value_style += high_risk_style
            elif stat['risk'] == 'critical':
                current_value_style += critical_risk_style

            if i > 0 and i % columns == 0:
                card_html += '</tr><tr><td height="15" style="font-size: 15px; line-height: 15px;">&nbsp;</td></tr><tr>'

            if i % columns != 0:
                 card_html += '<td width="15" style="width: 15px;">&nbsp;</td>'

            card_html += f'<td width="{round(100/columns)}%" style="{card_style}" valign="top">'
            card_html += f'<strong style="{current_value_style}">{stat["value"]}</strong>'
            card_html += f'<span style="{label_style}">{stat["label"]}</span>'
            card_html += '</td>'

        remaining_cols = columns - (len(stats_data) % columns)
        if remaining_cols < columns:
            for _ in range(remaining_cols):
                 card_html += '<td width="15" style="width: 15px;">&nbsp;</td>'
                 card_html += f'<td width="{round(100/columns)}%"></td>'

        card_html += '</tr></table>'
        return card_html

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>BigQuery Sensei Activity Report</title>
        <style>
            body {{ font-family: sans-serif; line-height: 1.6; color: {season_colors['text']}; background-color: {season_colors['background']}; margin: 0; padding: 10px; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; width: 100% !important; }}
            table {{ border-collapse: collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; }}
            td {{ vertical-align: top; }}
            img {{ border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; -ms-interpolation-mode: bicubic; }}
            a {{ text-decoration: none; }}

            .container {{ max-width: 800px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; border: 1px solid #dddddd; }}
            .content-padding {{ padding: 20px; }}
            h1, h2, h3 {{ color: {season_colors['primary']}; margin-top: 0; margin-bottom: 15px; }}
            p {{ margin-top: 0; margin-bottom: 10px; }}

            .header {{ background-color: {season_colors['primary']}; color: #ffffff; padding: 15px; text-align: center; border-top-left-radius: 8px; border-top-right-radius: 8px; }}
            .header h2 {{ color: #ffffff; margin:0; }}
            .header p {{ color: #eeeeee; margin-bottom: 0; margin-top: 5px; font-size: 0.9em;}}

            .section {{ padding-bottom: 15px; margin-bottom: 15px; border-bottom: 1px solid #eeeeee; }}
            .section-no-border {{ padding-bottom: 15px; margin-bottom: 15px; }}


            .disclaimer {{
                margin-top: 20px;
                padding: 15px;
                background-color: #ffebee;
                color: #c62828;
                border-left: 4px solid #ef5350;
                font-size: 0.9em;
                line-height: 1.5;
            }}

            .footer {{ margin-top: 20px; text-align: center; font-size: 0.9em; color: #aaaaaa; padding-bottom: 20px; }}

            @media only screen and (max-width: 600px) {{
                .container {{ width: 100% !important; border-radius: 0 !important; }}
                td {{ display: block !important; width: 100% !important; }}
                .content-padding {{ padding: 10px !important; }}
                 .section, .section-no-border {{ padding-bottom: 10px !important; margin-bottom: 10px !important; }}
                 .stat-card {{ margin-bottom: 10px !important; }}
                 table table {{ width: 100% !important; }}
                 table table td {{ width: 100% !important; display: block !important; padding: 0 !important; margin-bottom: 10px; }}
                 table table td:last-child {{ margin-bottom: 0; }}
            }}
        </style>
    </head>
    <body style="margin: 0; padding: 10px; background-color: {season_colors['background']};">
        <div style="background-color: {season_colors['background']};">
            <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="max-width: 800px; width: 100%; background-color: #ffffff; border-radius: 8px; border: 1px solid #dddddd;" class="container">
                <tr>
                    <td align="center" class="header" style="background-color: {season_colors['primary']}; color: #ffffff; padding: 15px; text-align: center; border-top-left-radius: 8px; border-top-right-radius: 8px;">
                        <h2 style="color: #ffffff; margin:0;">BigQuery Sensei Activity Report</h2>
                        <p>This is your {mail_report_frequency.capitalize()} updates for the queries performed over the project &lt;{project_id}&gt;.</p>
                        <p style="color: #eeeeee; margin-bottom: 0; margin-top: 5px; font-size: 0.9em;">Summary for the period: {window_start_str} to {window_end_str} </p>
                    </td>
                </tr>

                <tr>
                    <td style="padding: 20px;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%">
                            <tr>
                                <td class="section" style="padding-bottom: 15px; margin-bottom: 15px; border-bottom: 1px solid #eeeeee;">
                                    <h3 style="color: {season_colors['primary']}; margin-top: 0; margin-bottom: 15px;">Service Account Activity Summary</h3>
                                    {generate_stat_cards_html(stats_list["Service Accounts"], columns=3)}
                                </td>
                            </tr>

                            <tr><td height="15" style="font-size: 15px; line-height: 15px;">&nbsp;</td></tr>

                            <tr>
                                <td class="section-no-border" style="padding-bottom: 15px; margin-bottom: 15px;">
                                     <h3 style="color: {season_colors['primary']}; margin-top: 0; margin-bottom: 15px;">Human User Activity Summary</h3>
                                     {generate_stat_cards_html(stats_list["Human Users"], columns=3)}
                                </td>
                            </tr>

                             <tr><td height="15" style="font-size: 15px; line-height: 15px;">&nbsp;</td></tr>


                            <tr>
                                <td style="padding: 0 15px;">
                                    <div class="disclaimer" style="margin-top: 0; padding: 15px; background-color: #ffebee; color: #c62828; border-left: 4px solid #ef5350; font-size: 0.9em; line-height: 1.5;">
                                        <strong>Disclaimer:</strong>
                                        Notice that all those stats are automatically performed by AI agents, all your information are kept private and useful only for you.
                                    </div>
                                </td>
                            </tr>

                        </table>
                    </td>
                </tr>

                 <tr>
                    <td align="center" class="footer" style="text-align: center; font-size: 0.9em; color: #aaaaaa; padding: 20px;">
                          This report is generated based on your BigQuery job analysis data.
                    </td>
                 </tr>

            </table>
        </div>
        </body>
    </html>
    """
    return html_content


def send_mail_to_recipients(recipients: List[str], subject: str, html_content: str, mailersend_api_token: str):
    """
    Sends an email to a list of recipients using MailerSend.

    Initializes the MailerSend client with the provided API token and sends
    an email with both HTML and plain text content to the specified recipients.
    Handles potential errors during the email sending process.

    Args:
        recipients: A list of email addresses to send the email to.
        subject: The subject line of the email.
        html_content: The HTML body of the email.
        mailersend_api_token: The API token for MailerSend.
    """
    if not recipients:
         logging.info("No recipients specified. Skipping email send.")
         return

    if not mailersend_api_token:
        logging.error("MailerSend API token is not available. Cannot send email.")
        return

    try:
        mailer = emails.NewEmail(mailersend_api_token)
    except Exception as e:
        logging.error(f"Error initializing MailerSend client: {e}", exc_info=True)
        return

    sender_email_address = f"noreply-bigquery-sensei@{MAILERSEND_DOMAIN}"
    sender_name = "BigQuery Sensei Agent Reports"
    mail_from = {
        "name": sender_name,
        "email": sender_email_address,
    }

    formatted_recipients = [{"email": rec} for rec in recipients]

    mail_body = {}
    mailer.set_mail_from(mail_from, mail_body)
    mailer.set_mail_to(formatted_recipients, mail_body)
    mailer.set_subject(subject, mail_body)
    mailer.set_html_content(html_content, mail_body)

    try:
        response = mailer.send(mail_body)
        logging.info(f"Email sent successfully to {', '.join(recipients)}.")
        # logging.debug(f"MailerSend API Response: {response.json()}") # Uncomment for detailed response
    except Exception as e:
        logging.error(f"Error sending email to {', '.join(recipients)}: {e}", exc_info=True)


def insert_user_report_to_bigquery(user_report_data: Dict[str, Any]):
    """
    Inserts a single summarized user report record into the BigQuery results table.

    Takes a dictionary containing the summarized report data for a user and
    inserts it as a new row into the `mail_notifier_suggestions_table`
    (RESULTS_TABLE_ID). Uses a ThreadLocal BigQuery client.

    Args:
        user_report_data: A dictionary containing the summarized report data,
                          formatted to match the `mail_notifier_suggestions_table` schema.
    """
    client = get_bigquery_client()
    table_ref = client.dataset(RESULTS_DATASET_ID).table(RESULTS_TABLE_ID)
    table = client.get_table(table_ref)

    if 'analysis_timestamp' in user_report_data and isinstance(user_report_data['analysis_timestamp'], datetime):
        if user_report_data['analysis_timestamp'].tzinfo is None:
            user_report_data['analysis_timestamp'] = user_report_data['analysis_timestamp'].replace(tzinfo=timezone.utc)
    if 'window_start_time' in user_report_data and isinstance(user_report_data['window_start_time'], datetime):
         if user_report_data['window_start_time'].tzinfo is None:
            user_report_data['window_start_time'] = user_report_data['window_start_time'].replace(tzinfo=timezone.utc)
    if 'window_end_time' in user_report_data and isinstance(user_report_data['window_end_time'], datetime):
         if user_report_data['window_end_time'].tzinfo is None:
            user_report_data['window_end_time'] = user_report_data['window_end_time'].replace(tzinfo=timezone.utc)


    rows_to_insert = [user_report_data]

    try:
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            logging.error(f"Errors inserting user report for {user_report_data.get('user_email', 'unknown user')} into BigQuery: {errors}")
        else:
            logging.info(f"Successfully inserted user report for {user_report_data.get('user_email', 'unknown user')} into BigQuery.")
    except Exception as e:
        logging.error(f"An error occurred during BigQuery insertion for user {user_report_data.get('user_email', 'unknown user')}: {e}", exc_info=True)


def process_user_report(user_email: str, analysis_window_start: datetime, analysis_window_end: datetime, mailersend_api_token: str, season_colors: Dict[str, str], mail_report_frequency: str, project_id: str, compiled_agent: Any, raw_llm_instance: ChatVertexAI, output_parser_instance: JsonOutputParser, llm_model_used: str, output_schema_str_used: str, prompt_template_used: ChatPromptTemplate):
    """
    Fetches data, aggregates, invokes LLM agent, generates graphs, builds email
    content, stores results in BigQuery, and sends a report for a single user.

    This function encapsulates the entire process for generating and sending
    a user-specific report. It is designed to be called concurrently for multiple
    users by the main function using a ThreadPoolExecutor. It orchestrates the
    data fetching, aggregation, LLM analysis, graph generation, data storage,
    and email sending steps.

    Args:
        user_email: The email address of the user to report on.
        analysis_window_start: The start timestamp (UTC) of the analysis window
                                (data fed to LLM and used for current period stats).
        analysis_window_end: The end timestamp (UTC) of the analysis window.
        mailersend_api_token: The API token for MailerSend.
        season_colors: Dictionary of seasonal color codes for email styling.
        mail_report_frequency: The frequency of the report (e.g., 'weekly').
        project_id: The Google Cloud project ID.
        compiled_agent: The compiled LangGraph agent instance for summarization
                        and URL validation.
        llm_chain: The initialized Langchain LLM chain object used by the agent.
        llm_model: The name of the LLM model used by the agent.
    """
    try:
        logging.info(f"Starting report generation for user: {user_email}")

        current_job_analysis_data = fetch_user_job_analysis_data(user_email, analysis_window_start, analysis_window_end)

        aggregated_data = aggregate_job_data(current_job_analysis_data)
        current_grade = aggregated_data.get('grade')
        current_total_bytes_billed = aggregated_data.get('total_bytes_billed')
        current_analyzed_jobs_count = aggregated_data.get('analyzed_jobs_count', 0)
        current_average_spider_scores = aggregated_data.get('average_spider_scores', {})

        if current_analyzed_jobs_count < MIN_STATS_JOBS_SEND_EMAIL:
            logging.info(f"Skipping email for {user_email}: Not enough analyzed jobs ({current_analyzed_jobs_count}) in the analysis window ({analysis_window_start} to {analysis_window_end}). Minimum required: {MIN_STATS_JOBS_SEND_EMAIL}")
            return

        historical_reports_data = fetch_historical_user_reports(user_email, analysis_window_end)

        llm_input_data = []
        for record in current_job_analysis_data:
            curated_record = {k: v for k, v in record.items() if k not in ['user_email']}
            llm_input_data.append(curated_record)

        agent_state_input: GraphState = {
            'llm_input': llm_input_data,
            'user_email': user_email,
            'window_start_time': analysis_window_start,
            'window_end_time': analysis_window_end,
            'total_bytes_billed': current_total_bytes_billed,
            'analyzed_jobs_count': current_analyzed_jobs_count,
            'average_spider_scores': current_average_spider_scores,
            'grade': current_grade,
            'llm_raw_output': None, 
            'search_query': None, 
            'search_results': [],
            'promising_results': [],
            'validated_urls': [], 
            'final_formatted_output': None, 
            'raw_llm': raw_llm_instance,
            'llm_model': llm_model_used,
            'output_parser': output_parser_instance, 
            '_prompt': prompt_template_used, 
            'output_schema_str': output_schema_str_used,
        }

        logging.info(f"Invoking LLM agent for user: {user_email} for analysis window {analysis_window_start} to {analysis_window_end}")

        final_agent_state = compiled_agent.invoke(agent_state_input)
        logging.info(f"LLM agent finished for user: {user_email}")

        user_report_data = final_agent_state.get('final_formatted_output')

        if not user_report_data:
             logging.error(f"LLM agent failed to produce final formatted output for user: {user_email}. Skipping email and BigQuery insertion.")
             return

        # --- Store the summarized report in BigQuery ---
        insert_user_report_to_bigquery(user_report_data)


        # --- Generate Graphs ---
        spider_graph_base64 = None
        trendlines_graph_base64 = None

        if current_average_spider_scores:
            spider_graph_path = f"/tmp/spider_graph_{user_email.replace('@', '_').replace('.', '_')}_{analysis_window_end.strftime('%Y%m%d%H%M%S')}.png"
            try:
                generated_spider_path = generate_spider_graph(current_average_spider_scores, spider_graph_path, season_colors)
                if generated_spider_path and os.path.exists(generated_spider_path):
                    spider_graph_base64 = get_image_base64(generated_spider_path)
                    os.remove(generated_spider_path)
            except Exception as e:
                logging.error(f"Error generating spider graph for {user_email}: {e}", exc_info=True)

        current_report_analysis_timestamp = user_report_data.get('analysis_timestamp', datetime.now(timezone.utc))

        current_period_trend_data = {
            'analysis_timestamp': current_report_analysis_timestamp,
            'grade': current_grade,
            'total_bytes_billed': current_total_bytes_billed,
            'analyzed_jobs_count': current_analyzed_jobs_count,
        }
        all_trend_data = historical_reports_data + [current_period_trend_data]
        all_trend_data.sort(key=lambda x: x.get('analysis_timestamp', datetime.min.replace(tzinfo=timezone.utc)))

        trendline_timestamps = []
        trendline_grades = []
        trendline_dollars_per_job = []

        for report in all_trend_data:
            timestamp = report.get('analysis_timestamp')
            grade = report.get('grade')
            bytes_billed = report.get('total_bytes_billed')
            jobs_count = report.get('analyzed_jobs_count')

            if timestamp is not None and grade is not None and jobs_count is not None and bytes_billed is not None:
                trendline_timestamps.append(timestamp)
                trendline_grades.append(grade)

                if jobs_count > 0 and bytes_billed is not None:
                    cost_in_dollars = (bytes_billed / BYTES_IN_TB) * BIGQUERY_COST_PER_TB_USD
                    trendline_dollars_per_job.append(cost_in_dollars / jobs_count)
                else:
                    trendline_dollars_per_job.append(None)
            else:
                 trendline_timestamps.append(timestamp)
                 trendline_grades.append(None)
                 trendline_dollars_per_job.append(None)


        if trendline_timestamps and len(trendline_timestamps) >= 2:
             trendlines_graph_path = f"/tmp/trendlines_graph_{user_email.replace('@', '_').replace('.', '_')}_{analysis_window_end.strftime('%Y%m%d%H%M%S')}.png"
             try:
                  generated_trendlines_path = generate_trendlines_graph(trendline_timestamps, trendline_grades, trendline_dollars_per_job, trendlines_graph_path, season_colors)
                  if generated_trendlines_path and os.path.exists(generated_trendlines_path):
                      trendlines_graph_base64 = get_image_base64(generated_trendlines_path)
                      os.remove(generated_trendlines_path)
             except Exception as e:
                  logging.error(f"Error generating trendlines graph for {user_email}: {e}", exc_info=True)


        # --- Build and Send Email ---
        subject = f"Your BigQuery Sensei {mail_report_frequency.capitalize()} Report for Project <{project_id}>"
        html_content = build_user_email_html(user_report_data, historical_reports_data, season_colors, mail_report_frequency, project_id, spider_graph_base64, trendlines_graph_base64)
        send_mail_to_recipients([user_email], subject, html_content, mailersend_api_token)

    except Exception as e:
        logging.error(f"An unhandled error occurred while processing report for user {user_email}: {e}", exc_info=True)


def main(request):
    """
    Google Cloud Function entry point for generating and sending BigQuery Sensei reports.

    This function is triggered by an HTTP request. It retrieves necessary
    environment variables, determines the reporting and analysis time windows,
    fetches the list of human users to report on, aggregates service account
    activity, builds the LLM analysis agent, and then processes reports for
    each human user concurrently using a ThreadPoolExecutor. It also generates
    and sends a developer report summarizing activity across human users and
    service accounts.

    Args:
        request: The HTTP request object. (Not directly used for request data
                 in this scheduled context, but required by Cloud Functions).
    """
    logging.info("BigQuery Sensei Report Generator started.")
    
    # --- Retrieve Environment Variables and Secrets ---
    if not all([GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION, MAILERSEND_API_TOKEN_SECRET_PATH, MAILERSEND_DOMAIN, STATSTABLE_DATASET_ID, STATSTABLE_TABLE_ID, RESULTS_DATASET_ID, RESULTS_TABLE_ID, DEVELOPER_USERS_MAILLIST_STR, AGENT_CONFIG_FILE, LLM_MODEL]):
        logging.critical("Missing required environment variables. Cannot proceed.")
        return {"statusCode": 500, "body": "Missing required environment variables."}

    mailersend_api_token = get_secret_value(MAILERSEND_API_TOKEN_SECRET_PATH)
    if not mailersend_api_token:
        logging.critical("Failed to retrieve MailerSend API token from Secret Manager. Cannot send emails.")


    developer_users_maillist = [
        email.strip().replace("user:", "").replace("serviceAccount:", "")
        for email in DEVELOPER_USERS_MAILLIST_STR.split(",")
        if email.strip()
    ]
    if not developer_users_maillist:
        logging.warning("DEVELOPER_USERS_MAILLIST is not set or is empty. Developer report will not be sent.")

    # --- Determine Reporting, Analysis Time Windows and Season Colors ---
    now_utc = datetime.now(timezone.utc)
    current_season_key = get_season(now_utc)
    season_colors = SEASONAL_COLORS.get(current_season_key, SEASONAL_COLORS['winter'])
    report_window_start, report_window_end = calculate_report_window(MAIL_REPORT_FREQUENCY, now_utc)
    analysis_window_start, analysis_window_end = calculate_analysis_window(report_window_start, report_window_end, MAIL_REPORT_FREQUENCY)
    
    logging.info(f"Analysis window: {analysis_window_start} to {analysis_window_end} (Used for LLM analysis and current stats)")


    # --- Build LLM Analysis Agent ---
    try:
        compiled_agent, raw_llm_instance, output_parser_instance, llm_model_used, output_schema_str_used, prompt_template_used = build_analysis_agent(
            project_id=GOOGLE_CLOUD_PROJECT,
            location=GOOGLE_CLOUD_LOCATION,
            llm_model=LLM_MODEL,
            agent_config_file=AGENT_CONFIG_FILE
        )
        logging.info(f"LLM Analysis Agent built successfully using model: {llm_model_used}")
    except Exception as e:
        logging.critical(f"Failed to build LLM Analysis Agent: {e}", exc_info=True)
        return {"statusCode": 500, "body": "Failed to build LLM Analysis Agent."}


    # --- Get List of Human Users to Report On ---
    users_to_report = fetch_users_for_reporting(analysis_window_start, analysis_window_end)

    if not users_to_report:
        logging.info("No human users found with activity in the report window. Skipping user reports.")
    else:
        logging.info(f"Found {len(users_to_report)} human users to potentially report on.")

    # --- Aggregate Service Account Activity ---
    service_account_activity_stats = aggregate_service_account_activity(analysis_window_start, analysis_window_end)
    logging.info("Aggregated service account activity.")

    # --- Aggregate Human User Activity for Developer Report ---
    human_user_activity_stats = aggregate_human_user_activity_stats(analysis_window_start, analysis_window_end)
    logging.info("Aggregated human user activity for developer report.")


    # --- Process User Reports in Parallel ---
    max_workers = min(32, len(users_to_report))
    results = {}

    if users_to_report:
        logging.info(f"Processing reports for {len(users_to_report)} human users in parallel with {max_workers} workers.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_user = {executor.submit(process_user_report, user_email, analysis_window_start, analysis_window_end, mailersend_api_token, season_colors, MAIL_REPORT_FREQUENCY, GOOGLE_CLOUD_PROJECT, compiled_agent, raw_llm_instance, output_parser_instance, llm_model_used, output_schema_str_used, prompt_template_used): user_email for user_email in users_to_report}

            for future in concurrent.futures.as_completed(future_to_user):
                user_email = future_to_user[future]
                try:
                    future.result()
                    logging.info(f"Successfully processed report for user: {user_email}")
                    results[user_email] = "Success"
                except Exception as exc:
                    logging.error(f"User report processing for {user_email} generated an exception: {exc}", exc_info=True)
                    results[user_email] = f"Error: {exc}"

        logging.info("Finished processing all user reports.")
        logging.info(f"User report processing results summary: {results}")
    else:
        logging.info("No human users to process reports for.")


    # --- Generate and Send Developer Report ---
    if developer_users_maillist:
        logging.info(f"Generating and sending developer report to: {', '.join(developer_users_maillist)}")
        developer_html = build_developer_email_html(
            human_user_activity_stats,
            service_account_activity_stats,
            season_colors,
            MAIL_REPORT_FREQUENCY,
            GOOGLE_CLOUD_PROJECT,
            analysis_window_start,
            analysis_window_end
        )
        send_mail_to_recipients(developer_users_maillist, f"BigQuery Sensei Activity Summary ({MAIL_REPORT_FREQUENCY.capitalize()})", developer_html, mailersend_api_token)
        logging.info("Developer report sent.")
    else:
        logging.warning("No developer users specified. Skipping developer report.")


    logging.info("BigQuery Sensei Report Generator finished.")
    return {"statusCode": 200, "body": "Reports processed."}