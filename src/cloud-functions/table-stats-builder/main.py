import os
import asyncio
import logging
import functions_framework
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
from agent_builder import build_resources_and_agent, GraphState


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
GOOGLE_CLOUD_PROJECT = os.getenv("PROJECT_ID")
GOOGLE_CLOUD_LOCATION = os.getenv("REGION")
EXEC_SERVICE_ACCOUNT = os.getenv("EXEC_SERVICE_ACCOUNT")
MAILNOTIFIER_JOBS_HEADER = os.getenv("MAILNOTIFIER_JOBS_HEADER")
DEFAULT_TIMEDELTA_SECONDS = int(os.getenv("TIMEDELTA_CONSIDERED_SEC", 600))
AGENT_CONFIG_FILE = os.getenv("AGENT_CONFIG_FILE")
LLM_MODEL_NAME = os.getenv("LLM_MODEL")
RESULTS_DATASET_ID = os.getenv("RESULTS_DATASET_ID")
RESULTS_TABLE_ID = os.getenv("RESULTS_TABLE_ID")
FAILED_RESULTS_TABLE_ID = os.getenv("FAILED_RESULTS_TABLE_ID")
RUNNED_EXTRACTIONS_TABLE_ID = os.getenv("RUNNED_EXTRACTIONS_TABLE_ID")


BQ_REGION_EU = "eu"
BQ_REGION_US = "us"
INFORMATION_SCHEMA_PATTERN = "INFORMATION_SCHEMA"


bq_client: Optional[bigquery.Client] = None
agent_graph = None
target_results_table_full_id: Optional[str] = None
failed_analysis_table_full_id: Optional[str] = None
run_summary_table_full_id: Optional[str] = None
service_initialized: bool = False
initialization_error: Optional[Exception] = None

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def _convert_datetimes_to_isoformat(data: Any) -> Any:
    """
    Recursively converts datetime objects within a dictionary or list to ISO 8601 strings.
    Handles nested structures.

    Args:
        data: The input data structure (dict, list, datetime, or other).

    Returns:
        A new data structure with datetime objects converted to ISO 8601 strings.
        Original data types are preserved for non-datetime values. Naive datetimes
        are assumed to be UTC.
    """
    if isinstance(data, datetime):
        if data.tzinfo is None:
            logging.warning(f"Naive datetime found: {data}. Assuming UTC for conversion.")
            return data.replace(tzinfo=timezone.utc).isoformat()
        return data.isoformat()
    elif isinstance(data, dict):
        return {key: _convert_datetimes_to_isoformat(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_convert_datetimes_to_isoformat(element) for element in data]
    else:
        return data


def run_job_query(start_time: datetime, end_time: datetime) -> List[Dict]:
    """
    Lists BigQuery jobs for the project using client.list_jobs() within a specific time range.

    Leverages the initialized BigQuery client and the 'bigquery.jobs.listAll' permission
    (granted via roles/bigquery.resourceViewer or similar) to fetch job metadata across
    all users in the project. Filters jobs by creation time. The 'region' parameter
    is kept for function signature compatibility but note that list_jobs operates
    project-wide.

    Args:
        start_time: The UTC start timestamp (inclusive) for the query window.
        end_time: The UTC end timestamp (inclusive) for the query window.

    Returns:
        A list of dictionaries, each representing a job, or an empty list if
        the operation fails or the client is unavailable.
    """
    if not bq_client:
        logging.error("BigQuery client is not available in run_job_query.")
        return []
    if not GOOGLE_CLOUD_PROJECT:
        logging.error("Google Cloud Project ID is not configured.")
        return []

    logging.info(f"Listing jobs for project: {GOOGLE_CLOUD_PROJECT}, time: {start_time.isoformat()} to {end_time.isoformat()}")

    try:
        jobs = bq_client.list_jobs(
            project=GOOGLE_CLOUD_PROJECT,
            min_creation_time=start_time,
            max_creation_time=end_time,
            all_users=True
        )

        results = []
        for job in jobs:
             job_dict = {
                 "job_id": job.job_id,
                 "user_email": job.user_email,
                 "creation_time": job.created,
                 "start_time": job.started,
                 "end_time": job.ended,
                 "job_type": job.job_type,
                 "state": job.state,
                 "error_result": job.error_result,
                 "total_bytes_billed": job.total_bytes_billed,
                 "query": job.query,
                 "statement_type": job.statement_type,
             }
             results.append(job_dict)


        logging.info(f"Successfully listed {len(results)} jobs for project {GOOGLE_CLOUD_PROJECT}.")
        return results
    except Exception as e:
        logging.error(f"Failed to list jobs for project {GOOGLE_CLOUD_PROJECT}: {e}", exc_info=True)
        return []


def insert_analysis_results_into_bigquery(rows: List[Dict]):
    """
    Inserts a batch of processed analysis results into the configured results table.

    Uses the initialized BigQuery client and target results table ID to perform a
    streaming insert. Logs success or detailed errors encountered during insertion.
    Designed to be run within a thread pool executor from an async context.
    Ensures the inserted rows match the schema of the successful analysis table.

    Args:
        rows: A list of dictionaries, where each dictionary conforms to the
              schema of the target BigQuery results table. Datetime objects
              are converted to ISO 8601 strings before insertion.
    """
    if not bq_client:
        logging.error("BigQuery client is not available for inserting analysis results.")
        return
    if not rows:
        logging.info("Received empty list of analysis results; nothing to insert.")
        return
    if target_results_table_full_id is None:
        logging.error("Target BigQuery analysis results table ID is not configured for insertion.")
        return

    converted_rows = [_convert_datetimes_to_isoformat(row) for row in rows]

    logging.info(f"Initiating insertion of {len(converted_rows)} analysis results into {target_results_table_full_id}.")
    try:
        errors = bq_client.insert_rows_json(target_results_table_full_id, converted_rows)
        if not errors:
            logging.info(f"Successfully inserted {len(converted_rows)} rows into {target_results_table_full_id}.")
        else:
            logging.error(f"Encountered errors inserting {len(errors)} row groups into {target_results_table_full_id}:")
            for error_detail in errors:
                error_messages = [str(err.get('message', 'Unknown error')) for err in error_detail.get('errors', [])]
                logging.error(f"  Row index {error_detail.get('index', 'N/A')}: {', '.join(error_messages)}")
    except Exception as e:
        logging.error(f"Unexpected error during BigQuery insert operation to {target_results_table_full_id}: {e}", exc_info=True)


def insert_failed_analyses_into_bigquery(rows: List[Dict]):
    """
    Inserts a batch of failed analysis job records into the configured failed analysis table.

    Uses the initialized BigQuery client and failed analysis table ID to perform a
    streaming insert. Logs success or detailed errors encountered during insertion.
    Designed to be run within a thread pool executor from an async context.
    Ensures the inserted rows match the schema of the failed analysis table.

    Args:
        rows: A list of dictionaries, where each dictionary conforms to the
              schema of the target BigQuery failed analysis table. Datetime objects
              are converted to ISO 8601 strings before insertion.
    """
    if not bq_client:
        logging.error("BigQuery client is not available for inserting failed analyses.")
        return
    if not rows:
        logging.info("Received empty list of failed analyses; nothing to insert.")
        return
    if failed_analysis_table_full_id is None:
        logging.error("Target BigQuery failed analysis table ID is not configured for insertion.")
        return

    converted_rows = [_convert_datetimes_to_isoformat(row) for row in rows]

    logging.info(f"Initiating insertion of {len(converted_rows)} failed analysis records into {failed_analysis_table_full_id}.")
    try:
        errors = bq_client.insert_rows_json(failed_analysis_table_full_id, converted_rows)
        if not errors:
            logging.info(f"Successfully inserted {len(converted_rows)} rows into {failed_analysis_table_full_id}.")
        else:
            logging.error(f"Encountered errors inserting {len(errors)} row groups into {failed_analysis_table_full_id}:")
            for error_detail in errors:
                error_messages = [str(err.get('message', 'Unknown error')) for err in error_detail.get('errors', [])]
                logging.error(f"  Row index {error_detail.get('index', 'N/A')}: {', '.join(error_messages)}")
    except Exception as e:
        logging.error(f"Unexpected error during BigQuery insert operation to {failed_analysis_table_full_id}: {e}", exc_info=True)


def insert_run_summary_into_bigquery(row: Dict):
    """
    Inserts a single summary row into the configured run details table.

    Uses the initialized BigQuery client and the run summary table ID to perform
    a streaming insert of a single record representing the outcome of a single
    execution run (time window). Designed to be run within a thread pool executor
    from an async context. Ensures the inserted row matches the schema of the
    run details table.

    Args:
        row: A dictionary representing the summary data for the run. Datetime objects
             are converted to ISO 8601 strings before insertion.
    """
    if not bq_client:
        logging.error("BigQuery client is not available for inserting run summary.")
        return
    if not row:
        logging.info("Received empty run summary row; nothing to insert.")
        return
    if run_summary_table_full_id is None:
        logging.error("Target BigQuery run summary table ID is not configured for insertion.")
        return

    rows_to_insert = [_convert_datetimes_to_isoformat(row)]

    logging.info(f"Initiating insertion of 1 run summary row into {run_summary_table_full_id}.")
    try:
        errors = bq_client.insert_rows_json(run_summary_table_full_id, rows_to_insert)
        if not errors:
            logging.info(f"Successfully inserted 1 row into {run_summary_table_full_id}.")
        else:
            logging.error(f"Encountered errors inserting the run summary row into {run_summary_table_full_id}:")
            for error_detail in errors:
                error_messages = [str(err.get('message', 'Unknown error')) for err in error_detail.get('errors', [])]
                logging.error(f"  Row index {error_detail.get('index', 'N/A')}: {', '.join(error_messages)}")
    except Exception as e:
        logging.error(f"Unexpected error during BigQuery insert operation to {run_summary_table_full_id}: {e}", exc_info=True)


async def invoke_agent_for_row(row: dict) -> Dict[str, Any]:
    """
    Asynchronously prepares input and invokes the compiled LangGraph agent for a single job row.

    Constructs the initial state for the agent graph based on the input job row,
    then invokes the agent asynchronously. Returns the final state of the graph,
    which includes both the original row and the analysis output (if successful)
    or error information if processing failed. This function is intended to process
    jobs fetched from BigQuery that are NOT internal INFORMATION_SCHEMA queries.

    Args:
        row: A dictionary representing a single job fetched from BigQuery,
             representing a job that needs analysis.

    Returns:
        The final state dictionary from the LangGraph agent. This dictionary is
        expected to contain keys like 'original_row', 'formatted_output' (if analysis
        was successful and parsed), and 'parsing_error' or other error details
        if analysis failed. Returns a state dictionary with minimal information
        if the agent is unavailable or invocation fails before the graph runs.
    """
    job_id = row.get("job_id", "UNKNOWN_JOB_ID")
    if not agent_graph:
        logging.error(f"LangGraph agent is not available. Cannot process job row {job_id}.")
        return {
            "llm_input": {},
            "original_row": row,
            "llm_raw_output": None,
            "formatted_output": None,
            "raw_llm_text_output": None,
            "parsing_error": "Agent not available"
        }

    logging.debug(f"Preparing input and invoking agent for job ID: {job_id}")

    try:
        llm_input_data = {
            "query": row.get("query"),
            "statement_type": row.get("statement_type"),
            "error_result": row.get("error_result"),
            "total_bytes_billed": row.get("total_bytes_billed"),
        }

        initial_state: GraphState = {
            "llm_input": llm_input_data,
            "original_row": row,
            "llm_raw_output": None,
            "formatted_output": None,
            "raw_llm_text_output": None,
            "parsing_error": None
        }

        final_state = await agent_graph.ainvoke(initial_state)

        logging.debug(f"Final state for job ID {job_id}: {final_state}")

        return final_state

    except Exception as e:
        logging.error(f"Agent invocation failed for job ID {job_id}: {e}", exc_info=True)
        return {
            "llm_input": {},
            "original_row": row,
            "llm_raw_output": None,
            "formatted_output": None,
            "raw_llm_text_output": None,
            "parsing_error": f"Agent invocation error: {e}"
        }


async def main_logic(start_time: datetime, end_time: datetime, is_initial_backfill_run: bool) -> None:
    """
    Orchestrates the core asynchronous workflow of fetching BigQuery job logs
    using list_jobs(), analyzing them using a LangGraph agent, and storing
    results and failures.

    Fetches job data from the project within a given time window using client.list_jobs().
    Filters out internal INFORMATION_SCHEMA queries. Concurrently invokes an
    analysis agent for each remaining job. Batches and inserts successful
    analysis results and records of failed analyses into separate BigQuery tables.
    Finally, inserts a summary row into a dedicated table indicating the outcome
    of this specific execution run and the processed time window.

    Args:
        start_time: The UTC start timestamp (inclusive) for the job fetching window.
        end_time: The UTC end timestamp (inclusive) for the job fetching window.
        is_initial_backfill_run: A boolean flag indicating if this run is part
                                 of an initial data backfill process.
    """
    if not service_initialized:
        logging.critical("Service is not initialized. Aborting main logic.")
        return

    logging.info(f"Executing main logic for time range: {start_time.isoformat()} to {end_time.isoformat()}")
    logging.info(f"Initial backfill flag for this run: {is_initial_backfill_run}")

    jobs_to_process: List[Dict] = []
    total_jobs_fetched_count = 0
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            loop = asyncio.get_running_loop()
            jobs_to_process = await loop.run_in_executor(executor, run_job_query, start_time, end_time)
            total_jobs_fetched_count = len(jobs_to_process)
            logging.info(f"Total jobs listed for project: {total_jobs_fetched_count}")
    except Exception as e:
        logging.error(f"Failed during job listing using client.list_jobs(): {e}", exc_info=True)
        jobs_to_process = []
        total_jobs_fetched_count = 0

    filtered_jobs_to_process = [
        job for job in jobs_to_process
        if isinstance(job.get('query'), str) and (
            # Exclude condition 1: INFORMATION_SCHEMA queries done by Service Account
            not (INFORMATION_SCHEMA_PATTERN in job.get('query', '') and job.get('user_email') == EXEC_SERVICE_ACCOUNT)
            # Exclude condition 2: Queries having the mail notifier jobs header
            and (MAILNOTIFIER_JOBS_HEADER is None or MAILNOTIFIER_JOBS_HEADER not in job.get('query', ''))
        )
    ]
    filtered_jobs_count = len(filtered_jobs_to_process)

    filtered_out_count = total_jobs_fetched_count - filtered_jobs_count
    if filtered_out_count > 0:
        logging.info(f"Filtered out {filtered_out_count} jobs querying {INFORMATION_SCHEMA_PATTERN} tables.")
    logging.info(f"Proceeding with analysis for {filtered_jobs_count} jobs.")

    if not filtered_jobs_to_process:
        logging.warning("No jobs eligible for analysis after filtering. Main logic concluding.")
        current_utc_timestamp_completion = datetime.now(timezone.utc)

        summary_row_no_analyzable_jobs = {
            "analysis_timestamp": current_utc_timestamp_completion,
            "window_start_time": start_time,
            "window_end_time": end_time,
            "is_initial_backfill_run": is_initial_backfill_run,
            "total_analysis_jobs_count": filtered_jobs_count,
            "successful_analysis_jobs_count": 0,
            "failed_analysis_jobs_count": 0,
        }
        try:
             with ThreadPoolExecutor(max_workers=1) as executor:
                 loop = asyncio.get_running_loop()
                 await loop.run_in_executor(executor, insert_run_summary_into_bigquery, summary_row_no_analyzable_jobs)
        except Exception as e:
             logging.error(f"Failed to insert summary row when no analyzable jobs were found: {e}", exc_info=True)
        return


    logging.info(f"Initiating concurrent analysis for {filtered_jobs_count} jobs...")
    analysis_tasks = [invoke_agent_for_row(row) for row in filtered_jobs_to_process]
    analysis_results_states = await asyncio.gather(*analysis_tasks)

    valid_results_to_insert: List[Dict] = []
    failed_analyses_to_insert: List[Dict] = []

    successful_analysis_jobs_count = 0
    failed_analysis_jobs_count = 0

    current_utc_timestamp_for_records = datetime.now(timezone.utc)

    for final_state in analysis_results_states:
        original_row = final_state.get('original_row', {})
        formatted_output = final_state.get('formatted_output')
        job_id = original_row.get('job_id', 'UNKNOWN_JOB_ID')

        if formatted_output is not None:
            successful_analysis_jobs_count += 1
            logging.debug(f"Job {job_id}: Analysis successful, adding to results batch.")

            successful_record = {
                "analysis_timestamp": formatted_output.get("analysis_timestamp", current_utc_timestamp_for_records),
                "job_id": formatted_output.get("job_id", job_id),
                "user_email":  original_row.get("user_email"),
                "total_bytes_billed": original_row.get("total_bytes_billed"),
                "agent_name": formatted_output.get("agent_name"),
                "llm_version": formatted_output.get("llm_version"),
                "query": formatted_output.get("query"),
                "statement_type": formatted_output.get("statement_type"),
                "average_score": formatted_output.get("average_score"),
                "score": formatted_output.get("score"),
                "recommendations": formatted_output.get("recommendations"),
                "risk_level": formatted_output.get("risk_level"),
                "is_safe_to_run": formatted_output.get("is_safe_to_run"),
                "score_confidence": formatted_output.get("score_confidence"),
            }
            valid_results_to_insert.append(successful_record)

        else:
            failed_analysis_jobs_count += 1
            logging.warning(f"Job {job_id}: Analysis failed or produced no valid output. Adding to failed batch.")

            bq_error_result = original_row.get("error_result", {})
            bq_error_code = bq_error_result.get("reason", "ANALYSIS_FAILED")

            bq_error_messages = []
            if "message" in bq_error_result:
                bq_error_messages.append(bq_error_result["message"])
            if "errors" in bq_error_result and isinstance(bq_error_result["errors"], list):
                for err in bq_error_result["errors"]:
                    if "message" in err:
                        bq_error_messages.append(f"Detail: {err['message']}")

            failure_detail = "; ".join(bq_error_messages) if bq_error_messages else final_state.get('parsing_error', 'LLM analysis failed to produce valid structured output or BQ error unavailable.')

            failed_record = {
                "analysis_timestamp": current_utc_timestamp_for_records,
                "job_id": job_id,
                "user_email":  original_row.get("user_email"),
                "total_bytes_billed": original_row.get("total_bytes_billed"),
                "agent_name": "langgraph_analyzer",
                "llm_version": original_row.get("llm_version", LLM_MODEL_NAME),
                "query": original_row.get("query"),
                "statement_type": original_row.get("statement_type"),
                "error_code": bq_error_code,
                "failure_reason": failure_detail,
            }
            failed_analyses_to_insert.append(failed_record)

    logging.info(f"Analysis processing complete. Successful analyses: {successful_analysis_jobs_count}, Failed analyses: {failed_analysis_jobs_count}")

    insert_tasks = []
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        if valid_results_to_insert:
            insert_tasks.append(loop.run_in_executor(executor, insert_analysis_results_into_bigquery, valid_results_to_insert))
        else:
            logging.info("No valid analysis results to insert.")

        if failed_analyses_to_insert:
            insert_tasks.append(loop.run_in_executor(executor, insert_failed_analyses_into_bigquery, failed_analyses_to_insert))
        else:
            logging.info("No failed analysis records to insert.")

        if insert_tasks:
            await asyncio.gather(*insert_tasks)

    current_utc_timestamp_completion = datetime.now(timezone.utc)

    summary_row = {
        "analysis_timestamp": current_utc_timestamp_completion,
        "window_start_time": start_time,
        "window_end_time": end_time,
        "is_initial_backfill_run": is_initial_backfill_run,
        "total_analysis_jobs_count": filtered_jobs_count,
        "successful_analysis_jobs_count": successful_analysis_jobs_count,
        "failed_analysis_jobs_count": failed_analysis_jobs_count,
    }

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, insert_run_summary_into_bigquery, summary_row)
    except Exception as e:
        logging.error(f"Failed during concurrent run summary insertion: {e}", exc_info=True)


    logging.info("Main logic execution finished.")


def _initialize_service():
    """
    Initializes the service by building resources and the analysis agent.

    This function runs once when the Cloud Function instance starts up. It reads
    required environment variables, configures the BigQuery client, loads the
    agent configuration, and builds the LangGraph agent and its components using
    the logic from `agent_builder.py`. It sets global state variables for the
    BigQuery client, agent graph, and full table IDs for results, failed analyses,
    and run summaries. It sets a global flag (`service_initialized`) to indicate
    readiness or captures an initialization error if setup fails.
    """
    global bq_client, agent_graph, target_results_table_full_id, failed_analysis_table_full_id, run_summary_table_full_id, service_initialized, initialization_error
    logging.info("Attempting service initialization...")

    project_id = GOOGLE_CLOUD_PROJECT
    location = GOOGLE_CLOUD_LOCATION
    llm_model = LLM_MODEL_NAME
    config_path = AGENT_CONFIG_FILE
    results_dataset_id = RESULTS_DATASET_ID
    results_table_id = RESULTS_TABLE_ID
    failed_table_id = FAILED_RESULTS_TABLE_ID
    run_summary_table_id = RUNNED_EXTRACTIONS_TABLE_ID

    required_env_vars = {
        "PROJECT_ID": project_id,
        "REGION": location,
        "LLM_MODEL": llm_model,
        "AGENT_CONFIG_FILE": config_path,
        "RESULTS_DATASET_ID": results_dataset_id,
        "RESULTS_TABLE_ID": results_table_id,
        "FAILED_RESULTS_TABLE_ID": failed_table_id,
        "RUNNED_EXTRACTIONS_TABLE_ID": run_summary_table_id,
    }
    missing_vars = [name for name, value in required_env_vars.items() if value is None]

    if missing_vars:
        initialization_error = ValueError(f"One or more required environment variables are not set: {missing_vars}")
        logging.critical(f"Service initialization failed: {initialization_error}")
        service_initialized = False
    else:
        try:
            bq_client, agent_graph, target_results_table_full_id = build_resources_and_agent(
                project_id=project_id,
                location=location,
                llm_model_name=llm_model,
                agent_config_path=config_path,
                results_dataset_id=results_dataset_id,
                results_table_id=results_table_id
            )

            failed_analysis_table_full_id = f"{project_id}.{results_dataset_id}.{failed_table_id}"
            run_summary_table_full_id = f"{project_id}.{results_dataset_id}.{run_summary_table_id}"

            logging.info(f"Target analysis results table ID set to: {target_results_table_full_id}")
            logging.info(f"Target failed analysis table ID set to: {failed_analysis_table_full_id}")
            logging.info(f"Target run summary table ID set to: {run_summary_table_full_id}")

            service_initialized = True
            logging.info("Service initialization successful. Ready to process requests.")

        except ImportError as ie:
             initialization_error = ie
             service_initialized = False
             logging.critical(f"Service initialization failed: Could not import agent_builder. {ie}", exc_info=True)
        except Exception as e:
            initialization_error = e
            service_initialized = False
            logging.critical(f"Service initialization failed during build process: {e}", exc_info=True)

_initialize_service()


@functions_framework.http
def main(request) -> Tuple[Dict[str, Any], int]:
    """
    HTTP Cloud Function entry point for triggering BigQuery job log analysis.

    Validates service readiness, parses the incoming HTTP request to determine
    the time range for querying BigQuery job logs and the initial backfill flag.
    Handles request validation (method, time format, flag value) and returns
    appropriate HTTP responses (200 for success, 400 for bad request, 405 for
    method not allowed, 500 for internal errors, 503 if not initialized).
    Includes a check to prevent initial backfill requests if a previous one
    is already recorded in the run details table.
    Triggers the main asynchronous job processing logic if the request is valid
    and the service is initialized, and the backfill check passes.

    Args:
        request: The Flask request object provided by the Functions Framework.
                 Expected to be a POST request with an optional JSON body containing
                 'start_time' (ISO 8601 string), 'end_time' (ISO 8601 string),
                 and 'is_initial_backfill' (string 'TRUE' or 'FALSE').

    Returns:
        A tuple containing a JSON response dictionary indicating the status
        ('success' or 'error') and a descriptive message, along with the
        corresponding HTTP status code (e.g., 200, 400, 500, 503).
    """
    if not service_initialized:
        err_msg = f"Service Unavailable: Initialization failed. Error: {initialization_error or 'Unknown initialization error'}"
        logging.error(f"Request denied. {err_msg}")
        return {"error": err_msg}, 503

    if request.method != 'POST':
        logging.warning(f"Received non-POST request method: {request.method}")
        return {"error": "Method Not Allowed. Use POST."}, 405

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    is_initial_backfill_run: bool = False

    try:
        data = request.get_json(silent=True)

        if data and isinstance(data, dict):
            start_time_str = data.get("start_time")
            end_time_str = data.get("end_time")
            is_initial_backfill_str = data.get("is_initial_backfill")

            if is_initial_backfill_str is not None:
                if isinstance(is_initial_backfill_str, str):
                    upper_is_initial_backfill = is_initial_backfill_str.upper()
                    if upper_is_initial_backfill == "TRUE":
                        is_initial_backfill_run = True
                        logging.info("Initial backfill flag set to TRUE based on request.")
                    elif upper_is_initial_backfill == "FALSE":
                         is_initial_backfill_run = False
                         logging.info("Initial backfill flag set to FALSE based on request.")
                    else:
                        msg = f"Invalid string value for is_initial_backfill: '{is_initial_backfill_str}'. Expected 'TRUE' or 'FALSE'."
                        logging.error(msg)
                        return {"error": f"Bad Request: {msg}"}, 400
                else:
                     msg = f"Invalid type for is_initial_backfill. Expected string 'TRUE' or 'FALSE', got {type(is_initial_backfill_str).__name__}."
                     logging.error(msg)
                     return {"error": f"Bad Request: {msg}"}, 400
            else:
                 logging.info("No 'is_initial_backfill' flag provided in request body. Defaulting to FALSE.")

            if start_time_str and end_time_str:
                try:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00')).astimezone(timezone.utc)
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00')).astimezone(timezone.utc)

                    current_utc = datetime.now(timezone.utc)
                    if end_time > current_utc + timedelta(minutes=5):
                         msg = f"Invalid time range: End time is too far in the future ({end_time.isoformat()})."
                         logging.error(msg)
                         return {"error": f"Bad Request: {msg}"}, 400
                    if start_time >= end_time:
                        msg = f"Invalid time range: Start time ({start_time.isoformat()}) is not before end time ({end_time.isoformat()})."
                        logging.error(msg)
                        return {"error": f"Bad Request: {msg}"}, 400

                    logging.info(f"Processing using requested time range: {start_time.isoformat()} to {end_time.isoformat()}")

                except ValueError as ve:
                    msg = f"Invalid datetime format in request body. Use ISO 8601 format (e.g., 'YYYY-MM-DDTHH:MM:SSZ'). Received: start='{start_time_str}', end='{end_time_str}'."
                    logging.error(f"{msg} Error: {ve}")
                    return {"error": f"Bad Request: {msg}"}, 400
            elif start_time_str or end_time_str:
                 msg = "Both 'start_time' and 'end_time' must be provided if specifying a time range."
                 logging.error(msg)
                 return {"error": f"Bad Request: {msg}"}, 400


        if start_time is None or end_time is None:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(seconds=DEFAULT_TIMEDELTA_SECONDS)
            logging.info(f"Processing using default time range: {start_time.isoformat()} to {end_time.isoformat()} (duration: {DEFAULT_TIMEDELTA_SECONDS}s).")


        response_message = f"Processing triggered for time range {start_time.isoformat()} to {end_time.isoformat()} with initial_backfill={is_initial_backfill_run}."

        if is_initial_backfill_run and run_summary_table_full_id and bq_client:
             try:
                 check_query = f"""
                 SELECT count(*) as count FROM `{run_summary_table_full_id}`
                 WHERE is_initial_backfill_run IS TRUE
                 LIMIT 1
                 """
                 check_result = bq_client.query(check_query).result()
                 row = next(check_result)
                 if row.count > 0:
                     msg = "Initial backfill run has already been recorded. Skipping this request."
                     logging.warning(msg)
                     return {"status": "skipped", "message": msg}, 200
             except Exception as e:
                 logging.error(f"Failed to check for existing initial backfill runs: {e}", exc_info=True)


        asyncio.run(main_logic(start_time, end_time, is_initial_backfill_run))

        return {"status": "success", "message": response_message}, 200

    except Exception as e:
        logging.exception("Unexpected error during request parsing or time range/flag setup.")
        return {"error": "Internal Server Error during request processing.", "details": str(e)}, 500
