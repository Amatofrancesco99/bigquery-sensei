import os
import json
import logging
from typing import List, Dict, Optional, Any, TypedDict, Tuple
from google.cloud import bigquery
from datetime import datetime, timezone
from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langgraph.graph import StateGraph, END
from pydantic import BaseModel, Field
from langchain_core.messages import AIMessage, HumanMessage

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def load_agent_config(config_path: str) -> Dict[str, Any]:
    """Loads, validates, and returns the agent configuration from a JSON file.

    This function orchestrates the reading and parsing of the agent's
    configuration specified in a JSON file. It ensures the file exists and
    is valid JSON.

    Args:
        config_path: The absolute or relative file path to the JSON
                     configuration file.

    Returns:
        A dictionary representing the parsed JSON configuration.

    Raises:
        ValueError: If the provided config_path is empty or None.
        FileNotFoundError: If the configuration file cannot be found at the
                           specified path.
        json.JSONDecodeError: If the file content is not valid JSON.
        Exception: For any other unexpected errors during file reading or parsing.
    """
    if not config_path:
        logging.error("Configuration path argument is missing.")
        raise ValueError("Agent configuration file path is not provided.")
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found at: {config_path}")
        raise FileNotFoundError(f"Agent configuration file not found: {config_path}")

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            logging.info(f"Agent configuration loaded successfully from {config_path}")
            return config
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from {config_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred loading configuration from {config_path}: {e}")
        raise


class AnalysisScores(BaseModel):
    """Defines the structure for detailed query analysis scores.

    Specifies various metrics for evaluating BigQuery queries, each scored
    on a scale of 0 to 10. Scores can be null if a particular metric is
    not applicable to the analyzed query.

    Attributes:
        security: Overall security posture score.
        readability: Code formatting and ease of understanding score.
        cost_efficiency: Score based on minimizing billed bytes.
        scalability: Ability to handle increasing data volumes score.
        performance: Execution speed and resource usage efficiency score.
        complexity: Query complexity score (lower indicates higher complexity).
        modularity: Potential for reuse (e.g., via UDFs/views) score.
        maintainability: Ease of future modifications score.
        commenting: Presence and quality of code comments score.
        data_volume_efficiency: Score for processing only necessary data amounts.
        partition_pruning: Effectiveness of table partitioning utilization score.
        sensitive_data_exposure: Risk of exposing PII/sensitive data score (lower indicates higher risk).
        null_handling: Appropriateness of NULL value management score.
        joins_quality: Efficiency and correctness of JOIN operations score.
    """
    security: Optional[float] = Field(None, description="Score 0-10 for overall security.")
    readability: Optional[float] = Field(None, description="Score 0-10 for code readability/formatting.")
    cost_efficiency: Optional[float] = Field(None, description="Score 0-10 for minimizing bytes billed.")
    scalability: Optional[float] = Field(None, description="Score 0-10 for handling larger data volumes.")
    performance: Optional[float] = Field(None, description="Score 0-10 for execution speed/efficiency.")
    complexity: Optional[float] = Field(None, description="Score 0-10 for query complexity (lower score means more complex).")
    modularity: Optional[float] = Field(None, description="Score 0-10 for potential reuse/modularity (e.g., using UDFs/views).")
    maintainability: Optional[float] = Field(None, description="Score 0-10 for ease of future maintenance.")
    commenting: Optional[float] = Field(None, description="Score 0-10 for presence and quality of comments.")
    data_volume_efficiency: Optional[float] = Field(None, description="Score 0-10 for processing only necessary data.")
    partition_pruning: Optional[float] = Field(None, description="Score 0-10 for effective use of table partitioning.")
    sensitive_data_exposure: Optional[float] = Field(None, description="Score 0-10 for avoiding PII/sensitive data (lower score means more exposure).")
    null_handling: Optional[float] = Field(None, description="Score 0-10 for appropriate handling of NULL values.")
    joins_quality: Optional[float] = Field(None, description="Score 0-10 for efficiency and correctness of JOINs.")


class AnalysisOutput(BaseModel):
    """Specifies the structured JSON output format for the analysis agent.

    Ensures the language model generates responses conforming to a predictable
    and parseable schema, including detailed scores, actionable recommendations,
    risk assessment, safety check, and confidence level.

    Attributes:
        score: An AnalysisScores object containing detailed metric scores.
        recommendations: A list of textual recommendations for improvement.
        risk_level: A classification of the overall query risk ('Low', 'Medium', 'High').
        is_safe_to_run: A boolean flag indicating if the query is deemed safe.
        score_confidence: A float between 0 and 1 representing the agent's
                          confidence in its analysis.
    """
    score: AnalysisScores = Field(..., description="Detailed scores for various aspects (0-10, null if N/A).")
    recommendations: List[str] = Field(..., description="List of recommendations for improving the query or job configuration.")
    risk_level: str = Field(..., description="Overall risk level associated with the query (cost, security, performance).", pattern="^(Low|Medium|High)$")
    is_safe_to_run: bool = Field(..., description="Boolean indicating if the query is considered safe based on analysis.")
    score_confidence: float = Field(..., description="Confidence level (0-1) in the provided scores and analysis.", ge=0, le=1)


class GraphState(TypedDict):
    """Defines the dictionary structure for the state passed between LangGraph nodes.

    Attributes:
        llm_input: Data specifically prepared as input for the language model.
        original_row: The complete original BigQuery job row for context.
        llm_raw_output: The parsed output from the language model, expected to
                        be an AnalysisOutput object or None on failure.
        formatted_output: The final dictionary formatted for BigQuery insertion,
                          derived from llm_raw_output and original_row, or None.
    """
    llm_input: Dict[str, Any]
    original_row: Dict[str, Any]
    llm_raw_output: Optional[AnalysisOutput]
    formatted_output: Optional[Dict[str, Any]]


def build_resources_and_agent(
    project_id: str,
    location: str,
    llm_model_name: str,
    agent_config_path: str,
    results_dataset_id: str,
    results_table_id: str
) -> Tuple[bigquery.Client, Any, str]:
    """Initializes clients, loads config, builds, and compiles the LangGraph agent.

    This comprehensive function orchestrates the entire setup process required
    before the agent can be used. It handles resource initialization (BigQuery client),
    configuration loading, agent component definition (LLM, prompt, nodes),
    and graph compilation.

    Args:
        project_id: Google Cloud project ID.
        location: Google Cloud location (region) for Vertex AI and other resources.
        llm_model_name: The name or identifier of the Vertex AI language model to use.
        agent_config_path: Filesystem path to the agent's JSON configuration file.
        results_dataset_id: BigQuery dataset ID for storing analysis results.
        results_table_id: BigQuery table ID for storing analysis results.

    Returns:
        A tuple containing:
            - The initialized BigQuery client instance.
            - The compiled LangGraph agent instance.
            - The fully qualified target BigQuery table ID (project.dataset.table).

    Raises:
        ValueError: If any required input arguments are missing or invalid.
        FileNotFoundError: If the agent configuration file is not found.
        Exception: For any errors encountered during client initialization,
                   LLM setup, or graph compilation.
    """
    logging.info("Starting resource and agent build process...")

    if not all([project_id, location, llm_model_name, agent_config_path, results_dataset_id, results_table_id]):
        missing = [arg for arg, val in locals().items() if not val]
        raise ValueError(f"Missing required arguments for building agent: {missing}")

    try:
        logging.info("Initializing BigQuery Client...")
        local_bq_client = bigquery.Client(project=project_id)
        logging.info(f"BigQuery Client initialized for project {project_id}.")

        local_target_table_id = f"{project_id}.{results_dataset_id}.{results_table_id}"
        logging.info(f"Target table ID set to: {local_target_table_id}")

        logging.info("Loading agent configuration...")
        agent_config = load_agent_config(agent_config_path)

        logging.info(f"Initializing ChatVertexAI LLM with model: {llm_model_name}")
        llm = ChatVertexAI(
            project=project_id,
            location=location,
            model_name=llm_model_name,
            temperature=agent_config.get("model_parameters", {}).get("temperature", 0.0),
        )
        logging.info("ChatVertexAI LLM initialized.")

        logging.info("Defining prompt template...")
        instructions = agent_config.get("instructions", "Analyze BigQuery job data.")
        output_schema_description = AnalysisOutput.schema_json(indent=2)
        example_messages = []
        if 'examples' in agent_config and agent_config['examples']:
            logging.info(f"Processing {len(agent_config['examples'])} examples for prompt.")
            for example in agent_config['examples']:
                if 'input' in example and 'output' in example:
                    example_messages.append(HumanMessage(content=f"Input: {json.dumps(example['input'], indent=2)}"))
                    example_messages.append(AIMessage(content=json.dumps(example['output'], indent=2)))
                else:
                    logging.warning(f"Skipping malformed example during prompt setup: {example}")
        prompt = ChatPromptTemplate.from_messages([
            ("system", instructions + "\n\nOutput MUST be valid JSON matching:\n{schema}"),
            *example_messages,
            ("human", "Analyze this BigQuery job data:\n{input_data}")
        ]).partial(schema=output_schema_description)
        logging.info("Prompt template defined.")

        logging.info("Defining LLM chain with JSON output parser...")
        llm_chain = prompt | llm | JsonOutputParser(pydantic_object=AnalysisOutput)
        logging.info("LLM chain defined.")

        logging.info("Defining LangGraph nodes...")
        def call_llm_node(state: GraphState) -> Dict[str, Any]:
            """Invokes the LLM chain using input from the current graph state."""
            job_id = state['original_row'].get('job_id', 'N/A')
            logging.debug(f"[Node: call_llm] - Processing job ID: {job_id}")
            try:
                llm_output = llm_chain.invoke({"input_data": json.dumps(state['llm_input'], indent=2)})
                return {"llm_raw_output": llm_output}
            except Exception as node_e:
                logging.error(f"[Node: call_llm] - LLM invocation error for job {job_id}: {node_e}", exc_info=True)
                return {"llm_raw_output": None}

        def format_output_node(state: GraphState) -> Dict[str, Any]:
            """Transforms raw LLM output into the final BigQuery row format,
            handling either AnalysisOutput object or a dictionary output.
            """
            job_id = state['original_row'].get('job_id', None)
            logging.debug(f"[Node: format_output] - Processing job ID: {job_id}")
            llm_analysis = state.get('llm_raw_output')
            original_row = state['original_row']

            score_data_source = None
            risk_level = None
            is_safe_to_run = None
            score_confidence = None
            recommendations: List[str] = []

            try:
                if isinstance(llm_analysis, AnalysisOutput):
                    logging.debug(f"[Node: format_output] - Processing AnalysisOutput instance for job ID: {job_id}")
                    score_data_source = llm_analysis.score.dict()
                    risk_level = llm_analysis.risk_level
                    is_safe_to_run = llm_analysis.is_safe_to_run
                    score_confidence = llm_analysis.score_confidence
                    recommendations = llm_analysis.recommendations or []
                elif isinstance(llm_analysis, dict):
                    logging.warning(f"[Node: format_output] - Received dict instead of AnalysisOutput for job {job_id}. Attempting to extract data from dict.")
                    score_data_source = llm_analysis.get('score')
                    risk_level = llm_analysis.get('risk_level')
                    is_safe_to_run = llm_analysis.get('is_safe_to_run')
                    score_confidence = llm_analysis.get('score_confidence')
                    recommendations = llm_analysis.get('recommendations', [])

                    if not isinstance(score_data_source, dict):
                        logging.warning(f"[Node: format_output] - Value under 'score' key in dict is not a dictionary for job {job_id}. Cannot process scores from dict.")
                        score_data_source = None
                else:
                    logging.warning(f"[Node: format_output] - Invalid or missing LLM analysis for job {job_id}. Type: {type(llm_analysis)} not accepted. Skipping.")
                    return {"formatted_output": None}

                if score_data_source is None:
                    logging.warning(f"[Node: format_output] - No valid score data source found after type checking for job {job_id}. Skipping.")
                    return {"formatted_output": None}

                formatted_score = {k: (v / 10.0 if isinstance(v, (int, float)) else None) for k, v in score_data_source.items()}
                valid_scores = [s for s in formatted_score.values() if s is not None]
                avg_score = sum(valid_scores) / len(valid_scores) if valid_scores else None
                result_dict = {
                    "analysis_timestamp": datetime.now(timezone.utc),
                    "job_id": original_row.get("job_id"),
                    "agent_name": "langgraph_analyzer",
                    "llm_version": llm_model_name,
                    "query": original_row.get("query"),
                    "statement_type": original_row.get("statement_type"),
                    "average_score": avg_score,
                    "score": formatted_score,
                    "recommendations": [str(r) for r in recommendations or []],
                    "risk_level": risk_level, 
                    "is_safe_to_run": is_safe_to_run, 
                    "score_confidence": score_confidence
                }
                logging.debug(f"[Node: format_output] - Successfully formatted output for job {job_id}")
                return {"formatted_output": result_dict}

            except Exception as node_e:
                logging.error(f"[Node: format_output] - Error processing analysis data or formatting output for job {job_id}: {node_e}", exc_info=True)
                return {"formatted_output": None}
        logging.info("LangGraph nodes defined.")

        logging.info("Defining and compiling LangGraph workflow...")
        workflow = StateGraph(GraphState)
        workflow.add_node("call_llm", call_llm_node)
        workflow.add_node("format_output", format_output_node)
        workflow.set_entry_point("call_llm")
        workflow.add_edge("call_llm", "format_output")
        workflow.add_edge("format_output", END)
        compiled_agent = workflow.compile()
        logging.info("LangGraph workflow compiled successfully.")

        logging.info("Resource and agent build process completed.")
        return local_bq_client, compiled_agent, local_target_table_id

    except Exception as e:
        logging.critical(f"Fatal error during resource and agent build: {e}", exc_info=True)
        raise 