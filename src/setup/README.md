# SETUP.md: Effortless Setup of [`bigquery-sensei`](../../README.md) on Any Google Cloud Project ✨

Setting up **`bigquery-sensei`** is designed to be a quick and simple process, allowing you to deploy this powerful BigQuery analysis engine onto any of your Google Cloud Projects with just a few commands. This document guides you through the necessary steps to get **`bigquery-sensei`** up and running, and how to tear it down when needed.

---

### **Deployment Times**

> [!TIP]
> The **initialization** is estimated to take approximately **5 minutes**. For **`backfill`** operations, the time is variable and unpredictable. Script **destruction** typically takes about **2 minutes**.

--- 

## Prerequisites

Before you begin, ensure you have the following:

> [!IMPORTANT]
> - **A Google Cloud Platform Account**: You need an active GCP account.
> - **Billing Account**: A valid GCP Billing Account is required to link to the new project that will be created, or to the existing project where you plan to deploy.
> - **Permissions**: The user or service account executing the setup script (`init.sh`) needs sufficient permissions within Google Cloud *before* running the script.
>   * **The quickest path is often to use an account with the `roles/owner` or `roles/editor` role.** Apply this role at the Organization/Folder level if creating a new project, or on the target Project if using an existing one.
>   * **If these broad roles are not feasible, a custom IAM role must be created and assigned *beforehand*.** This custom role must include *all* the granular permissions required by the setup process.
>   * **The default compute service account** (`<project_number>-compute@developer.gserviceaccount.com`) **must** have `roles/editor` permission in the project of interest.

The following list details the granular permissions needed for creating a custom IAM role. If you are using an **Owner** or **Editor** role, you can likely **skip** this detailed list:

> [!WARNING]
> * Create new Google Cloud Projects (if not deploying to an existing one) (`resourcemanager.projects.create`).
>* Link a billing account to a project (`resourcemanager.projects.updateBillingInfo`).
>* Enable APIs on a project (`serviceusage.services.enable`).
>* Create and manage IAM roles and bindings (`resourcemanager.projects.setIamPolicy`).
>* Create and manage BigQuery datasets and tables (`bigquery.datasets.create`, `bigquery.tables.create`, etc.).
>* Create and manage Cloud Storage buckets and objects (`storage.buckets.create`, `storage.objects.create`, etc.).
>* Create and manage Cloud Functions (Gen 2) (`cloudfunctions.functions.create`, `cloudfunctions.functions.update`, etc.).
>* Create and manage Cloud Scheduler jobs (`cloudscheduler.jobs.create`, `cloudscheduler.jobs.update`, etc.).
>* Create and manage Cloud Tasks queues (`cloudtasks.queues.create`, `cloudtasks.queues.update`, etc.).
>* Create and manage Secret Manager secrets and versions (`secretmanager.secrets.create`, `secretmanager.secrets.addVersion`, etc.).
>* Get the default Compute Service Account email (`iam.serviceAccounts.get`).
>* Permission to run local-exec provisioners in Terraform, which requires the `gcloud` and `jq` command-line tools to be installed and authenticated locally.

---

## Setup Steps

### 1. Clone the Repository

First, clone the `bigquery-sensei` repository to your local machine.

```bash
git clone <repository_url>
cd bigquery-sensei
```

Replace `<repository_url>` with the actual URL of the `bigquery-sensei` GitHub repository.

#### Windows Users:
If you don't have `git` installed, download and install [Git for Windows](https://git-scm.com/download/win). This will provide you with Git Bash, which is recommended for running the setup scripts.

---

### 2. Install and Authenticate Google Cloud SDK (`gcloud`)

The setup scripts and Terraform's local-exec provisioners rely on the Google Cloud SDK.

- **Install gcloud**: Follow the official Google Cloud documentation to install the SDK for your operating system.
  - [Linux/macOS](https://cloud.google.com/sdk/docs/install)
  - [Windows](https://cloud.google.com/sdk/docs/install?windows)

- **Authenticate**: Initialize and authenticate the SDK. This will open a web browser for you to log in to your Google account.

```bash
gcloud init
```

- **Login**: Ensure you are logged in with an account that has the necessary permissions (as described in the Prerequisites section). This command sets up Application Default Credentials (ADC) used by the clients.

```bash
gcloud auth application-default login
```

---

### 3. Install jq

The `init.sh` script uses `jq` to parse and manipulate JSON data for Cloud Tasks payloads.

Install `jq` using your system's package manager or by downloading the executable:

#### Linux/macOS:

```bash
# For Debian/Ubuntu
sudo apt-get update && sudo apt-get install jq

# For macOS (using Homebrew)
brew install jq

# For CentOS/RHEL
sudo yum install jq
```

#### Windows:
Download the appropriate `jq.exe` from the official [jq releases page](https://jqlang.org/download/). Place the `jq.exe` file in a directory that is included in your system's PATH environment variable, or in the same directory where you will run the `init.sh` script.

If using Git Bash or WSL, you might be able to use the Linux/macOS instructions.

---

### 4. Install Terraform

The project uses Terraform to provision and manage the Google Cloud infrastructure.

- **Install Terraform**: Follow the official HashiCorp documentation to install Terraform for your operating system.
  - [Linux/macOS](https://developer.hashicorp.com/terraform/downloads)
  - [Windows](https://developer.hashicorp.com/terraform/install#windows)

- **Verify Installation**: After installation, verify that Terraform is installed correctly by opening a new terminal or command prompt and running:

```bash
terraform -v
```

---

### 5. Create and Configure the `.env` File

The `.env` file holds essential configuration variables for both the Terraform deployment and the Cloud Functions.

- **Copy the example file**: Navigate to the root directory of the cloned repository.

```bash
cp .env.example .env
```

- **Edit the `.env` file**: Open the newly created `.env` file in a text editor (like VS Code, Notepad++, or even Notepad) and fill in the required values. 

> [!CAUTION]
> It's crucial never to commit or push this file.

### 6. Run the Initialization Script (`init.sh`)

The `init.sh` script automates the Terraform deployment process. Navigate to the root directory of the cloned repository if you are not already there.

```bash
cd bigquery-sensei && cd src/setup
```

Run the script with the desired options:

```bash
./init.sh [options]
```

#### Windows Users:
These are Bash scripts. You should run them using Git Bash or Windows Subsystem for Linux (WSL). Navigate to the repository directory in your chosen environment and execute the command as shown above.

---

**`init.sh` Options:**

- `--simulate-queries`:  
  **Purpose**: Deploys the query-simulator Cloud Function and configures a one-off Cloud Task to trigger it immediately after deployment. This is useful for generating some initial BigQuery job logs if you don't have recent activity in a test project.  
  **Usage**:  
  ```bash
  ./init.sh --simulate-queries
  ```

- `--run-initial-backfill [<start_time_iso8601> [<end_time_iso8601>]]`:  
  **Purpose**: Deploys a one-off Cloud Task to trigger the `table-stats-builder` Cloud Function for a historical time window. This allows you to analyze past BigQuery job data.  
  **Arguments**:  
    - `<start_time_iso8601>`: (Optional) The explicit start time for the backfill window in ISO 8601 format (e.g., `2023-01-01T00:00:00Z`).
    - `<end_time_iso8601>`: (Optional) The explicit end time for the backfill window in ISO 8601 format (e.g., `2024-01-01T00:00:00Z`).
  
  **Usage Scenarios**:
  
  ```bash
  ./init.sh --run-initial-backfill
  ```
  (Calculates a default backfill window from 1 year ago to now minus `TIMEDELTA_CONSIDERED_SEC`.)
  
  ```bash
  ./init.sh --run-initial-backfill 2023-01-01T00:00:00Z
  ```
  (Uses the provided start time; the end time is calculated as now minus `TIMEDELTA_CONSIDERED_SEC`.)
  
  ```bash
  ./init.sh --run-initial-backfill 2023-01-01T00:00:00Z 2024-01-01T00:00:00Z
  ```
  (Uses the explicitly provided start and end times.)

- `--help`:  
  **Purpose**: Displays the usage information for the script.  
  **Usage**:  
  ```bash
  ./init.sh --help
  ```
  (Must be used exclusively, no other options.)

---

**Example**: To deploy the infrastructure and trigger an initial backfill for the last 6 months:

```bash
end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
start_time=$(date -u -d "6 months ago" +"%Y-%m-%dT%H:%M:%SZ")
./init.sh --run-initial-backfill "$start_time" "$end_time"
```

The script will:
- Load variables from `.env`.
- Validate configuration.
- Zip the Cloud Function source code.
- Generate the `terraform.tfvars` file.
- Run `terraform init -upgrade`.
- Attempt to import the project into Terraform state if it already exists in GCP but not in state.
- Run `terraform apply -auto-approve` to create/update all resources.
- If `--simulate-queries` or `--run-initial-backfill` were used, it will create the respective Cloud Task(s) via `gcloud`.

Monitor the script output for any errors during the Terraform apply phase.

---

### 7. Run the Destruction Script (`destroy.sh`)

The `destroy.sh` script automates the process of tearing down the Google Cloud infrastructure provisioned by `init.sh`. Use this script with extreme caution, as it will delete your GCP resources and potentially incur costs.

Navigate to the root directory of the cloned repository if you are not already there.

```bash
cd bigquery-sensei && cd src/setup
```

Run the script with the desired options:

```bash
./destroy.sh [options]
```

#### Windows Users:
As with `init.sh`, run this script using Git Bash or WSL.

**`destroy.sh` Options:**

- `--delete-zips <true|false>`:  
  **Purpose**: Controls whether to delete the local Cloud Functions source zip files (`source.zip` in the function directories) after the destroy operation.  
  **Default**: `true`  
  **Usage**:  
  ```bash
  ./destroy.sh --delete-zips false
  ```

- `--delete-tf-files <true|false>`:  
  **Purpose**: Controls whether to delete local Terraform state files (`terraform.tfstate`, `.terraform` directory, etc.) and configuration files (like `terraform.tfvars`) from the `terraform` directory after the destroy operation. This will remove ALL files and directories inside the `terraform` folder, EXCEPT for `main.tf`. Use with extreme caution.  
  **Default**: `false`  
  **Usage**:  
  ```bash
  ./destroy.sh --delete-tf-files true
  ```

- `--help`:  
  **Purpose**: Displays the usage information for the script.  
  **Usage**:  
  ```bash
  ./destroy.sh --help
  ```
  (Must be used exclusively, no other options.)

---

**Example**: To destroy the infrastructure and delete local zip files, but keep Terraform state/config:

```bash
./destroy.sh --delete-zips true --delete-tf-files false
```

The script will:
- Load the `PROJECT_ID` from `.env`.
- Run `terraform destroy -var-file="terraform.tfvars" -auto-approve` to delete resources.

**Optional**: You can modify the flags to control whether to delete additional files and Terraform configuration.