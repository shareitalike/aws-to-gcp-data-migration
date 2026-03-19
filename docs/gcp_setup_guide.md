# ☁️ GCP Free Tier Setup Guide

This guide explains how to set up your Google Cloud Platform (GCP) account and authenticate your local machine for Stage 3 of the migration project.

## Step 1: Create a GCP Project
1. Log in to the [Google Cloud Console](https://console.cloud.google.com/).
2. In the top-left corner, click the project drop-down menu and select **New Project**.
3. Name your project (e.g., `aws-gcp-migration-demo`) and click **Create**.
4. Once created, make sure your new project is selected in the top drop-down menu.
5. Note down your **Project ID** from the Project Info card on the dashboard (it usually has numbers at the end, like `aws-gcp-migration-demo-123456`).

## Step 2: Enable Billing (Free Tier)
GCP requires a billing account to create BigQuery datasets and GCS buckets, even if you stay strictly within the free tier.
1. In the left navigation menu, go to **Billing**.
2. Click **Link a billing account** and follow the prompts to set up your free trial / free tier account.

## Step 3: Install the Google Cloud CLI
If you don't already have it, install the `gcloud` CLI tools so you can authenticate from your terminal:
- **Windows / Mac / Linux Download:** [Install gcloud CLI](https://cloud.google.com/sdk/docs/install)

## Step 4: Authenticate Your Terminal
Once `gcloud` is installed, open your terminal (Powershell, Command Prompt, or bash) and run these exact three commands:

1. Log in with your Google account:
```powershell
gcloud auth login
```

2. Set your active project (Replace with your actual Project ID from Step 1):
```powershell
gcloud config set project your-actual-gcp-project-id
```

3. Ensure Terraform and Python applications can use your credentials:
```powershell
gcloud auth application-default login
```
*(This will open a browser window again to confirm granting Application Default Credentials).*

## Step 5: Configure Your Project `.env`
Open your `.env` file and fill in the GCP variables:

```env
# ─── GCP Settings ───
GCP_PROJECT_ID=your-actual-gcp-project-id
GCP_REGION=us-central1

# These buckets will be created automatically by Terraform. 
# GCP bucket names MUST be globally unique across ALL users.
GCS_BUCKET_RAW=yourname-migration-raw
GCS_BUCKET_VALIDATED=yourname-migration-validated
GCS_BUCKET_QUARANTINE=yourname-migration-quarantine
GCS_BUCKET_PROCESSED=yourname-migration-processed

# You can delete the GOOGLE_APPLICATION_CREDENTIALS line
# at the bottom of the file since we used the terminal login above!
```

## Step 6: Enable Required APIs
Before Terraform can build the infrastructure, you need to tell GCP you plan to use Storage and BigQuery. Run this command in your terminal:

```powershell
gcloud services enable storage.googleapis.com bigquery.googleapis.com
```

## Step 7: Run Terraform (Stage 3)
Your machine is now fully authenticated and ready to deploy infrastructure! Run these commands to build the GCS buckets and BigQuery tables:

```powershell
cd 03_gcs_ingestion/terraform
terraform init
terraform apply -var="project_id=your-actual-gcp-project-id"
cd ../..
```
