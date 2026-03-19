# ☁️ AWS Free Tier Setup Guide

This guide explains how to get the AWS credentials needed for Stage 2 of the migration project (`upload_to_s3.py`).

## Step 1: Create an IAM User
1. Log in to the [AWS Management Console](https://console.aws.amazon.com/) with your free tier account.
2. In the search bar at the very top, type **IAM** and select the IAM service.
3. In the left navigation pane, click **Users** → **Add users**.
4. **User name**: `data-migration-demo` (or similar).
5. Click **Next**.

## Step 2: Grant S3 Permissions
1. On the "Set permissions" page, select **Attach policies directly**.
2. In the search box below it, type `AmazonS3FullAccess`.
3. Check the box next to **AmazonS3FullAccess** in the results.
4. Click **Next**, then **Create user**.

## Step 3: Generate Access Keys
1. Back on the **Users** list, click on the user you just created (`data-migration-demo`).
2. Go to the **Security credentials** tab.
3. Scroll down to the **Access keys** section and click **Create access key**.
4. Select **Command Line Interface (CLI)** as the use case.
5. Check the confirmation box at the bottom and click **Next** → **Create access key**.
6. **IMPORTANT**: Keep this tab open or download the `.csv` file. You will not be able to see the Secret Access Key again after closing this page!

## Step 4: Configure Your Project
1. In your project folder (`f:\pyspark_study\project_bigquery_auto`), copy the `.env.example` file to a new file named `.env`.
2. Open `.env` and fill in the AWS section with the keys you just generated:

```env
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Keep us-east-1 unless you strongly prefer another region
AWS_REGION=us-east-1

# This MUST be globally unique across all AWS users. Add a random number or your name!
S3_BUCKET_NAME=your-name-migration-raw-data-123
```

## Step 5: Run Stage 2
You don't need to manually create the S3 bucket! The `upload_to_s3.py` script will automatically create the bucket in your specified region if it doesn't already exist.

```powershell
python 02_aws_s3/upload_to_s3.py
```
