import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

# --- Dynamic Configuration (Falls back to defaults if env vars are unset) ---
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "adzuna-job-data-pipeline-2026")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")


def setup_production_s3():
    # Implicitly picks up AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    print(f"🚀 Provisioning Production S3 Bucket: '{BUCKET_NAME}' in '{AWS_REGION}'...")

    # -------------------------------------------------------------
    # 1. Create S3 Bucket
    # -------------------------------------------------------------
    try:
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print("✅ Bucket created successfully.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "BucketAlreadyOwnedByYou":
            print("ℹ️ Bucket already exists and is owned by your account. Updating configuration...")
        elif error_code == "BucketAlreadyExists":
            print(f"❌ Critical: Global bucket name '{BUCKET_NAME}' is already taken by another AWS user. Choose a unique name in .env.")
            return
        else:
            print(f"❌ Failed to create bucket: {e}")
            return

    # -------------------------------------------------------------
    # 2. Enable Public Access Block (Production Hardening)
    # -------------------------------------------------------------
    try:
        s3_client.put_public_access_block(
            Bucket=BUCKET_NAME,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        print("✅ Security: Blocked all public access.")
    except ClientError as e:
        print(f"❌ Failed to enforce Public Access Block: {e}")

    # -------------------------------------------------------------
    # 3. Enable Bucket Versioning
    # -------------------------------------------------------------
    try:
        s3_client.put_bucket_versioning(
            Bucket=BUCKET_NAME,
            VersioningConfiguration={"Status": "Enabled"},
        )
        print("✅ Bucket versioning enabled.")
    except ClientError as e:
        print(f"❌ Failed to enable versioning: {e}")

    # -------------------------------------------------------------
    # 4. Initialize Folder Structure (Prefixes)
    # -------------------------------------------------------------
    now = datetime.now(timezone.utc)
    raw_partition_prefix = f"raw/{now.strftime('%Y/%m/%d')}/"

    prefixes = [
        raw_partition_prefix,
        "processed/",
        "logs/",
    ]

    print("\n📂 Initializing production folder structure:")
    for prefix in prefixes:
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=f"{prefix}.keep",
                Body=b"",
            )
            print(f"  • Created prefix: {prefix}")
        except ClientError as e:
            print(f"  ❌ Failed to create prefix '{prefix}': {e}")

    print(f"\n✨ Production setup complete for '{BUCKET_NAME}'.")


def generate_raw_s3_key(filename: str) -> str:
    """Returns dynamic date-partitioned S3 key: raw/YYYY/MM/DD/filename"""
    now = datetime.now(timezone.utc)
    return f"raw/{now.strftime('%Y/%m/%d')}/{filename}"


if __name__ == "__main__":
    setup_production_s3()