import os
import subprocess
import sys

def run_dbt():
    print("=== Starting dbt Transformations ===")
    
    # Absolute paths relative to repository root
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.join(base_dir, "analytics")
    profiles_dir = os.path.join(base_dir, "profiles")

    print(f"Executing dbt in: {dbt_dir}")
    print(f"Using profiles dir: {profiles_dir}")

    try:
        # 1. Run dbt models
        subprocess.run(
            ["dbt", "run", "--profiles-dir", profiles_dir],
            cwd=dbt_dir,
            check=True,
            text=True
        )
        print("=== dbt Run Completed Successfully ===")

        # 2. Run dbt data tests
        subprocess.run(
            ["dbt", "test", "--profiles-dir", profiles_dir],
            cwd=dbt_dir,
            check=True,
            text=True
        )
        print("=== dbt Test Completed Successfully ===")

    except subprocess.CalledProcessError as e:
        print(f"Error executing dbt: {e}")
        sys.exit(1)