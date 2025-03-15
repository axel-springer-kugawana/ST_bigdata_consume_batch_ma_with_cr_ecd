import time
from datetime import datetime, timedelta

import boto3

# AWS Glue client
session = boto3.session.Session(profile_name="iwt-bigdata-live")
glue_client = session.client("glue", region_name="eu-central-1")

# Glue job name
GLUE_JOB_NAME = "consume-batch-ma-with-cr-ecd-live"

# Start and end date
start_date = datetime(2025, 3, 6)  # Start date
end_date = datetime.today()  # Today's date

# Iterate day by day
partition_date = start_date

while partition_date <= end_date:
    time.sleep(90)  # Wait 90 seconds before another loop
    
    partition_date_str = partition_date.strftime("%Y-%m-%d")
    print(f"Starting job with partition_date: {partition_date_str}")

    # Start Glue job
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--partition_date": partition_date_str,
        },
    )

    job_run_id = response["JobRunId"]
    print(f"Job started with Run ID: {job_run_id}")

    # Wait for the job to complete
    while True:
        job_status = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        state = job_status["JobRun"]["JobRunState"]

        if state in ["SUCCEEDED", "FAILED", "STOPPED"]:
            print(f"Job {job_run_id} finished with status: {state}")
            break

        time.sleep(60)  # Wait 60 seconds before checking again

    if state != "SUCCEEDED":
        print(f"Job failed or stopped, exiting loop.")
        break  # Stop loop if the job fails

    # Move to the next day's partition_date
    partition_date += timedelta(days=1)

print("All jobs completed.")
