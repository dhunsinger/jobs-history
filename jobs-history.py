import streamsets
from streamsets.sdk import ControlHub
from datetime import datetime, timedelta
import csv

# Print the StreamSets SDK version
print(streamsets.sdk.__version__)

# Connect to the StreamSets DataOps Platform.
sch = ControlHub(credential_id=<credential ID>, token=<token>)

# Function to get the metrics attributes
def get_metrics_attributes(metrics):
    attributes = {
        'error_count': metrics.error_count,
        'error_records_per_sec': metrics.error_records_per_sec,
        'input_count': metrics.input_count,
        'input_records_per_sec': metrics.input_records_per_sec,
        'output_count': metrics.output_count,
        'output_records_per_sec': metrics.output_records_per_sec,
        'pipeline_version': metrics.pipeline_version,
        'run_count': metrics.run_count,
        'sdc_id': metrics.sdc_id,
        'stage_errors_count': metrics.stage_errors_count,
        'stage_error_records_per_sec': metrics.stage_error_records_per_sec,
        'total_error_count': metrics.total_error_count
    }
    return attributes

# Function to get job information
def get_job_info(job, start_date, end_date):
    full_job_info = str(job)
    Job_ID = full_job_info.split("=")[1].split(",")[0]
    Job_Name = full_job_info.split("=")[2].split(")")[0]
    individual_job = sch.jobs.get(job_id=Job_ID)

    try:
        history = individual_job.history.get()
        time_series = str(history).split(',')
        dates_list = []

        for dates in time_series[1:3]:
            date = int(dates.split("=")[1]) / 1000
            if date == 0:
                dates_list.append("Unknown")
            else:
                converted_date = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
                dates_list.append(converted_date)

        start_format, end_format = dates_list

        if start_format != "Unknown" and end_format != "Unknown":
            job_start_date = datetime.strptime(start_format, '%Y-%m-%d %H:%M:%S')
            job_end_date = datetime.strptime(end_format, '%Y-%m-%d %H:%M:%S')
            if job_start_date < start_date or job_end_date > end_date:
                return None

    except:
        start_format, end_format = "Unknown", "Unknown"

    try:
        metrics = individual_job.metrics.get()
        input_count = metrics.input_count
        output_count = metrics.output_count
        error_count = metrics.error_count
        run_count = metrics.run_count
    except:
        run_count = "Unknown"
        input_count = "Unknown"
        output_count = "Unknown"
        error_count = "Unknown"

    #Calculate duration
    if start_format != "Unknown" and end_format != "Unknown":
        duration_seconds = (job_end_date - job_start_date).total_seconds()
        minutes, seconds = divmod(duration_seconds, 60)
        duration = f"{int(minutes):02d}:{int(seconds):02d}"
    else:
        duration = "Unknown"

    return [Job_ID, Job_Name, start_format, end_format, duration, run_count, input_count, output_count, error_count]

# Get user input for start and end dates
start_input = input("Enter start date (YYYY-MM-DD): ")
end_input = input("Enter end date (YYYY-MM-DD): ")
output_destination = input("Enter the destination for the output CSV (e.g., output.csv): ")

# Convert input dates to datetime objects
start_date = datetime.strptime(start_input, "%Y-%m-%d")
end_date = datetime.strptime(end_input, "%Y-%m-%d") + timedelta(days=1)

# Get all jobs from Control Hub
Jobs = sch.jobs.get_all()

# Generate a list of job information for jobs that meet the date criteria
full_list = [get_job_info(job, start_date, end_date) for job in Jobs]
full_list = [job for job in full_list if job is not None]

print(full_list)

# Write the job information to a CSV file
with open(output_destination, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Job ID', 'Job Name', 'Start Date', 'End Date', 'Duration', 'Run Count', 'Input Count', 'Output Count', 'Error Count'])
    for row in full_list:
        csv_writer.writerow(row)
