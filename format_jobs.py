"""
This script formats scraped job data to match Supabase column structure.
It reads `scraped_jobs.json`, extracts necessary fields, and saves `formatted_jobs.json`.
"""

import json
import os

# Check if scraped_jobs.json exists
if not os.path.exists("scraped_jobs.json"):
    print("❌ Error: scraped_jobs.json not found!")
    exit()

# Load scraped data
try:
    with open("scraped_jobs.json", "r", encoding="utf-8") as file:
        scraped_data = json.load(file)
        print(f"✅ Loaded {len(scraped_data)} jobs.")
except json.JSONDecodeError:
    print("❌ Invalid JSON format.")
    exit()

# Format the data
formatted_data = []
for job in scraped_data:
    formatted_job = {
        "title": job.get("positionName", "Unknown Title"),
        "company": job.get("company", "Unknown Company"),
        "location": job.get("location", "Unknown Location"),
        "salary": job.get("salary", "Not Specified"),
        "description": job.get("description", "No description available"),
        "posted_date": job.get("postingDateParsed", ""),
        "job_url": job.get("externalApplyLink") or job.get("url", "No URL")
    }
    formatted_data.append(formatted_job)

print(f"✅ Formatted {len(formatted_data)} jobs.")

# Save formatted data
with open("formatted_jobs.json", "w", encoding="utf-8") as file:
    json.dump(formatted_data, file, indent=4)

print("✅ Formatted jobs saved to formatted_jobs.json")
