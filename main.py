import time
import random
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from selenium import webdriver
from time import sleep
import random
import os
import unicodedata
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import json
from selenium.webdriver.chrome.options import Options
import os
import requests
from enum import Enum
from typing import List, Optional, Any
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import math

session = requests.Session()

base_url = "https://blackstone.wd1.myworkdayjobs.com"
career_url = f"{base_url}/en-US/Blackstone_Careers"

# First request to get cookies
session.get(career_url)

jobs_url = f"{base_url}/wday/cxs/blackstone/Blackstone_Careers/jobs"

payload = {
    "appliedFacets": {},
    "limit": 1,
    "offset": 0,
    "searchText": ""
}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": base_url,
    "Referer": career_url,
}

response = session.post(jobs_url, json=payload, headers=headers)

total_jobs = response.json().get("total")
number_of_pages = math.ceil(total_jobs/20)
offsets = [0,20,40,60,80,100,120,140,160,180,200,220,240,260,280,300]
needed_offsets = offsets[:number_of_pages]

session = requests.Session()

base_url = "https://blackstone.wd1.myworkdayjobs.com"
career_url = f"{base_url}/en-US/Blackstone_Careers"

# First request to get cookies
session.get(career_url)

jobs_url = f"{base_url}/wday/cxs/blackstone/Blackstone_Careers/jobs"
job_urls = []

for offset in needed_offsets:
    payload = {
        "appliedFacets": {},
        "limit": 20,
        "offset": offset,
        "searchText": ""
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": base_url,
        "Referer": career_url,
    }

    response = session.post(jobs_url, json=payload, headers=headers)

    data = response.json()

    base_prefix = "https://blackstone.wd1.myworkdayjobs.com/en-US/Blackstone_Careers"

    for job in data.get("jobPostings", []):
        external_path = job.get("externalPath")
        bullet_fields = job.get("bulletFields", [])

        if external_path:
            full_url = base_prefix + external_path

            # Get second item in bulletFields if it exists
            bullet_value = bullet_fields[1] if len(bullet_fields) > 1 else None

            job_urls.append({
                "url": full_url,
                "bulletField": bullet_value
            })

print(f"Found {len(job_urls)} jobs")
df = pd.DataFrame(job_urls)
df = df.rename(columns={"bulletField": "division"})
job_urls = df["url"].tolist()

#------------------------CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# Query existing URLs from your BigQuery table
query = """
    SELECT url
    FROM `databasealfred.alfredFinance.blackstoneStudent`
    WHERE url IS NOT NULL
"""
query_job = client.query(query)

# Convert results to a set for fast lookup
existing_urls = {row.url for row in query_job}

print(f"Loaded {len(existing_urls)} URLs from BigQuery")

# Filter job_urls
job_urls = [url for url in job_urls if url not in existing_urls]

print(f"✅ Remaining job URLs to scrape: {len(job_urls)}")


#------------------------ FIN CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------


# Set up Selenium options (headless mode for efficiency)
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Initialize WebDriver
driver = webdriver.Chrome(options=options)

# Initialize an empty list to store job data
job_data = []


for job_url in job_urls:
    driver.get(job_url)

    def get_text(selector, multiple=False):
        """Helper function to extract text from an element."""
        try:
            if multiple:
                return [elem.text.strip() for elem in driver.find_elements(By.CSS_SELECTOR, selector)]
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "" if not multiple else []


    try:
        # Wait until the h2 is present
        title = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'h2[data-automation-id="jobPostingHeader"]')
            )
        )
    
        title = title.text.strip()

    except TimeoutException:
        job_title = ""
    
    print(title)

        
    try:
        # Wait until description container is present
        container = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'div[data-automation-id="jobPostingDescription"]')
            )
        )

        # Get all <p> inside the container
        paragraphs = container.find_elements(By.TAG_NAME, "p")

        # Extract and clean text
        description = "\n".join(
            p.text.strip() for p in paragraphs if p.text.strip()
        )

    except (NoSuchElementException, TimeoutException):
        description = ""


    try:
        location = get_text('[data-automation-id="locations"] dd')
        location = location.strip() if location else ""
    except NoSuchElementException:
        location = ""

    experienceLevel = ""
    scrappedDateTime = datetime.datetime.now().isoformat()
    scrappedDate = datetime.datetime.now().strftime("%Y-%m-%d")
    scrappedHour = datetime.datetime.now().strftime("%H")
    scrappedMinutes = datetime.datetime.now().strftime("%M")
    
    # Append extracted data to list
    job_data.append({
        "title": title,
        "location": location,
        "scrappedDateTime": scrappedDateTime,
        "description": description,
        #"division": division,
        "experienceLevel": experienceLevel,
        "url": job_url,
        "source":"Blackstone",
        "scrappedDate": scrappedDate,
        "scrappedHour": scrappedHour,
        "scrappedMinutes": scrappedMinutes,
        "scrappedDateTimeText": scrappedDateTime
    })

# Convert list to Pandas DataFrame
df_jobs = pd.DataFrame(job_data)

new_data = pd.merge(df_jobs, df, on = "url")

new_order = [
    'title',
    'location',
    'scrappedDateTime',
    'description',
    'division',
    'experienceLevel',
    'url',
    'source',
    'scrappedDate',
    'scrappedHour',
    'scrappedMinutes',
    'scrappedDateTimeText'
]

new_data = new_data[new_order]

import re
import numpy as np

def extract_experience_level(title):
    if pd.isna(title):
        return ""
    
    title = title.lower()

    patterns = [
        (r'\bsummer\s+analyst\b', "Summer Analyst"),
        (r'\bsummer\s+associate\b', "Summer Associate"),
        (r'\bvice\s+president\b|\bsvp\b|\bvp\b|\bprincipal\b', "Vice President"),
        (r'\bassistant\s+vice\s+president\b|\bsavp\b|\bavp\b', "Assistant Vice President"),
        (r'\bsenior\s+manager\b', "Senior Manager"),
        (r'\bproduct\s+manager\b|\bpm\b', "Product Manager"),
        (r'\bmanager\b', "Manager"),
        (r'\bengineer\b|\bengineering\b', "Engineer"),
        (r'\badministrative\s+assistant\b|\bexecutive\s+assistant\b|\badmin\b', "Assistant"),
        (r'\bassociate\b', "Associate"),
        (r'\banalyst\b', "Analyst"),
        (r'\bchief\b|\bhead\b', "C-Level"),
        (r'\bmarketing\b', "Marketing"),
        (r'\bsales\b', "Sales"),
    ]

    for pattern, label in patterns:
        if re.search(pattern, title):
            return label

    return ""  # or "" if you prefer empty

# Apply to dataframe
new_data["experienceLevel"] = new_data["title"].apply(extract_experience_level)

#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.alfredFinance.blackstoneStudent"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()
