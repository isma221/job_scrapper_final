"""
LinkedIn Job Scraper Module
=========================

This module provides functionality to scrape job listings from LinkedIn.
It uses the linkedin-jobs-scraper package to fetch job data based on search criteria.

Key Features:
- Job search with multiple filters
- Experience level filtering
- Location-based search
- Rate limiting and error handling
- Data export to JSON and CSV

Author: Isma
Date: 21-April-2025
Version: 1.0.0
"""

from typing import List, Dict, Any
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters
import logging
import time
import traceback
import json
import csv
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.propagate = False
def scrape_linkedin_sync(position: str, location: str, experience: str) -> List[Dict[str, Any]]:
    """
    Synchronously scrape jobs from LinkedIn based on search criteria.
    
    Args:
        position (str): Job position/title to search for
        location (str): Location to search in
        experience (str): Required experience level
        
    Returns:
        List[Dict[str, Any]]: List of job listings with details
        
    Raises:
        Exception: If scraping fails
    """
    jobs = []
    processed_count = 0
    max_jobs = 10
    
    def on_data(data: EventData) -> None:
        """
        Callback function to process each job listing.
        
        Args:
            data (EventData): Job listing data from LinkedIn
        """
        nonlocal processed_count
        if processed_count < max_jobs:
            try:
                # Map experience level to LinkedIn filters
                exp_level = ExperienceLevelFilters.ENTRY_LEVEL
                if "2" in experience or "two" in experience.lower():
                    exp_level = ExperienceLevelFilters.ASSOCIATE
                elif "5" in experience or "five" in experience.lower():
                    exp_level = ExperienceLevelFilters.MID_SENIOR
                
                job_data = {
                    "title": data.title,
                    "company": data.company,
                    "experience": data.insights or experience,
                    "location": data.location,
                    "apply_link": data.link,
                    "description": data.description,
                    "salary": "",  # LinkedIn doesn't always provide salary
                    "jobNature": "onsite"  # Default value
                }
                
                logger.info(f"Processing job {processed_count + 1}: {job_data['title']}")
                jobs.append(job_data)
                processed_count += 1
                time.sleep(5)  # Rate limiting
            except Exception as e:
                logger.error(f"Error processing job data: {str(e)}")
                logger.error(f"Error details: {traceback.format_exc()}")
    
    # Initialize scraper with appropriate settings
    scraper = LinkedinScraper(
        chrome_executable_path=None,
        chrome_binary_location=None,
        chrome_options=None,
        headless=True,
        max_workers=1,
        slow_mo=5.0,
        page_load_timeout=60
    )
    
    # Set up event handlers
    scraper.on(Events.DATA, on_data)
    scraper.on(Events.ERROR, lambda error: logger.error(f'[ON_ERROR] {error}'))
    scraper.on(Events.END, lambda: logger.info(f'[ON_END] {len(jobs)} jobs processed'))
    
    # Configure search query
    queries = [
        Query(
            query=position,
            options=QueryOptions(
                locations=[location],
                limit=max_jobs,
                filters=QueryFilters(                    
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME],
                    experience=[ExperienceLevelFilters.ENTRY_LEVEL, 
                              ExperienceLevelFilters.ASSOCIATE]
                )
            )
        ),
    ]
    
    try:
        scraper.run(queries)
        return jobs
    except Exception as e:
        logger.error(f"Error in scraper.run: {str(e)}")
        logger.error(f"Error details: {traceback.format_exc()}")
        return []

def save_to_json(jobs: List[Dict[str, Any]], position: str) -> None:
    """
    Save job listings to a JSON file.
    
    Args:
        jobs (List[Dict[str, Any]]): List of job listings to save
        position (str): Job position used in filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_{position.replace(' ', '_')}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved jobs to {filename}")

def save_to_csv(jobs: List[Dict[str, Any]], position: str) -> None:
    """
    Save job listings to a CSV file.
    
    Args:
        jobs (List[Dict[str, Any]]): List of job listings to save
        position (str): Job position used in filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_{position.replace(' ', '_')}_{timestamp}.csv"
    
    if jobs:
        fieldnames = jobs[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
    
    logger.info(f"Saved jobs to {filename}")