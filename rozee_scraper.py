"""
Rozee.pk Job Scraper Module
========================

This module provides functionality to scrape job listings from Rozee.pk.
It uses Selenium with undetected-chromedriver to fetch job data based on search criteria.

Key Features:
- City code mapping for location-based search
- Job description extraction
- Rate limiting and error handling
- Data export to JSON and CSV
- Selenium-based scraping with anti-detection measures

Author: Isma
Date: 21-April-2025
Version: 1.0.0
"""

from typing import List, Dict, Any, Optional
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import csv
from datetime import datetime
import asyncio
import time
import logging
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
logger.propagate = False
class RozeeScraper:
    """
    Scraper class for Rozee.pk job listings.
    
    This class handles the scraping of job listings from Rozee.pk using Selenium
    with undetected-chromedriver to avoid detection.
    
    Attributes:
        cities (Dict[str, str]): Mapping of city names to Rozee.pk city codes
        chrome_options (uc.ChromeOptions): Chrome options for the browser
        base_url (str): Base URL for Rozee.pk
    """
    
    def __init__(self) -> None:
        """
        Initialize the scraper with city codes and browser settings.
        """
        # Load city codes from cities.json
        try:
            with open('cities.json', 'r') as f:
                self.cities = json.load(f)
                logger.info(f"Successfully loaded {len(self.cities)} cities from cities.json")
                logger.debug(f"First few cities: {list(self.cities.keys())[:5]}")
        except Exception as e:
            logger.error(f"Error loading cities.json: {str(e)}")
            self.cities = {}  # Initialize empty dict if file can't be loaded
        
        # Setup Chrome options
        self.chrome_options = uc.ChromeOptions()
        self.chrome_options.add_argument('--start-maximized')
        
        self.base_url = "https://www.rozee.pk"
        logger.info("RozeeScraper initialized")
        self.driver = None
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C by closing the browser and exiting"""
        logger.info("Ctrl+C detected. Closing browser and shutting down...")
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing browser: {str(e)}")
        sys.exit(0)

    def get_job_details(self, driver: uc.Chrome, job_url: str) -> Optional[Dict[str, Any]]:
        """
        Extract detailed job information from the job page.
        
        Args:
            driver (uc.Chrome): Selenium WebDriver instance
            job_url (str): URL of the job listing
            
        Returns:
            Optional[Dict[str, Any]]: Job details or None if extraction fails
        """
        try:
            driver.get(job_url)
            time.sleep(2)  # Add a small delay for page load
            
            # Extract job description
            description = ""
            try:
                desc_div = driver.find_element(By.CLASS_NAME, 'job-desc')
                description = desc_div.text.strip()
            except NoSuchElementException:
                try:
                    desc_div = driver.find_element(By.CLASS_NAME, 'description')
                    description = desc_div.text.strip()
                except NoSuchElementException:
                    pass
            
            # Extract job details
            details = {}
            try:
                job_details_div = driver.find_element(By.CLASS_NAME, 'job-details')
                rows = job_details_div.find_elements(By.CLASS_NAME, 'detail-row')
                for row in rows:
                    try:
                        key = row.find_element(By.CLASS_NAME, 'label').text.strip()
                        value = row.find_element(By.CLASS_NAME, 'value').text.strip()
                        details[key] = value
                    except NoSuchElementException:
                        continue
            except NoSuchElementException:
                pass
            
            return {
                "description": description,
                "details": details
            }
        except Exception as e:
            logger.error(f"Error fetching job details: {str(e)}")
            return None

    async def scrape_jobs(self, position: str, city_code: str, max_jobs: int = 10) -> List[dict]:
        """
        Scrape jobs from Rozee.pk with duplicate page detection
        
        Args:
            position (str): Job position to search for
            city_code (str): City code for location filtering
            max_jobs (int): Maximum number of jobs to scrape
            
        Returns:
            List[dict]: List of unique scraped jobs
        """
        jobs = []
        page = 1
        previous_page_jobs = set()  # Store job identifiers from previous page
        
        # Store driver instance in class for access by signal handler
        self.driver = uc.Chrome(options=self.chrome_options)
        search_url = f"{self.base_url}/job/jsearch/q/{position.replace(' ', '%20')}/fc/{city_code}"
        
        try:
            while len(jobs) < max_jobs:
                logger.info(f"Fetching page {page} from Rozee.pk...")
                current_url = f"{search_url}/p/{page}"
                self.driver.get(current_url)
                time.sleep(3)  # Wait for page load
                
                try:
                    # Updated selector to match the actual HTML structure
                    job_listings = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, 'job'))
                    )
                    
                    if not job_listings:
                        logger.info("No jobs found on this page")
                        break
                    
                    logger.info(f"Found {len(job_listings)} job listings on page {page}")
                    
                    current_page_jobs = set()  # Store job identifiers from current page
                    
                    for job in job_listings:
                        if len(jobs) >= max_jobs:
                            break
                            
                        try:
                            # Extract job details
                            title = job.find_element(By.CLASS_NAME, 's-18').find_element(By.TAG_NAME, 'a').text.strip()
                            company = job.find_element(By.CLASS_NAME, 'cname').text.strip()
                            location = job.find_element(By.CLASS_NAME, 'float-left').text.strip()
                            apply_link = job.find_element(By.CLASS_NAME, 's-18').find_element(By.TAG_NAME, 'a').get_attribute('href')
                            
                            # Updated experience extraction with multiple fallbacks
                            try:
                                experience = job.find_element(By.CLASS_NAME, 'func-area-drn').text.strip()
                            except NoSuchElementException:
                                try:
                                    experience = job.find_element(By.CLASS_NAME, 'experience').text.strip()
                                except NoSuchElementException:
                                    experience = "Not specified"  # Default value if experience is not found
                            
                            description = job.find_element(By.CLASS_NAME, 'jbody').text.strip()
                            
                            # Create a unique identifier for the job
                            job_identifier = f"{title}_{company}_{location}"
                            current_page_jobs.add(job_identifier)
                            
                            # Skip if we've already seen this job
                            if job_identifier in previous_page_jobs:
                                continue
                            
                            try:
                                salary = job.find_element(By.CSS_SELECTOR, 'span[data-original-title="Offer Salary - PKR"] span').text.strip()
                            except NoSuchElementException:
                                salary = "Not Listed"
                            
                            job_data = {
                                "title": title,
                                "company": company,
                                "location": location,
                                "experience": experience,
                                "apply_link": apply_link,
                                "description": description,
                                "salary": salary,
                                "jobNature": "onsite"
                            }
                            
                            logger.info(f"Processing job {len(jobs) + 1}: {title} at {company}")
                            jobs.append(job_data)
                            time.sleep(1)  # Small delay between jobs
                            
                        except Exception as e:
                            logger.error(f"Error processing job: {str(e)}")
                            continue
                    
                    # Check if current page has the same jobs as the previous page
                    if page > 1 and current_page_jobs.issubset(previous_page_jobs):
                        logger.info("Found duplicate jobs on next page, stopping search")
                        break
                    
                    # Update previous page jobs for next iteration
                    previous_page_jobs = current_page_jobs
                    page += 1
                    time.sleep(2)  # Delay between pages
                    
                except TimeoutException:
                    logger.warning("Timeout waiting for job listings")
                    break
                
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user. Cleaning up...")
            return jobs
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
            
        return jobs

async def scrape_rozee_sync(position: str, location: str, experience: str) -> List[dict]:
    """Synchronous interface for the scraper"""
    scraper = RozeeScraper()
    
    # Get city code from location with better debugging
    location_lower = location.lower()
    location_title = location.title()  # Convert to Title Case
    
    # Try different case variations
    city_code = scraper.cities.get(location) or \
                scraper.cities.get(location_lower) or \
                scraper.cities.get(location_title)
    
    logger.debug(f"Attempting to find city code:")
    logger.debug(f"Original location: '{location}'")
    logger.debug(f"Lowercase: '{location_lower}'")
    logger.debug(f"Title case: '{location_title}'")
    logger.debug(f"Available cities: {list(scraper.cities.keys())}")
    
    if city_code is None:
        logger.warning(f"City code not found for location: {location}. Using default code (1184) for Karachi")
        logger.warning(f"Cities in database: {list(scraper.cities.keys())}")
        city_code = "1184"  # Default to Karachi's code
    else:
        logger.info(f"Found city code {city_code} for location: {location}")
    
    jobs = await scraper.scrape_jobs(position, city_code)
    return jobs

def save_to_json(jobs: List[dict], position: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rozee_jobs_{position.replace(' ', '_')}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    print(f"Saved jobs to {filename}")

def save_to_csv(jobs: List[dict], position: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rozee_jobs_{position.replace(' ', '_')}_{timestamp}.csv"
    
    if jobs:
        fieldnames = jobs[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
    
    print(f"Saved jobs to {filename}")