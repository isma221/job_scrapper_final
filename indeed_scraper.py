"""
Indeed Job Scraper Module
========================

This module provides functionality to scrape job listings from Indeed.com.
It uses Selenium with undetected-chromedriver to fetch job data based on search criteria.

Key Features:
- Location-based search
- Job description extraction
- Rate limiting and error handling
- Data export to JSON and CSV
- Selenium-based scraping with anti-detection measures

Author: Isma
Date: 21-April-2025
Version: 1.0.0
"""

from typing import List, Dict, Any, Optional
import nodriver as uc
from bs4 import BeautifulSoup  # Add BeautifulSoup for HTML parsing
import json
import csv
from datetime import datetime
import asyncio
import time
import logging
import re
from selenium.common.exceptions import NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add this to prevent duplicate logs
logger.propagate = False

class IndeedScraper:
    """
    Scraper class for Indeed.com job listings.
    
    This class handles the scraping of job listings from Indeed.com using Selenium
    with undetected-chromedriver to avoid detection.
    
    Attributes:
        base_url (str): Base URL for Indeed.com
    """
    
    def __init__(self) -> None:
        """
        Initialize the scraper with browser settings.
        """
        self.base_url = "https://pk.indeed.com"
        logger.info("IndeedScraper initialized")

    async def get_job_details(self, browser, job_url: str) -> Optional[Dict[str, Any]]:
        """Modified to use nodriver instead of Selenium"""
        try:
            page = await browser.get(job_url)
            await asyncio.sleep(2)
            
            html = await page.get_content()
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract job description
            description = ""
            desc_div = soup.find("div", {"id": "jobDescriptionText"})
            if desc_div:
                description = desc_div.get_text(strip=True)
            
            # Extract job details
            details = {}
            job_details_div = soup.find("div", class_="jobsearch-JobInfoHeader")
            if job_details_div:
                company_info = job_details_div.find("div", class_="jobsearch-CompanyInfo")
                subtitle = job_details_div.find("div", class_="jobsearch-JobInfoHeader-subtitle")
                date_div = job_details_div.find("div", class_="jobsearch-JobInfoHeader-date")
                
                details = {
                    "company": company_info.get_text(strip=True) if company_info else "",
                    "location": subtitle.get_text(strip=True) if subtitle else "",
                    "posted_date": date_div.get_text(strip=True) if date_div else ""
                }
            
            return {
                "description": description,
                "details": details
            }
        except Exception as e:
            logger.error(f"Error fetching job details: {str(e)}")
            return None

    async def scrape_jobs(self, position: str, location: str, max_jobs: int = 10) -> List[Dict[str, Any]]:
        jobs = []
        processed_count = 0
        page_num = 0
        first_page_retried = False
        
        logger.info(f"\nStarting Indeed search with parameters:")
        logger.info(f"Position: {position}")
        logger.info(f"Location: {location}")
        
        search_url = f"{self.base_url}/jobs?q={position.replace(' ', '+')}&l={location.replace(' ', '+')}&sort=date"
        
        try:
            browser = await uc.start(
                headless=False,
                options=[
                    '--start-maximized',
                    '--disable-gpu',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            try:
                while processed_count < max_jobs and page_num < 3:
                    try:
                        current_url = f"{search_url}&start={page_num * 10}"
                        logger.info(f"\nAccessing page {page_num + 1}: {current_url}")
                        
                        page = await browser.get(current_url)
                        await asyncio.sleep(5)  # Increased wait time
                        
                        html = await page.get_content()
                        soup = BeautifulSoup(html, "html.parser")
                        
                        # Try multiple selectors for job cards
                        job_cards = (
                            soup.find_all("div", class_="job_seen_beacon") or
                            soup.find_all("div", class_="cardOutline") or
                            soup.find_all("div", {"data-tn-component": "organicJob"})
                        )
                        
                        logger.info(f"Found {len(job_cards)} job cards on page {page_num + 1}")
                        
                        # If we find jobs on second page and haven't retried first page yet
                        if page_num == 1 and len(job_cards) > 0 and not first_page_retried:
                            logger.info("\nFound jobs on second page. Retrying first page...")
                            page_num = 0  # Go back to first page
                            first_page_retried = True  # Mark that we've retried
                            await asyncio.sleep(3)  # Wait before retrying
                            continue
                        
                        for job in job_cards:
                            if processed_count >= max_jobs:
                                break
                            
                            try:
                                # Extract title
                                title_el = (
                                    job.select_one("h2.jobTitle") or
                                    job.select_one("a.jobtitle") or
                                    job.select_one("[data-testid='jobTitle']")
                                )
                                title = title_el.get_text(strip=True) if title_el else None
                                
                                # Extract company
                                company_el = (
                                    job.select_one("span[data-testid='company-name']") or
                                    job.select_one(".company") or
                                    job.select_one("[data-testid='company']")
                                )
                                company = company_el.get_text(strip=True) if company_el else None
                                
                                # Extract location
                                location_el = (
                                    job.select_one("div[data-testid='text-location']") or
                                    job.select_one(".location") or
                                    job.select_one("[data-testid='location']")
                                )
                                location = location_el.get_text(strip=True) if location_el else None
                                
                                # Extract salary
                                salary_el = (
                                    job.select_one("div.metadata.salary-snippet-container") or
                                    job.select_one(".salaryText") or
                                    job.select_one("[data-testid='salary']")
                                )
                                salary = salary_el.get_text(strip=True) if salary_el else "Not Listed"
                                
                                # Get job link and description
                                link_el = (
                                    job.select_one("h2.jobTitle a") or
                                    job.select_one("a.jobtitle") or
                                    job.select_one("[data-testid='jobTitle'] a")
                                )
                                
                                if link_el and link_el.get('href'):
                                    full_link = self.base_url + link_el['href'] if link_el['href'].startswith('/') else link_el['href']
                                    description = await self.get_job_description(browser, full_link)
                                else:
                                    full_link = "No link available"
                                    description = "No description available"
                                
                                logger.info(f"\nProcessing job {processed_count + 1}:")
                                logger.info(f"Title: {title}")
                                logger.info(f"Company: {company}")
                                logger.info(f"Location: {location}")
                                
                                if title and company:
                                    job_data = {
                                        "title": title,
                                        "company": company,
                                        "experience": self.extract_experience(description),
                                        "location": location or "Location not specified",
                                        "apply_link": full_link,
                                        "description": description,
                                        "salary": salary,
                                        "jobNature": "onsite"
                                    }
                                    
                                    jobs.append(job_data)
                                    processed_count += 1
                                    job_message = f"Job {processed_count} analyzed: {title} at {company}"
                                    logger.info(job_message)
                                    print(job_message)  # This will show directly in terminal
                                
                                await asyncio.sleep(1)  # Reduced delay between jobs
                                
                            except Exception as e:
                                logger.error(f"Error processing job card: {str(e)}")
                                continue
                        
                        if processed_count >= max_jobs:
                            break
                        
                        page_num += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing page {page_num + 1}: {str(e)}")
                        break
            
            except Exception as e:
                logger.error(f"Error scraping Indeed: {str(e)}")
                return []
        
        except Exception as e:
            logger.error(f"Error scraping Indeed: {str(e)}")
            return []
        
        logger.info(f"\nTotal jobs collected: {len(jobs)}")
        return jobs

    async def get_job_description(self, browser, full_link: str) -> str:
        """Helper method to get job description"""
        try:
            page = await browser.get(full_link)
            await asyncio.sleep(2)  # Reduced sleep time
            html = await page.get_content()
            soup = BeautifulSoup(html, "html.parser")
            description_el = soup.find("div", {"id": "jobDescriptionText"})
            return description_el.get_text(strip=True) if description_el else "No description available"
        except Exception as e:
            logger.error(f"Error fetching description: {str(e)}")
            return "Error fetching description"

    def extract_text(self, element, selectors, default=""):
        """Helper method to try multiple selectors for text extraction"""
        for by, selector in selectors:
            try:
                return element.find_element(by, selector).text.strip()
            except NoSuchElementException:
                continue
        return default

    def extract_experience(self, text: str) -> str:
        """Extract experience requirements from text using regex patterns"""
        experience_patterns = [
            r'(\d+[\+]?\s*(?:-\s*\d+)?)\s*(?:years?)?\s*(?:of)?\s*experience',
            r'experience[\s\:]+(\d+[\+]?\s*(?:-\s*\d+)?\s*years?)',
            r'(\d+[\+]?\s*(?:-\s*\d+)?)\s*years?[\s\-]*experience',
            r'minimum[\s\:]+(\d+[\+]?\s*(?:-\s*\d+)?)\s*years?'
        ]
        
        for pattern in experience_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return f"{matches[0]} years"
        
        return "Not specified"

async def scrape_indeed_sync(position: str, location: str, experience: str) -> List[Dict[str, Any]]:
    """
    Synchronous interface for the Indeed.com scraper.
    
    Args:
        position (str): Job position/title to search for
        location (str): Location to search in
        experience (str): Required experience level
        
    Returns:
        List[Dict[str, Any]]: List of job listings with details
    """
    scraper = IndeedScraper()
    jobs = await scraper.scrape_jobs(position, location)
    return jobs

def save_to_json(jobs: List[Dict[str, Any]], position: str) -> None:
    """
    Save job listings to a JSON file.
    
    Args:
        jobs (List[Dict[str, Any]]): List of job listings to save
        position (str): Job position used in filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"indeed_jobs_{position.replace(' ', '_')}_{timestamp}.json"
    
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
    filename = f"indeed_jobs_{position.replace(' ', '_')}_{timestamp}.csv"
    
    if jobs:
        fieldnames = jobs[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)
    
    logger.info(f"Saved jobs to {filename}")
