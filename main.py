"""
Job Finder API
=============

A FastAPI-based job search API that aggregates and analyzes job listings from multiple sources.
This API implements job search functionality with LLM-powered relevance analysis.

Key Features:
- Multi-source job aggregation (LinkedIn, Indeed, Rozee.pk)
- LLM-based job relevance analysis
- Structured JSON response format
- Asynchronous job processing
- Error handling and logging

Author: Isma
Date: 21-April-2025
Version: 1.0.0
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import logging
import asyncio
import json
import os
from dotenv import load_dotenv
# Restore imports
from indeed_scraper import scrape_indeed_sync, save_to_json, save_to_csv
from linkedin_scraper import scrape_linkedin_sync
from datetime import datetime
from rozee_scraper import scrape_rozee_sync
from llm_analyzer import OllamaAnalyzer
import httpx
import signal
import sys

# Load environment variables
load_dotenv()

# Verify OLLAMA_API is set
if not os.getenv("OLLAMA_API"):
    print("Warning: OLLAMA_API environment variable not set")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Disable httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Job Finder API",
    description="API for searching and analyzing job listings from multiple sources",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

class JobRequest(BaseModel):
    """
    Request model for job search parameters.
    
    Attributes:
        position (str): The job position/title to search for
        experience (str): Required experience level
        salary (str): Expected salary range
        jobNature (str): Job nature (onsite/remote/hybrid)
        location (str): Job location
        skills (str): Required skills (comma-separated)
        sources (List[str]): List of job sources to search from
    """
    position: str = Field(..., description="Job position/title to search for")
    experience: str = Field(..., description="Required experience level")
    salary: str = Field(..., description="Expected salary range")
    jobNature: str = Field(..., description="Job nature (onsite/remote/hybrid)")
    location: str = Field(..., description="Job location")
    skills: str = Field(..., description="Required skills (comma-separated)")
    sources: List[str] = Field(
        default=["indeed", "linkedin", "rozee"],
        description="List of job sources to search from"
    )

class JobResponse(BaseModel):
    """
    Response model for job search results.
    
    Attributes:
        job_title (str): Title of the job
        company (str): Company name
        experience (str): Required experience
        jobNature (str): Job nature
        location (str): Job location
        salary (str): Salary information
        apply_link (str): URL to apply for the job
        description (str): Job description
        source (str): Source of the job listing
        relevance_score (float): LLM-generated relevance score
        matched_skills (List[str]): List of matched skills
    """
    job_title: str = Field(..., description="Title of the job")
    company: str = Field(..., description="Company name")
    experience: str = Field(..., description="Required experience")
    jobNature: str = Field(..., description="Job nature")
    location: str = Field(..., description="Job location")
    salary: str = Field(..., description="Salary information")
    apply_link: str = Field(..., description="URL to apply for the job")
    description: str = Field(..., description="Job description")
    source: str = Field(..., description="Source of the job listing")
    relevance_score: float = Field(..., description="LLM-generated relevance score")
    matched_skills: List[str] = Field(..., description="List of matched skills")

    class Config:
        json_encoders = {str: lambda v: v}
        schema_extra = {
            "example": {
                "job_title": "Software Engineer",
                "company": "XYZ Pvt Ltd",
                "experience": "2+ years",
                "jobNature": "onsite",
                "location": "Karachi, Pakistan",
                "salary": "100,000 PKR",
                "apply_link": "https://rozee.pk/job/123",
                "description": "Full job description here...",
                "source": "rozee",
                "relevance_score": 0.85,
                "matched_skills": ["Python", "FastAPI", "SQL"]
            }
        }

# Restore Indeed jobs function
async def get_indeed_jobs(request: JobRequest) -> List[Dict[str, Any]]:
    """
    Fetch jobs from Indeed based on search criteria.
    
    Args:
        request (JobRequest): Job search request parameters
        
    Returns:
        List[Dict[str, Any]]: List of job listings from Indeed
    """
    try:
        logger.info("Starting Indeed scraper...")
        jobs = await scrape_indeed_sync(
            request.position,
            request.location,
            request.experience
        )
        logger.info(f"Found {len(jobs)} jobs from Indeed")
        return [dict(**job, source="indeed") for job in jobs]
    except Exception as e:
        logger.error(f"Error in Indeed scraper: {str(e)}")
        return []

# Restore LinkedIn jobs function
async def get_linkedin_jobs(request: JobRequest) -> List[Dict[str, Any]]:
    """
    Fetch jobs from LinkedIn based on search criteria.
    
    Args:
        request (JobRequest): Job search request parameters
        
    Returns:
        List[Dict[str, Any]]: List of job listings from LinkedIn
    """
    try:
        logger.info("Starting LinkedIn scraper...")
        loop = asyncio.get_event_loop()
        jobs = await loop.run_in_executor(
            None,
            scrape_linkedin_sync,
            request.position,
            request.location,
            request.experience
        )
        logger.info(f"Found {len(jobs)} jobs from LinkedIn")
        return [dict(**job, source="linkedin") for job in jobs]
    except Exception as e:
        logger.error(f"Error in LinkedIn scraper: {str(e)}")
        return []

async def get_rozee_jobs(request: JobRequest) -> List[Dict[str, Any]]:
    """
    Fetch jobs from Rozee.pk based on search criteria.
    
    Args:
        request (JobRequest): Job search request parameters
        
    Returns:
        List[Dict[str, Any]]: List of job listings from Rozee.pk
    """
    try:
        logger.info("Starting Rozee.pk scraper...")
        jobs = await scrape_rozee_sync(
            request.position,
            request.location,
            request.experience
        )
        logger.info(f"Found {len(jobs)} jobs from Rozee.pk")
        return [dict(**job, source="rozee") for job in jobs]
    except Exception as e:
        logger.error(f"Error in Rozee.pk scraper: {str(e)}")
        return []

@app.post(
    "/search-jobs/",
    response_model=List[JobResponse],
    summary="Search for jobs across multiple platforms",
    description="""Search for jobs across multiple platforms (LinkedIn, Indeed, Rozee.pk) 
    and analyze their relevance using LLM.""",
    response_description="List of relevant jobs with analysis results"
)
async def search_jobs(request: JobRequest) -> List[JobResponse]:
    """
    Search for jobs across multiple platforms and analyze their relevance.
    
    Args:
        request (JobRequest): Job search request parameters
        
    Returns:
        List[JobResponse]: List of relevant jobs with analysis results
        
    Raises:
        HTTPException: If there's an error in the search process
    """
    try:
        logger.info(f"Starting job search for position: {request.position}")
        logger.info(f"Location: {request.location}")
        logger.info(f"Sources: {request.sources}")
        
        # Reload environment variables
        load_dotenv(override=True)
        ollama_url = os.getenv("OLLAMA_API")
        logger.info("Initializing Ollama analyzer...")
        
        # Initialize analyzer and test connection
        analyzer = OllamaAnalyzer(ollama_url)
        if not await analyzer.test_connection():
            raise HTTPException(
                status_code=503,
                detail="Cannot connect to Ollama API. Please check if the service is running."
            )
        
        all_jobs = []
        
        # Fetch jobs from each source
        if "indeed" in request.sources:
            indeed_jobs = await get_indeed_jobs(request)
            if indeed_jobs:
                all_jobs.extend(indeed_jobs)
                logger.info(f"Indeed jobs collected: {len(indeed_jobs)}")
            await asyncio.sleep(5)  # Rate limiting
        
        if "linkedin" in request.sources:
            linkedin_jobs = await get_linkedin_jobs(request)
            if linkedin_jobs:
                all_jobs.extend(linkedin_jobs)
                logger.info(f"LinkedIn jobs collected: {len(linkedin_jobs)}")
        
        if "rozee" in request.sources:
            rozee_jobs = await get_rozee_jobs(request)
            if rozee_jobs:
                all_jobs.extend(rozee_jobs)
                logger.info(f"Rozee.pk jobs collected: {len(rozee_jobs)}")
        
        logger.info(f"Total jobs found: {len(all_jobs)}")
        
        # Analyze jobs using LLM
        analyzed_jobs = await analyzer.analyze_jobs_batch(all_jobs, request)
        
        # Convert to response format
        relevant_jobs = []
        for job in analyzed_jobs:
            try:
                # Ensure experience field exists with a fallback
                job_experience = job.get('experience', "Not specified")
                if not job_experience or job_experience.lower() == "none":
                    job_experience = "Not specified"

                job_response = JobResponse(
                    job_title=job['title'],
                    company=job['company'],
                    experience=job_experience,  # Use processed experience
                    jobNature=job.get('jobNature', 'Not specified'),
                    location=job['location'],
                    salary=job.get('salary', 'Not specified'),
                    apply_link=job['apply_link'],
                    description=job.get('description', 'No description available'),
                    source=job['source'],
                    relevance_score=job.get('relevance_score', 0),
                    matched_skills=job.get('matched_skills', [])
                )
                relevant_jobs.append(job_response)
            except Exception as e:
                logger.error(f"Error creating JobResponse for job {job.get('title', 'Unknown')}: {str(e)}")
                logger.error(f"Job data: {json.dumps(job, indent=2)}")  # Add detailed logging
                continue
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jobs_{request.position.replace(' ', '_')}_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "relevant_jobs": [
                        {
                            **job.model_dump(),
                            "relevance_score": job.relevance_score,
                            "matched_skills": job.matched_skills
                        }
                        for job in relevant_jobs
                    ]
                },
                f,
                indent=2,
                ensure_ascii=False
            )
        
        logger.info(f"Saved all jobs to {filename}")
        return relevant_jobs
        
    except Exception as e:
        logger.error(f"Error in search_jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/test-ollama/",
    summary="Test Ollama API connection",
    description="Test the connection to the Ollama LLM service",
    response_description="Connection test results"
)
async def test_ollama() -> Dict[str, Any]:
    """
    Test the connection to the Ollama LLM service.
    
    Returns:
        Dict[str, Any]: Connection test results
        
    Raises:
        HTTPException: If there's an error in the test
    """
    try:
        load_dotenv(override=True)
        ollama_url = os.getenv("OLLAMA_API")
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(
                f"{ollama_url}/api/generate",
                json={
                    "model": "deepseek-r1:7b",
                    "prompt": "Return a JSON with test: true",
                    "stream": False
                },
                headers={"Content-Type": "application/json"}
            )
            
            return {
                "status": response.status_code,
                "response": response.json() if response.status_code == 200 else response.text
            }
    except Exception as e:
        logger.error(f"Error in test_ollama: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Add this near the top of the file after imports
def signal_handler(sig, frame):
    logger.info("Ctrl+C detected. Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting server at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        logger.info("Server shutdown requested. Exiting...")
        sys.exit(0)