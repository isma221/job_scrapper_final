"""
LLM Job Analyzer Module
=====================

This module provides functionality to analyze job listings using Ollama LLM.
It compares job descriptions with user requirements and generates relevance scores.

Key Features:
- Job analysis using Ollama LLM
- Relevance scoring (0-100)
- Skill matching
- Batch processing with rate limiting
- Error handling and retries

Author: Isma
Date: 21-April-2025
Version: 1.0.0
"""

import httpx
import json
from typing import Tuple, List, Dict, Any, Optional
import asyncio
from pydantic import BaseModel, Field
import logging
import re
import traceback
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add this to prevent duplicate logs
logger.propagate = False

class SensitiveFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, api_url=None):
        super().__init__(fmt, datefmt)
        self.api_url = api_url

    def format(self, record):
        if isinstance(record.msg, str) and hasattr(self, 'api_url') and self.api_url:
            record.msg = record.msg.replace(self.api_url, '[HIDDEN_API_URL]')
        return super().format(record)

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

class OllamaAnalyzer:
    """
    Analyzer class for processing job listings using Ollama LLM.
    
    This class handles the communication with Ollama API and processes job listings
    to determine their relevance to user requirements.
    
    Attributes:
        api_url (str): Base URL for the Ollama API
    """
    
    def __init__(self, ngrok_url: str):
        """
        Initialize the analyzer with the Ollama API URL.
        
        Args:
            ngrok_url (str): URL of the Ollama API (can be ngrok URL)
        """
        self.api_url = ngrok_url.rstrip('/')
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        # Log with a placeholder instead of the actual URL
        logger.info("Initialized OllamaAnalyzer with API URL: [HIDDEN]")
        
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C by stopping analysis"""
        logger.info("Ctrl+C detected. Stopping analysis...")
        self.running = False
        sys.exit(0)

    async def analyze_job(self, job: Dict[str, Any], requirements: JobRequest) -> Tuple[float, List[str]]:
        """
        Analyze a single job posting against the given requirements.
        
        Args:
            job (Dict[str, Any]): Job posting details
            requirements (JobRequest): User's job requirements
            
        Returns:
            Tuple[float, List[str]]: A tuple containing:
                - relevance_score (float): Score between 0-100
                - matched_skills (List[str]): List of matched skills
                
        Raises:
            Exception: If analysis fails after all retries
        """
        message_content = f"""
        Analyze this job posting against the requirements and return a JSON response with a relevance score (0-100).
        
        Job Details:
        Title: {job.get('title', '')}
        Description: {job.get('description', '')}
        Experience Required: {job.get('experience', '')}
        Salary: {job.get('salary', '')}
        
        Requirements:
        Position Looking For: {requirements.position}
        Experience Level: {requirements.experience}
        Expected Salary: {requirements.salary}
        Required Skills: {requirements.skills}
        
        Return only a JSON object with these fields:
        1. relevance_score (number between 0-100)
        2. matched_skills (list of matched skills found in description)
        """
        
        logger.info(f"Analyzing job: {job.get('title', 'Unknown Title')}")
        
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                timeout = httpx.Timeout(80.0, connect=30.0)
                async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
                    logger.info(f"Attempt {attempt + 1} of {max_retries}")
                    
                    request_data = {
                        "model": "deepseek-r1:7b",
                        "messages": [
                            {"role": "user", "content": message_content}
                        ],
                        "stream": False
                    }
                    
                    response = await client.post(
                        f"{self.api_url}/api/chat",
                        json=request_data,
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if response.status_code == 200:
                        try:
                            json_data = response.json()
                            if "message" in json_data and "content" in json_data["message"]:
                                content = json_data["message"]["content"]
                                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                                if json_match:
                                    analysis = json.loads(json_match.group())
                                    logger.info(f"Successfully analyzed job: {job.get('title', 'Unknown Title')}")
                                    return (
                                        analysis.get('relevance_score', 0),
                                        analysis.get('matched_skills', [])
                                    )
                                else:
                                    logger.warning(f"No JSON found in response content for job: {job.get('title', 'Unknown Title')}")
                            else:
                                logger.warning(f"Unexpected response format for job: {job.get('title', 'Unknown Title')}")
                        except Exception as e:
                            logger.error(f"Error parsing response for job {job.get('title', 'Unknown Title')}: {str(e)}")
                    else:
                        logger.error(f"Error response for job {job.get('title', 'Unknown Title')}: {response.text}")
                    
            except httpx.ReadTimeout as e:
                logger.warning(f"Timeout on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Max retries reached")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                logger.error(f"Full error details:\n{traceback.format_exc()}")
                
        logger.error(f"All analysis attempts failed for job: {job.get('title', 'Unknown Title')}")
        return 0, []

    async def analyze_jobs_batch(self, jobs: List[Dict[str, Any]], requirements: JobRequest) -> List[Dict[str, Any]]:
        """
        Analyze a batch of jobs and return them sorted by relevance.
        
        Args:
            jobs (List[Dict[str, Any]]): List of job postings to analyze
            requirements (JobRequest): User's job requirements
            
        Returns:
            List[Dict[str, Any]]: List of analyzed jobs with relevance scores and matched skills,
                                 sorted by relevance score in descending order
        """
        analyzed_jobs = []
        
        # Rate limiting semaphore
        sem = asyncio.Semaphore(3)
        
        async def analyze_single_job(job: Dict[str, Any]) -> Dict[str, Any]:
            """
            Analyze a single job with rate limiting.
            
            Args:
                job (Dict[str, Any]): Job posting to analyze
                
            Returns:
                Dict[str, Any]: Job posting with analysis results
            """
            async with sem:
                try:
                    job_message = f"Analyzing job: {job.get('title', 'Unknown Title')}"
                    logger.info(job_message)
                    print(job_message)  # Direct terminal output
                    
                    score, skills = await self.analyze_job(job, requirements)
                    result = {
                        **job,
                        "relevance_score": score,
                        "matched_skills": skills
                    }
                    
                    # Print analysis result
                    analysis_message = f"Analysis complete - Score: {score}, Skills matched: {len(skills)}"
                    logger.info(analysis_message)
                    print(analysis_message)  # Direct terminal output
                    
                    return result
                except Exception as e:
                    error_message = f"Error analyzing job {job.get('title', 'Unknown Title')}: {str(e)}"
                    logger.error(error_message)
                    print(error_message)  # Direct terminal output
                    return {
                        **job,
                        "relevance_score": 0,
                        "matched_skills": [],
                        "error": str(e)
                    }
        
        # Process jobs in smaller batches
        batch_size = 5
        for i in range(0, len(jobs), batch_size):
            if not self.running:
                print("Analysis interrupted by user")  # Direct terminal output
                break
                
            batch = jobs[i:i + batch_size]
            batch_message = f"Processing batch {i//batch_size + 1} ({len(batch)} jobs)"
            logger.info(batch_message)
            print(batch_message)  # Direct terminal output
            
            tasks = [analyze_single_job(job) for job in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful_results = [
                result for result in batch_results 
                if isinstance(result, dict)
            ]
            analyzed_jobs.extend(successful_results)
            
            if i + batch_size < len(jobs):
                logger.info("Waiting 5 seconds before next batch...")
                await asyncio.sleep(5)
        
        # Sort by relevance score
        analyzed_jobs.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        logger.info(f"Successfully analyzed {len(analyzed_jobs)} out of {len(jobs)} jobs")
        return analyzed_jobs

    async def test_connection(self) -> bool:
        """
        Test the connection to the Ollama API.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            timeout = httpx.Timeout(10.0, connect=5.0)
            async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
                response = await client.post(
                    f"{self.api_url}/api/chat",
                    json={
                        "model": "deepseek-r1:7b",
                        "messages": [
                            {"role": "user", "content": "test"}
                        ],
                        "stream": False
                    },
                    headers={"Content-Type": "application/json"}
                )
                success = response.status_code == 200
                logger.info(f"Connection test {'succeeded' if success else 'failed'}")
                return success
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False