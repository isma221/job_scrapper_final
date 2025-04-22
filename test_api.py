import requests

url = "http://localhost:8000/search-jobs/"

payload = {
    "position": "Software Engineer",
    "experience": "2 years",
    "salary": "70,000 PKR to 120,000 PKR",
    "jobNature": "onsite",
    "location": "Islamabad",
    "skills": "full stack, MERN, Node.js, Express.js, React.js, Next.js, Firebase, TailwindCSS, CSS Frameworks, Tokens handling",
    "sources": ["indeed", "linkedin", "rozee"]
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())
