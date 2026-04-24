from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

def extract_sources(prompt: str):
    system_prompt = """
Extract data sources and paths.

Supported sources: s3, azure

Return ONLY JSON:
{
  "sources": [
    {"type": "s3", "path": "..."},
    {"type": "azure", "path": "..."}
  ]
}
"""

    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    return response.choices[0].message.content
