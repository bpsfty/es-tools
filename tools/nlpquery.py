"""
This program is intended to convert used query in natural language to a Kibana query.
"""
import os
import openai
import dotenv
from common_variables import *

dotenv.load_dotenv()

client = openai.AzureOpenAI(azure_endpoint=oa_endpoint, api_key=oa_api_key, api_version=oa_api_version)

completion = client.chat.completions.create(model=oa_deployment, messages=[{'role': 'user', 'content': 'What is your name?'}])

print(f"{completion.choices[0].message.role}: {completion.choices[0].message.prompt}")

