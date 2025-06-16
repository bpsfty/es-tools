"""
This program is intended to convert used query in natural language to a Kibana query.
"""
import os
import openai
import dotenv

dotenv.load_dotenv()

oa_endpoint = 'https://iona-openai.openai.azure.com/'
oa_api_key = ''
oa_deployment = 'gpt-4o-mini'
oa_api_version = '2024-05-01-preview'

client = openai.AzureOpenAI(azure_endpoint=oa_endpoint, api_key=oa_api_key, api_version=oa_api_version)

completion = client.chat.completions.create(model=oa_deployment, messages=[{'role': 'user', 'content': 'What is your name?'}])

print(f"{completion.choices[0].message.role}: {completion.choices[0].message.prompt}")

# from ollama import chat
#
# def converseWithOllama(modelName, prompt):
#     try:
#         response = chat(model=modelName, messages=[{"role": "user", "content": prompt}])
#         return response['message']['content']
#     except Exception as e:
#         return f"An error occurred: {e}"
#
# if __name__ == "__main__":
#     modelName = "llama3.1:8b"
#     prompt = input("You: ")
#     print(converseWithOllama(modelName, prompt))
