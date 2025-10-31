import os
import time
import traceback
import json
from openai import OpenAI  # Import OpenAI library

# Set API Key
os.environ['OPENAI_API_KEY'] = ''

# ----------------- Configure OpenAI client -----------------
# Note: base_url should point to the base path of the API, not the specific /chat/completions
# The library will automatically concatenate this part of the path.
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    base_url="https://api.openai.com/v1"
)
# -------------------------------------------------------------

def call_api(model, messages, max_retries=30, **kwargs):
    """
    Call API compatible with OpenAI format using the official openai library.
    """
    for i in range(max_retries):
        try:
            # Use client.chat.completions.create for calling
            response_stream = client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True,
                **kwargs # Pass other standard parameters, such as temperature, top_p, etc.
            )

            # List for caching streaming data
            full_content = []

            # openai library has already handled parsing of streaming data
            for chunk in response_stream:
                # Check if chunk has content
                content_part = chunk.choices[0].delta.content
                if content_part:
                    full_content.append(content_part)
            
            # Concatenate full content and return
            result = ''.join(full_content)
            return result

        except Exception as e:
            print(f"Attempt {i+1}/{max_retries}: API call error: {str(e)}")
            traceback.print_exc()
            time.sleep(1) # Wait before retrying

    raise Exception(f"API call failed, reached maximum retries ({max_retries})")


if __name__ == "__main__":
    
    test_messages = [
        {"role": "user", "content": "Hi."}
    ]
    
    try:
        response = call_api(
            "chatgpt-4o-latest",
            messages=test_messages, 
        )
        print("API call successful!")
        print("Response content:", response)
    except Exception as e:
        print(f"API call failed: {e}")