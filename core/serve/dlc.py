from openai import OpenAI
import time
import random

MODEL_TO_API_BASE = {
    # Option 1: Qwen2___5-72B-Instruct with load balancing between two endpoints
    # During inference, requests will be randomly distributed between the two endpoints
    # "Qwen2___5-72B-Instruct": ["http://10.36.8.59:8012/v1", "http://10.36.4.112:8012/v1"], 
    
    # Option 2: Qwen2___5-72B-Instruct with single endpoint (currently active)
    "Qwen2___5-72B-Instruct": "http://10.36.4.112:8012/v1",
    
    # Other model configurations
    "Meta-Llama-3___1-8B-Instruct": "http://10.36.5.208:8012/v1",
    "qwen3-8b": "http://10.36.1.188:8012/v1",
    # # "DeepSeek-R1-Distill-Qwen-32B": "http://10.36.0.114:8012/v1",
    # # "DeepSeek-R1-Distill-Qwen-7B": "http://10.36.0.123:8012/v1",
    # # "Phi-4-mini-instruct": "http://10.36.7.106:8012/v1",
    # "LongWriter-glm4-9b": "http://10.36.0.29:8012/v1",
    # "gemma-3-12b-it": "http://10.36.0.29:8012/v1",
    # "gemma-3-27b-it": "http://10.36.7.106:8012/v1",
    # "qwen3-32b": "http://10.36.2.128:8012/v1",
    # "qwen3-14b": "http://10.36.5.35:8012/v1",
    # "qwen3-4b": "http://10.36.3.70:8012/v1",

}

def get_api_base_from_model(model_name):
    """
    Dynamically determine openai_api_base based on model name.
    :param model_name: Model name, e.g. "Qwen2___5-3B-Instruct"
    :return: Corresponding openai_api_base address
    """
    try:
        api_base = MODEL_TO_API_BASE.get(model_name)
        if not api_base:
            raise ValueError(f"No API address found for model size {model_name}")
        # If there are multiple addresses, randomly select one
        if isinstance(api_base, list):
            return random.choice(api_base)
        return api_base
    except Exception as e:
        raise ValueError(f"Unable to parse model name: {model_name}. Error: {str(e)}")

def call_api(model, messages, temperature=0.1, max_retries=30, **kwargs):
    """
    Call API and support dynamic setting of base_url.
    """
    openai_api_base = get_api_base_from_model(model)
    openai_api_key = "EMPTY"

    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    kwargs['stream'] = False
    for _ in range(max_retries):
        try:
            chat_response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                **kwargs
            )
            if kwargs.get("stream") == False:
                return chat_response.choices[0].message.content.strip()
            else:
                full_response = ""
                for chunk in chat_response:
                    if chunk.choices[0].delta.content is not None:  # Ensure there is content
                        full_response += chunk.choices[0].delta.content
                return full_response
        except Exception as e:
            print(f"API error: {str(e)}, retrying...")
            time.sleep(1)
    raise Exception("Max retries reached")

if __name__ == "__main__":
    # # Example call
    model = "qwen3-8b"
    messages = [{"role": "user", "content": "9.11 and 9.8, which is greater?"}]
    response = call_api(model, messages, temperature=0.1, stream=False)
    print(response)