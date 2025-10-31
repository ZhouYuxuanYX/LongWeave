from openai import OpenAI
import time

# Define the mapping relationship between model scale and API address
MODEL_TO_API_BASE = {
    "qwen3-32b-r": "http://10.36.13.16:8012/v1",  
    "qwen3-14b-r": "http://10.36.5.33:8012/v1", 
    # "qwen3-8b-r": "http://10.36.5.36:8012/v1", 
    # "qwen3-4b-r": "http://10.36.5.35:8012/v1", 
}

def get_api_base_from_model(model_name):
    """
    Dynamically determine the openai_api_base based on the model name.
    :param model_name: Model name, for example "Qwen2___5-14B-Instruct"
    :return: Corresponding openai_api_base address
    """
    try:
        api_base = MODEL_TO_API_BASE.get(model_name)
        if not api_base:
            raise ValueError(f"No API address found corresponding to model scale {model_name}")
        return api_base
    except Exception as e:
        raise ValueError(f"Unable to parse model name: {model_name}. Error: {str(e)}")

def call_api(model, messages, temperature=0.1, max_retries=30, **kwargs):
    """
    Call the API and support dynamic setting of base_url.
    """
    # Dynamically determine openai_api_base
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
                # extra_body={
                #     "top_k": 20, 
                #     "chat_template_kwargs": {"enable_thinking": True},
                # },
                **kwargs
            )
            print("Reasoning response:", chat_response.choices[0].message.reasoning_content)
            print("Chat response:", chat_response.choices[0].message.content)
            return chat_response.choices[0].message.content.strip()

            # # print(chat_response.choices[0].message.reasoning_content)
            # return chat_response.choices[0].message.content.strip()
        except Exception as e:
            print(f"API error: {str(e)}, retrying...")
            time.sleep(1)
    raise Exception("Max retries reached")

if __name__ == "__main__":
    # # Example call
    model = "qwen3-32b-r"
    prompt = """9.11 and 9.8, which is greater?"""

    messages = [{"role": "user", "content": prompt}]
    response = call_api(model, messages, temperature=0.1, stream=False)

    # # Print response content
    # print("response:", response)