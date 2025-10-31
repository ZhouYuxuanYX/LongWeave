import os
import time
import traceback
from openai import OpenAI

os.environ['DASHSCOPE_API_KEY'] = 'sk-xxx'

client = OpenAI(
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

def call_api(model, messages, seed=42, max_retries=30, stream=True, collect_reasoning=True, **kwargs):
    """
    Call DashScope's OpenAI-compatible interface, supporting streaming responses with thinking process
    
    Parameters:
        model: Model name
        messages: Conversation history
        seed: Random seed
        max_retries: Maximum number of retries
        stream: Whether to use streaming output
        collect_reasoning: Whether to collect reasoning content
        **kwargs: Other parameters, such as extra_body etc.
        
    Returns:
        Dictionary containing complete reasoning and answer content
    """
    stream = True
    for _ in range(max_retries):
        try:
            # Construct request parameters
            request_params = {
                "model": model,
                "messages": messages,
                "seed": seed,
                "stream": stream,
                "extra_body": {
                    "enable_thinking": False,
                    "thinking_budget": 8192
                },
                **kwargs
            }

            chat_response = client.chat.completions.create(**request_params)

            if not stream:
                content = chat_response.choices[0].message.content.strip()
                reasoning = getattr(chat_response.choices[0].message, 'reasoning_content', '')
                return {"reasoning": reasoning, "answer": content}

            else:
                full_reasoning = ""
                full_answer = ""
                is_answering = False

                # if collect_reasoning:
                #     print("\n" + "=" * 20 + "Thinking Process" + "=" * 20 + "\n")

                for chunk in chat_response:
                    if not chunk.choices:
                        continue

                    delta = chunk.choices[0].delta

                    # Collect reasoning content
                    if collect_reasoning and hasattr(delta, "reasoning_content") and delta.reasoning_content:
                        # if not is_answering:
                        #     print(delta.reasoning_content, end="", flush=True)
                        full_reasoning += delta.reasoning_content

                    # Answer phase begins
                    if hasattr(delta, "content") and delta.content:
                        if not is_answering:
                            # print("\n" + "=" * 20 + "Complete Response" + "=" * 20 + "\n")
                            is_answering = True
                        # print(delta.content, end="", flush=True)
                        full_answer += delta.content

                # return {"reasoning": full_reasoning.strip(), "answer": full_answer.strip()}
                return full_answer.strip()

        except Exception as e:
            print(f"Error: {str(e)}, retrying...")
            traceback.print_exc()
            time.sleep(1)

    raise Exception("Max retries exceeded")