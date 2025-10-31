import argparse
import os
import sys
from pathlib import Path

# Add the project root directory to Python path so we can import modules
# This allows the script to run independently from different locations
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Try to import with standard production paths first
try:
    from core.serve.dashscope import call_api as dashscope_call
    from core.serve.dashscope_think_budget import call_api as dashscope_think_budget_call
    from core.serve.dlc import call_api as dlc_call
    from core.serve.oai import call_api as openai_call
    from core.serve.dlc_r import call_api as dlc_r_call
    from core.serve.huggingface import call_api as huggingface_call
    IMPORT_SUCCESS = True
except ImportError:
    # If that fails, try direct imports (for when running from the script's directory)
    try:
        import dashscope
        import dashscope_think_budget
        import dlc
        import oai
        import dlc_r
        import huggingface
        from dashscope import call_api as dashscope_call
        from dashscope_think_budget import call_api as dashscope_think_budget_call
        from dlc import call_api as dlc_call
        from oai import call_api as openai_call
        from dlc_r import call_api as dlc_r_call
        from huggingface import call_api as huggingface_call
        IMPORT_SUCCESS = True
    except ImportError:
        # If all imports fail, set flag to False
        IMPORT_SUCCESS = False
        print("Warning: Could not import API modules. Functions will not work until proper modules are installed.")


def unified_call(backend, model, prompt, **kwargs):
    """
    Unified calling interface
    :param backend: Backend type (dashscope/dlc/openai)
    :param model: Model name
    :param messages: Message list
    :param kwargs: Other API parameters
    """
    messages = [
        {'role': 'system', 'content': 'You are a helpful assistant.'},
        {'role': 'user', 'content': prompt}
    ]

    if backend == 'dashscope':
        return dashscope_call(model=model, messages=messages, **kwargs)
    if backend == 'dashscope_think_budget':
        return dashscope_think_budget_call(model=model, messages=messages, **kwargs)
    elif backend == 'dlc_r':
        return dlc_r_call(model=model, messages=messages, **kwargs)
    elif backend == 'dlc':
        return dlc_call(model=model, messages=messages, **kwargs)
    elif backend == 'openai':
        return openai_call(model=model, messages=messages, **kwargs)
    elif backend == 'huggingface':
        return huggingface_call(model=model, messages=messages, **kwargs)
    else:
        raise ValueError(f"Unsupported backend: {backend}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Unified model calling interface')
    # Set parameters with default values
    parser.add_argument('--backend', type=str, default='dashscope',
                      choices=['dashscope', 'dlc', 'openai', 'huggingface'],
                      help='Select backend service to use (default: dashscope)')
    parser.add_argument('--model', type=str, default='qwen-turbo',
                      help='Model name to use (default: qwen-turbo)')
    parser.add_argument('--prompt', type=str, default='9.11 and 9.8, which is greater?',
                      help='User input prompt (default: Hello, please introduce yourself)')
    parser.add_argument('--temperature', type=float, default=0.1,
                      help='Generation temperature parameter (default: 0.1)')
    parser.add_argument('--max_tokens', type=int, default=1024*8,
                      help='Maximum number of generated tokens (default: 1024)')

    args = parser.parse_args()

    prompt = args.prompt
    try:
        # response = unified_call(
        #     backend="openai",
        #     model="gpt-4o-2024-11-20",  # gpt-4o-2024-08-06, gpt-4o-2024-11-20, gpt-4o-mini(2024-07-18), o1-mini-0912, o1-preview-2024-09-12
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens
        # )
        # response = unified_call(
        #     backend="dlc",
        #     model="qwen3-32b",
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens,
        #     stream=False
        # )
        response = unified_call(
            backend="dashscope",
            model="qwen-plus",
            prompt=prompt,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            stream=False
        )
        # response = unified_call(
        #     backend="dashscope_think_budget",
        #     model="qwen3-32b",
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens,
        #     stream=False
        # )
        # response = unified_call(
        #     backend="dlc_r",
        #     model="qwen3-32b-r",
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens,
        #     stream=True
        # )
        # response = unified_call(
        #     backend=args.backend,
        #     model=args.model,
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens
        # )
        # response = unified_call(
        #     backend="huggingface",
        #     model="gpt-oss-20b",
        #     prompt=prompt,
        #     temperature=args.temperature,
        #     max_tokens=args.max_tokens
        # )
        print(f"[{args.backend.upper()}] Response result:\n{response}")
    except Exception as e:
        print(f"Call failed: {str(e)}")