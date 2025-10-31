from core.simulation.state_machine import StateMachine, generate_transition_table
from core.tasks.base_task import BaseTask, TaskFactory
from core.serve.unified_api import unified_call
import time
import random
import re
from core.seed import generate_seed_from_id


class StateMachineTask(BaseTask):
    """Task for generating simulation steps based on state machine rules"""
    registered_metrics = ['match_ratio']

    def __init__(self, config: dict):
        """
        Initialize the task
        
        Args:
            config: Task configuration dictionary
        """
        registered_metrics = ['match_ratio', 'is_correct']
        self.config = config
        self.states = [f"S{i}" for i in range(config.get('num_states', 20))]
        self.initial_state = config.get('initial_state', 'S0')
        self.input_alphabet = [str(i) for i in range(config.get('input_size', 10))]
        self.output_alphabet = [str(i) for i in range(config.get('output_size', 10))]
        self.transition_table = generate_transition_table(
            states=self.states,
            input_alphabet=self.input_alphabet,
            output_alphabet=self.output_alphabet
        )
        self.state_machine = StateMachine(
            states=self.states,
            initial_state=self.initial_state,
            input_alphabet=self.input_alphabet,
            output_alphabet=self.output_alphabet,
            transition_table=self.transition_table
        )

    def generate_prompt(self, **kwargs) -> str:
        """
        Generate the prompt text for the task.
        
        Args:
            kwargs: Optional parameters (e.g., input string)
            
        Returns:
            str: The generated prompt text
        """
        # Get random seed
        seed = generate_seed_from_id(kwargs.get("sample_id"))
        rng = random.Random(seed)  # Create independent random number generator instance

        # Example: Generate random input string
        input_alphabet = [str(i) for i in range(kwargs['task_config']['input_size'])]  # Input alphabet (numbers 0-9)
        input_string_length = kwargs['task_config'].get('input_string_length', 20)  # Fixed length, default is 20
        input_string = ''.join(rng.choices(input_alphabet, k=input_string_length))

        # Construct prompt text
        prompt = (
            f"Your task is to simulate a state transition process based on the following rules.\n"
            f"The input string for this simulation is: '{input_string}'.\n\n"
            "The state machine operates with the following configuration:\n\n"
            f"1. Initial State: {self.initial_state}\n"
            "2. State Transition Rules:\n\n"
            "   Current State | Input | Next State | Output Signal\n"
            "   --------------------------------------------------\n"
        )

        # Display state transition rules in table format
        for state in self.states:
            for input_char in self.input_alphabet:
                next_state = self.transition_table[state][input_char]['next_state']
                output = self.transition_table[state][input_char]['output']
                prompt += (
                    f"   {state:<12} | {input_char:<5} | {next_state:<10} | {output}\n"
                )
        prompt += "\n"

        # Dynamically generate One-Shot example
        example_input_string = input_string[:3]  # Take first three characters of input string
        if len(example_input_string) < 3:
            # If less than three characters, pad with random characters
            example_input_string += ''.join(random.choices(self.input_alphabet, k=3 - len(example_input_string)))

        # Process example input string using state machine
        example_output_signal = self.state_machine.process_input(example_input_string)
        example_process = []
        current_state = self.initial_state

        for i, char in enumerate(example_input_string):
            next_state = self.transition_table[current_state][char]['next_state']
            output = self.transition_table[current_state][char]['output']
            example_process.append(f"{current_state:<12} | {char:<5} | {next_state:<10} | {output}")
            current_state = next_state

        # Add dynamically generated One-Shot example
        prompt += (
            "Here is an example of a valid state transition process:\n"
            f"Assume the input string is '{example_input_string}'. The state transition process would be as follows:\n"
            "Current State | Input | Next State | Output Signal\n"
            "-----------------------------------------------\n"
        )
        prompt += "\n".join(example_process) + "\n"

        prompt += (
            "\nNote: The above example is dynamically generated based on the state transition rules and the input string. "
            "The actual output may vary depending on the specific input string.\n"
        )

        # Specify input string and request corresponding output
        prompt += (
            "\nBased on the above rules, please generate a simulated state transition process "
            f"for the input string '{input_string}'.\n"
            "Display the current state, input, next state, and output signal for each step.\n"
            "Ensure that the generated process strictly adheres to the state machine rules.\n"
            "Important: \n"
            "1. Do NOT generate any code or explanatory text. "
            "2. Do **NOT** use any form of truncation. You **must** list all steps.\n"
            "Only provide the state transition process in the following format:\n"
            "Current State | Input | Next State | Output Signal\n"
            "-----------------------------------------------\n"
            "<State>       | <Char>| <NextState>| <Output>\n"
            "...\n"
        )
        meta_data = {"input_string": input_string}
        return prompt, meta_data

    def call_api(self, prompt: str) -> str:
        """
        Call API to generate text
        
        Args:
            prompt: Input prompt
            
        Returns:
            str: Generated text
            
        Note:
            Retry up to 3 times if API call fails
        """
        for _ in range(3):
            try:
                return unified_call(
                    backend=self.config['api']['backend'],
                    model=self.config['api']['model'],
                    messages=[
                        {'role': 'user', 'content': prompt}
                    ],
                    **self.config['api']['params']
                )
            except Exception as e:
                print(f"API error: {str(e)}")
                time.sleep(1)
        return ""

    def evaluate(self, response: str, **kwargs) -> dict:
        """
        Evaluate the generated text against the state machine rules.
        
        Args:
            response: The generated text from the model
            kwargs: Optional parameters (e.g., input_string)
            
        Returns:
            dict: Evaluation results, including total steps, valid steps, match ratio, and error details
        """
        # Get user input string
        input_string = kwargs['metadata']['input_string']
        if not input_string:
            raise ValueError("Evaluation requires an input string.")

        # Use state machine to generate correct state transition process and output signals
        try:
            correct_output_signal = self.state_machine.process_input(input_string)
        except Exception as e:
            return {
                'total_steps': len(input_string),
                'valid_steps': 0,
                'match_ratio': 0.0,
                'is_correct': False,
                'errors': [f"Error processing input string: {str(e)}"]
            }

        # Initialize evaluation results
        total_steps = len(input_string)
        valid_steps = 0
        errors = []  # Record error information

        # Filter out empty lines and header lines
        lines = response.strip().split("\n")
        filtered_lines = []
        for line in lines:
            line = line.strip()
            # Skip empty lines, header lines and separator lines
            if (
                not line or 
                re.match(r"^\s*current\s+state\s*\|", line, re.IGNORECASE) or 
                re.match(r"^\s*-+\s*$", line)
            ):
                continue
            filtered_lines.append(line)
        
        # Truncate filtered_lines to ensure its length does not exceed the length of correct_output_signal
        filtered_lines = filtered_lines[:len(correct_output_signal)]


        # Start line-by-line comparison
        for step_index, line in enumerate(filtered_lines):
            # Extract output signal of current step
            parts = [part.strip() for part in re.split(r"\s*[|:,]\s*", line)]  # Support multiple separators
            if len(parts) >= 4:
                output_signal = parts[3]  # Assume output signal is in column 4
            else:
                # If unable to extract output signal correctly, record error and stop
                errors.append(f"Step {step_index + 1}: Invalid format - '{line}'")
                break

            # Compare if output signals match
            if step_index < len(correct_output_signal) and output_signal == correct_output_signal[step_index]:
                valid_steps += 1
            else:
                errors.append(
                    f"Step {step_index + 1}: Mismatch - "
                    f"Expected Output: {correct_output_signal[step_index]}, Got: {output_signal}"
                )
                break  # Output signal mismatch, stop immediately

        # Calculate match ratio
        match_ratio = valid_steps / total_steps if total_steps > 0 else 0.0
        is_correct = match_ratio == 1.0

        # Return evaluation results
        return {
            'total_steps': total_steps,
            'valid_steps': valid_steps,
            'match_ratio': match_ratio,
            'is_correct': is_correct,
            'errors': errors
        }

# Register task
TaskFactory.register_task('STATE_MACHINE', StateMachineTask)