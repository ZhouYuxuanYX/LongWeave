# core/tasks/sales_report_task.py

import json
import re
import random
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple, Union # Added Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm # Ensure tqdm is imported

from core.tasks.base_task import BaseTask, TaskFactory
# from core.seed import generate_seed_from_id # Not strictly needed here unless prompt varies by seed
from core.serve.unified_api import unified_call # Assuming this is available


class SalesReporterTask(BaseTask):
    """
    Task for generating sales reports based on CSV data and evaluating them
    against target analytical questions and answers derived from synthetic data.
    The prompt includes specific questions to guide the generation.
    Evaluation uses batched, parallel checking for both answering the question
    and the correctness of the answer.
    """
    # Updated metrics
    registered_metrics = ['answered_rate', 'correctness_rate', 'words', 'total_score']

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.task_path = config.get('task_path', {}) # Store the specific task path (e.g., "SALES_REPORT_GENERATION/1k")

        # Store evaluation settings
        self.evaluation_model_config = config.get('evaluation_model', )
        self.batch_size = config.get('conclusion_evaluation_batch_size', 5) # Lower default batch size due to more complex eval prompt
        self.max_workers = config.get('evaluation_max_workers', 5) # Maybe adjust based on complexity
        self.test_length = config.get('test_length', ) # Keep track of target length if specified

        # Store data paths defined in task_config.yaml
        self.data_paths = {
            "1k": config.get("data_path_1k"),
            "2k": config.get("data_path_2k"),
            "4k": config.get("data_path_4k"),
            "8k": config.get("data_path_8k"),
            # Add more if needed, or handle dynamically
        }

        if not self.evaluation_model_config:
            print("[WARN] Evaluation model configuration missing in SalesReporterTask config.")
            self.evaluation_model_config = {'backend':'dummy', 'model':'dummy', 'params':{}}

    def _get_task_suffix(self) -> str:
        parts = self.task_path.split('/')
        return parts[-1] if len(parts) > 1 else None

    def _get_data_paths_for_sample(self, sample_id: str) -> Tuple[str, str]:
        suffix = self._get_task_suffix()
        if not suffix or suffix not in self.data_paths or not self.data_paths[suffix]:
            raise ValueError(f"Invalid task path suffix '{suffix}' or missing data path configuration for it in task_config.yaml.")

        base_data_dir = self.data_paths[suffix]
        try:
            # Attempt to parse sample_id directly as index first
            sample_idx = int(sample_id)
        except ValueError:
            # If that fails, try parsing the last part after '_'
            try:
                sample_idx = int(sample_id.split('_')[-1])
            except (ValueError, IndexError):
                 raise ValueError(f"Could not parse sample index from sample_id '{sample_id}'. Expected integer or 'prefix_index'.")

        csv_filename = f"sales_data_{sample_idx:03d}.csv"
        json_filename = f"sales_conclusions_{sample_idx:03d}.json"
        csv_path = os.path.join(base_data_dir, csv_filename)
        json_path = os.path.join(base_data_dir, json_filename)

        # More robust checks
        if not Path(base_data_dir).is_dir():
             raise FileNotFoundError(f"Base data directory not found: {base_data_dir}")
        if not Path(json_path).is_file():
            print(f"[WARN] Context/Conclusions JSON file not found: {json_path}")
            # For evaluation, this is more critical now
            # raise FileNotFoundError(f"Evaluation conclusions JSON file not found: {json_path}")
        if not Path(csv_path).is_file():
            print(f"[WARN] Data CSV file not found: {csv_path}")
            # Decide if this is fatal

        return csv_path, json_path

    def _count_words(self, text: str) -> int:
        words = re.findall(r'\b\w+\b', text.lower())
        return len(words)

    # --- Modified Function ---
    def _load_qa_data(self, json_path: str) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
        """
        Loads generation context (config) and target Q&A pairs from JSON.
        Returns context and a list of {'question': str, 'answer': str} dicts.
        """
        default_context = {"region": "N/A", "target_month_str": "N/A", "currency": "N/A", "regional_target": 0.0, "prev_month_sales": 0.0}
        qa_pairs = []

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            context = data.get("configuration", default_context)
            raw_conclusions = data.get("target_conclusions", [])

            if not isinstance(raw_conclusions, list):
                print(f"[WARN] 'target_conclusions' in {json_path} is not a list. Using empty list.")
            else:
                for idx, item in enumerate(raw_conclusions):
                    if isinstance(item, dict) and 'question' in item and 'answer' in item:
                        # Basic validation: ensure question and answer are non-empty strings
                        question = str(item['question']).strip()
                        answer = str(item['answer']).strip()
                        if question and answer:
                            # Store as a dictionary pair
                            qa_pairs.append({"question": question, "answer": answer})
                        else:
                            print(f"[WARN] Skipping item {idx} in {json_path} due to empty question or answer.")
                    else:
                        print(f"[WARN] Skipping invalid item at index {idx} in 'target_conclusions' list in {json_path}. Expected dict with 'question' and 'answer'. Found: {type(item)}")

            if not qa_pairs:
                 print(f"[WARN] No valid Q&A pairs loaded from 'target_conclusions' in {json_path}.")

            return context, qa_pairs

        except FileNotFoundError:
            print(f"[WARN] Context/Conclusions JSON file not found: {json_path}. Using default context and no Q&A pairs.")
            return default_context, []
        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON from {json_path}. Using default context and no Q&A pairs.")
            return default_context, []
        except Exception as e:
            print(f"[ERROR] Unexpected error loading QA data from {json_path}: {e}. Using defaults.")
            return default_context, []


    def _read_csv_data(self, csv_path: str, max_rows: int = 1000) -> str:
        """Reads CSV data into a string format suitable for the prompt."""
        try:
            # Ensure the file exists before trying to read
            if not os.path.exists(csv_path):
                 raise FileNotFoundError(f"CSV file not found at {csv_path}")
            df = pd.read_csv(csv_path)
            if len(df) > max_rows:
                print(f"[INFO] CSV has {len(df)} rows, limiting to {max_rows} for prompt.")
                csv_string = df.head(max_rows).to_markdown(index=False)
                csv_string += "\n... (data truncated)"
            else: csv_string = df.to_markdown(index=False)
            return csv_string
        except FileNotFoundError as e:
             print(f"[ERROR] {e}")
             return "[ERROR: Sales data CSV file not found]"
        except Exception as e:
             print(f"[ERROR] Failed to read CSV {csv_path}: {e}")
             return "[ERROR: Failed to read sales data]"

    # --- Modified Function ---
    def generate_prompt(self, **kwargs) -> Tuple[str, Dict]:
        """
        Generates the prompt asking the LLM to analyze the provided CSV data
        by answering specific questions and writing a sales report.
        Stores Q&A pairs in metadata for evaluation.
        """
        sample_id = kwargs.get('sample_id')
        if not sample_id: raise ValueError("SalesReporterTask.generate_prompt requires 'sample_id'.")

        csv_path, json_path = self._get_data_paths_for_sample(sample_id)

        # Load context and the list of Q&A pairs
        prompt_context, qa_pairs = self._load_qa_data(json_path)

        # Extract just the questions for the generation prompt
        target_questions = [item['question'] for item in qa_pairs]
        # The full qa_pairs list will be stored in metadata

        csv_data_string = self._read_csv_data(csv_path, max_rows=1000) # Adjust max_rows if needed

        # --- Refined Prompt with Specific Analysis Questions ---
        prompt_lines = [
            "**Role:** Senior Business Analyst",
            "",
            f"**Task:** You are provided with raw sales transaction data in CSV format. Your goal is to perform a detailed analysis based *only* on this data and generate a comprehensive sales performance report.",
            "",
            "**Input Sales Data (CSV Format):**",
            "```csv",
            csv_data_string.strip(),
            "```",
            "",
            "**Analysis Structure Guidance:**",
            "Please structure your sales performance report logically. Start with an overall performance summary, then delve into analyses of sales representative performance, product performance, and any other relevant insights identified from the data. Use a narrative style suitable for a management report, ensuring all insights are directly derived from the provided CSV data.",
            "",
            "**Required Content - Address These Specific Questions:**",
            "Within your structured analysis, ensure you specifically attempt to answer the following questions based *only* on the provided data:"
        ]

        # Add the dynamically loaded questions
        if target_questions:
            for idx, question in enumerate(target_questions):
                prompt_lines.append(f"    - **Question {idx + 1}:** {question}")
        else:
            prompt_lines.append("    - (No specific questions provided - perform a general analysis based on the data.)")


        # Add length specifications if test_length is set
        prompt_lines.append("") # Add a blank line before length spec
        if self.test_length:
            prompt_lines.extend([
                f"- **Length Specifications (TARGET WORD COUNT):**",
                f"- The report should be **around {self.test_length} words**. The deviation from this length may affect your evaluation.",
                "",
                f"You may now begin your analysis and write the approximately {self.test_length} words report:"
            ])
        else:
             prompt_lines.extend([
                f"You may now begin your analysis and write the report:"
            ])


        # Combine lines into the final prompt string
        prompt = "\n".join(prompt_lines)

        # --- Store the full Q&A pairs in metadata for evaluation ---
        metadata = {
            'qa_pairs': qa_pairs, # Store the list of {'question': q, 'answer': a} dicts
        }
        return prompt, metadata


    # --- New Parsing Function for Complex Evaluation ---
    def _parse_batch_evaluation_response(self, response: str, batch_size: int) -> List[Dict[str, bool]]:
        """
        Parses the LLM response which should contain a JSON list of evaluation objects.
        Each object should have 'answered' (bool) and 'correct' (bool) keys.
        Handles errors and ensures a list of the correct size is returned.
        """
        default_result = {'answered': False, 'correct': False}
        try:
            # Use a more flexible regex to find the JSON list, allowing whitespace and surrounding text
            match = re.search(r'\[\s*(\{.*?\})\s*(?:,\s*(\{.*?\})\s*)*\]', response, re.DOTALL)
            if match:
                json_str = match.group(0)
                parsed_list = json.loads(json_str)

                if isinstance(parsed_list, list):
                    validated_list = []
                    for item in parsed_list:
                        if isinstance(item, dict) and \
                           'answered' in item and isinstance(item['answered'], bool) and \
                           'correct' in item and isinstance(item['correct'], bool):
                            # Ensure correct is False if answered is False
                            if not item['answered']:
                                item['correct'] = False
                            validated_list.append(item)
                        else:
                            print(f"[WARN] Invalid item format in evaluation response: {item}. Using default.")
                            validated_list.append(default_result)

                    # Check length and pad/truncate if necessary
                    if len(validated_list) == batch_size:
                        return validated_list
                    elif len(validated_list) < batch_size:
                        print(f"[WARN] LLM returned fewer valid results ({len(validated_list)}) than expected ({batch_size}). Padding with defaults.")
                        return validated_list + [default_result] * (batch_size - len(validated_list))
                    else: # len > batch_size
                        print(f"[WARN] LLM returned more results ({len(validated_list)}) than expected ({batch_size}). Truncating.")
                        return validated_list[:batch_size]
                else:
                    raise ValueError("Parsed JSON is not a list.")
            else:
                # Try lenient parsing if strict regex fails
                 start = response.find('[')
                 end = response.rfind(']')
                 if start != -1 and end != -1 and start < end:
                     potential_json = response[start:end+1]
                     try:
                         parsed_list = json.loads(potential_json)
                         if isinstance(parsed_list, list):
                             print("[WARN] Used lenient JSON parsing for batch evaluation response.")
                             # Repeat validation and length check logic (simplified here for brevity)
                             validated_list = []
                             for item in parsed_list:
                                if isinstance(item, dict) and 'answered' in item and 'correct' in item and \
                                   isinstance(item['answered'], bool) and isinstance(item['correct'], bool):
                                    validated_list.append({'answered': item['answered'], 'correct': (item['correct'] and item['answered'])}) # Ensure correct=False if answered=False
                                else:
                                    validated_list.append(default_result)
                             # Adjust length
                             if len(validated_list) < batch_size: validated_list += [default_result] * (batch_size - len(validated_list))
                             return validated_list[:batch_size] # Truncate if too long
                         else: raise ValueError("Leniently parsed object is not a list.")
                     except (json.JSONDecodeError, ValueError):
                        raise ValueError("Could not find or parse a JSON list of objects using strict or lenient methods.")
                 else:
                    raise ValueError("Could not find square brackets potentially enclosing a JSON list.")

        except (json.JSONDecodeError, ValueError, Exception) as e:
            print(f"[ERROR] Failed to parse batch evaluation response: {e}. Response: '{response[:200]}...'. Returning all defaults for batch.")
            return [default_result] * batch_size


    # --- Modified Evaluation Function ---
    def _evaluate_qa_batch(self, qa_batch: List[Dict[str, str]], report_text: str) -> List[Dict[str, bool]]:
        """
        Uses an LLM to evaluate if questions in a batch were answered in the report
        and if the answers were correct based on the target answers.

        Args:
            qa_batch (List[Dict[str, str]]): List of {'question': q, 'answer': a} dicts.
            report_text (str): The generated sales report text.

        Returns:
            List[Dict[str, bool]]: List of {'answered': bool, 'correct': bool} dicts, one per Q&A pair.
        """
        if not qa_batch:
            return []

        batch_size = len(qa_batch)
        # Format the Q&A pairs for the prompt
        qa_prompt_text = "\n".join([
            f"Item {i+1}:\n"
            f"  Question: {qa['question']}\n"
            f"  Target Answer: {qa['answer']}\n"
            for i, qa in enumerate(qa_batch)
        ])

        eval_prompt = f"""
        **Task:** Evaluate the 'SALES REPORT' against a list of 'Question/Target Answer' pairs.
        For each item, determine:
        1.  **Answered:** Did the report *attempt* to answer the specific 'Question'? (true/false)
        2.  **Correct:** *If* the question was answered, does the answer provided in the report align with the 'Target Answer'? (true/false). If the question was not answered, this MUST be false.

        **Evaluation Guidance:**
        -   Focus on the *substance* of the question and answer, not exact wording.
        -   'Answered' means the report addresses the core topic of the question, even briefly.
        -   'Correct' means the information given in the report matches the *meaning* of the 'Target Answer'. Minor phrasing differences are acceptable. Numerical values should be reasonably close if applicable.
        -   If the report doesn't mention the topic of the question at all, 'answered' is false and 'correct' is automatically false.

        **Question/Target Answer Pairs:**
        --- START PAIRS ---
        {qa_prompt_text}
        --- END PAIRS ---

        **SALES REPORT:**
        --- START REPORT ---
        {report_text}
        --- END REPORT ---

        **Output Format:**
        Respond ONLY with a single JSON list containing exactly {batch_size} objects. Each object must correspond *in order* to the 'Item' number above and have two keys: "answered" (boolean) and "correct" (boolean).

        **Example Output (for 2 items):**
        [
          {{"answered": true, "correct": true}},
          {{"answered": false, "correct": false}}
        ]

        **Your JSON Output:**
        """

        try:
            eval_response = unified_call(
                backend=self.evaluation_model_config.get('backend', 'dummy'),
                model=self.evaluation_model_config.get('model', 'dummy'),
                prompt=eval_prompt,
                **self.evaluation_model_config.get('params', {})
            ).strip()

            # Use the new parsing function
            return self._parse_batch_evaluation_response(eval_response, batch_size)

        except Exception as e:
            print(f"[ERROR] LLM call failed during batch Q&A evaluation: {e}")
            # Return defaults for all items in batch on API error
            return [{'answered': False, 'correct': False}] * batch_size

    # --- Modified Evaluation Function ---
    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        """
        Evaluates the generated sales report based on whether questions were
        answered and whether the answers were correct, using batched parallel evaluation.
        """
        metadata = kwargs.get('metadata', {})
        # Retrieve the list of Q&A pairs stored during prompt generation
        qa_pairs = metadata.get('qa_pairs', [])

        # Initialize results structure with new metrics
        results = {
            "answered_rate": 0.0,
            "correctness_rate": 0.0,
            "words": self._count_words(response),
            "total_score": 0.0,        
            "answered_count": 0,
            "correct_count": 0,
            "total_questions": len(qa_pairs),
        }

        if not qa_pairs:
            print("[WARN] No Q&A pairs found in metadata for evaluation.")
            # Return only registered metrics + counts, ensuring keys exist
            final_results = {k: results.get(k, 0.0) for k in self.registered_metrics} # Default to 0.0 for rates
            final_results["answered_count"] = 0
            final_results["correct_count"] = 0
            final_results["total_questions"] = 0
            return final_results

        answered_count = 0
        correct_count = 0
        tasks_data = [] # This will hold the batches of Q&A pairs

        # Create batches of Q&A pairs
        for i in range(0, len(qa_pairs), self.batch_size):
            batch = qa_pairs[i : i + self.batch_size]
            if batch:
                tasks_data.append(batch) # Add the actual batch data

        total_batches = len(tasks_data)
        processed_batches = 0

        # Use ThreadPoolExecutor for parallel batch execution
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                # Pass the qa_batch to the new evaluation function
                executor.submit(self._evaluate_qa_batch, batch, response): batch
                for batch in tasks_data
            }

            print(f"Evaluating {len(qa_pairs)} Q&A pairs in {total_batches} batches (batch size: {self.batch_size}) using {self.max_workers} workers...")

            # Use tqdm for progress bar based on total Q&A pairs
            with tqdm(total=len(qa_pairs), desc="Evaluating Q&A Pairs", unit="pair") as pbar:
                for future in as_completed(future_to_batch):
                    batch_ref = future_to_batch[future] # Get which batch this was
                    try:
                        # Result is a list of {'answered': bool, 'correct': bool} dicts
                        batch_results: List[Dict[str, bool]] = future.result()

                        # Aggregate counts from the batch results
                        for item_result in batch_results:
                            if item_result.get('answered', False):
                                answered_count += 1
                            if item_result.get('correct', False):
                                correct_count += 1 # Correctness implies answered

                        processed_batches += 1
                        pbar.update(len(batch_ref)) # Update progress bar by batch size

                    except Exception as exc:
                         print(f'[ERROR] Batch processing generated an exception: {exc} for a batch of size {len(batch_ref)}')
                         pbar.update(len(batch_ref)) # Still update progress for the failed batch items

        # Final calculations
        if results["total_questions"] > 0:
            results["answered_rate"] = round(answered_count / results["total_questions"], 4)
            # Correctness rate is based on total questions, not just answered ones
            results["correctness_rate"] = round(correct_count / results["total_questions"], 4)
            results["answered_count"] = answered_count
            results["correct_count"] = correct_count
        else:
             # This case should be caught earlier, but included for safety
             results["answered_rate"] = 0.0
             results["correctness_rate"] = 0.0
             results["answered_count"] = 0
             results["correct_count"] = 0


        if results["answered_rate"] > 1e-9 and results["correctness_rate"] > 1e-9:      
            results["total_score"] = 2.0 / (1.0/results["answered_rate"] + 1.0/results["correctness_rate"])
        else:
            results["total_score"] = 0.0

        print(f"[INFO] Evaluation Complete: "
              f"Answered: {answered_count}/{results['total_questions']} ({results['answered_rate']:.2%}), "
              f"Correct: {correct_count}/{results['total_questions']} ({results['correctness_rate']:.2%}). "
              f"Word count: {results['words']}")

        # Return only the registered metrics plus the counts
        final_results = {k: results.get(k, 0.0) for k in self.registered_metrics} # Default to 0.0 for rates
        final_results["answered_count"] = results["answered_count"]
        final_results["correct_count"] = results["correct_count"]
        final_results["total_questions"] = results["total_questions"]
        return final_results


# Register the task with the factory
TaskFactory.register_task('SALES_REPORT_GENERATION', SalesReporterTask)