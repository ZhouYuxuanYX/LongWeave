# code_fix_task.py
import ast
import os
import sys
import re
import tempfile
import subprocess
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from argparse import Namespace
import math

# Import flake8 API
try:
    from flake8.api import legacy as flake8_api
    from flake8.formatting import base as flake8_base
    FLAKE8_AVAILABLE = True
except ImportError:
    print("[WARN] flake8 library not found. Evaluation will skip flake8 checks. Install with: pip install flake8")  # Only flake8 needed now
    FLAKE8_AVAILABLE = False

from core.tasks.base_task import BaseTask, TaskFactory

# Import unified API for LLM calls
try:
    from core.serve.unified_api import unified_call
    UNIFIED_API_AVAILABLE = True
except ImportError:
    print("[WARN] core.serve.unified_api not found. Relevance check (LLM as Judge) will be skipped.")
    UNIFIED_API_AVAILABLE = False

from core.tasks.base_task import BaseTask, TaskFactory

# ---------------------------------------------------------------------------
# Flake8 Custom Reporter (Simplified)
# ---------------------------------------------------------------------------
if FLAKE8_AVAILABLE:

    class Flake8ViolationCollector(flake8_base.BaseFormatter):
        """Light‑weight reporter that just collects all violations."""

        def __init__(self, options):
            super().__init__(options)
            self._errors: List[dict] = []

        # -- formatter API ---------------------------------------------------
        def start(self):
            self._errors = []

        def handle(self, error):
            # Store all errors for potential debugging / later aggregation
            self._errors.append({
                "line": error.line_number,
                "col": error.column_number,
                "code": error.code,
                "message": error.text,
            })

        # -- helper properties ----------------------------------------------
        @property
        def violations(self) -> List[dict]:
            """Returns the list of *all* flake8 violations, sorted by location."""
            return sorted(self._errors, key=lambda v: (v["line"], v["col"]))

# ---------------------------------------------------------------------------
# flake8 helper
# ---------------------------------------------------------------------------
DEFAULT_FLAKE8_SELECT = ["E", "W", "F", "B", "N", "SIM", "C4"]

def run_flake8_check(
    filename: str,
    select_prefixes: Optional[List[str]] = None,
) -> Tuple[int, List[dict]]:
    """Run flake8 and return *(violation_count, detailed_list)*.

    Counts **all** reported violations whose code starts with a prefix in
    *select_prefixes* (defaults to E, W, F, B, N, SIM, C4). If flake8 isn't
    available or an error occurs, returns ``(-1, [])``.
    """
    if not FLAKE8_AVAILABLE:
        return -1, []
    if not (os.path.exists(filename) and os.path.isfile(filename)):
        print(f"[ERROR] Flake8 Check: File missing: {filename}", file=sys.stderr)
        return -1, []

    prefixes = select_prefixes or DEFAULT_FLAKE8_SELECT

    try:
        style_guide = flake8_api.get_style_guide(select=prefixes, quiet=2)
        style_guide.init_report(Flake8ViolationCollector)
        style_guide.check_files([filename])
        collector: Flake8ViolationCollector = style_guide._application.formatter
        total_count = len(collector.violations)
        return total_count, collector.violations
    except Exception as exc:
        print(f"[ERROR] Flake8 Check failed on {filename}: {exc}", file=sys.stderr)
        return -1, []


# ---------------------------------------------------------------------------
# generic helpers - Unchanged from original
# ---------------------------------------------------------------------------
def check_code_runnable(code_string: str) -> bool:
    """Return ``True`` if *code_string* compiles successfully."""
    try:
        compile(code_string, "<string>", "exec")
        return True
    except SyntaxError:
        return False
    # Be slightly more specific about exceptions if possible, but broad Exception is okay here
    except Exception as e:
        # Optionally log the specific error for debugging
        # print(f"[DEBUG] check_code_runnable failed with: {type(e).__name__}: {e}")
        return False

# ---------------------------------------------------------------------------
# AST Helper to Count Functions
# ---------------------------------------------------------------------------
def count_top_level_functions(code_string: str) -> int:
    """Counts top-level function definitions (def/async def) using AST.

    Returns:
        The number of top-level functions found, or 0 if parsing fails
        (e.g., due to syntax errors) or the code is empty.
    """
    if not code_string.strip():
        return 0
    count = code_string.count('def ')
    return count

# ---------------------------------------------------------------------------
# Main Task Class
# ---------------------------------------------------------------------------
class CODE_FIXING(BaseTask):
    """Evaluate an LLM's ability to fix Python code (syntax + style),
       including a relevance check to prevent off-topic responses."""

    # Metrics - Unchanged
    registered_metrics = [
        "runnable_ratio",
        "style_quality_score", # New metric: absolute style quality of fixed code
        "func_count_score",
        "total_score",
    ]

    LENGTH_TO_SUFFIX_MAP = {1024: "1k", 2048: "2k", 4096: "4k", 8192: "8k"}

    # Construction helpers - Added evaluation_model_config
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.task_path = config.get("task_path", "")
        self.task_specific_config = config

        # --- Added: Store evaluation model configuration ---
        self.evaluation_model_config = config.get('evaluation_model')
        # Determine judge availability based on API and non-dummy config
        self.judge_available = (
            UNIFIED_API_AVAILABLE and
            self.evaluation_model_config and
            self.evaluation_model_config.get('backend') != 'dummy' and
            self.evaluation_model_config.get('model') # Ensure model is also specified
        )

        if not self.judge_available:
            print("[WARN] CODE_FIXING: LLM-based relevance check is disabled (unified_api not available or evaluation_model not configured/dummy).")
        # --- End Added ---

    # --- Helper methods _get_task_suffix_from_path, _map_length_to_suffix, _get_data_path_for_sample, _read_python_code remain the same ---
    def _get_task_suffix_from_path(self) -> Optional[str]:
        parts = self.task_path.split("/")
        return parts[-1] if len(parts) > 1 else None

    def _map_length_to_suffix(self, test_length: int) -> Optional[str]:
        return self.LENGTH_TO_SUFFIX_MAP.get(test_length)

    def _get_data_path_for_sample(self, sample_id: str, test_length: Optional[int] = None) -> str:
        derived_suffix = None
        if test_length is not None:
            try:
                test_length = int(test_length)
                derived_suffix = self._map_length_to_suffix(test_length)
            except ValueError:
                pass # Keep derived_suffix as None if conversion fails
        if not derived_suffix:
            derived_suffix = self._get_task_suffix_from_path()
        if not derived_suffix:
            # Provide more context in the error message
            raise ValueError(
                f"Could not determine data suffix for sample '{sample_id}' "
                f"(task_path: '{self.task_path}', test_length: {test_length})."
            )

        config_key_name = f"pep8_data_path_{derived_suffix}"
        base_data_dir = self.task_specific_config.get(config_key_name)
        if not base_data_dir:
            raise ValueError(
                f"Missing data path configuration for suffix '{derived_suffix}'. "
                f"Looked for key '{config_key_name}' in task config: {self.task_specific_config.keys()}"
            )
        try:
            # Handle potential prefix before the index (e.g., "sample_1", "item_001")
            sample_idx_str = sample_id.split("_")[-1]
            sample_idx = int(sample_idx_str)
        except (ValueError, IndexError):
            raise ValueError(f"Could not parse sample index from sample_id '{sample_id}'. Expected format like 'prefix_index'.")

        code_filename = f"file_{sample_idx:03d}.py"
        return os.path.join(base_data_dir, code_filename)

    @staticmethod
    def _read_python_code(file_path: str) -> str:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            # Keep error messages concise for the relevance check prompt
            return f"# ERROR: Original file not found at path: {file_path}"
        except Exception as e:
            return f"# ERROR: Failed to read original file: {e}"


    # Prompt generation / response cleaning - Unchanged from previous version (includes original_code in metadata)
    def clean_code_extraction(self, response: str) -> str:
        """Extracts the Python code block from an LLM response."""
        response = response.strip()
        # Improved regex to handle optional language specifier and surrounding whitespace
        match = re.search(r"```(?:python)?\s*(.*?)\s*```", response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        else:
            # If no markdown block found, assume the whole response is code, but be cautious.
            lines = response.strip().split('\n')
            # Check if it looks like code (starts with common keywords or minimal leading prose)
            if lines and (lines[0].startswith(('import ', 'def ', 'class ', '@', '#', '"""', "'''")) or not re.search(r'[a-zA-Z]{4,}\s', lines[0])): # Avoid lines clearly starting with prose
                 return response.strip()
            else:
                 print(f"[WARN] Could not reliably extract Python code block from response starting with: {response[:100]}...")
                 return response.strip() # Return stripped response as fallback

    def generate_prompt(self, **kwargs) -> Tuple[str, Dict]:
        sample_id = kwargs.get("sample_id")
        test_length = kwargs.get("test_length")
        if not sample_id:
            raise ValueError("Requires 'sample_id'.")

        # Allow test_length to be passed as string or int
        if test_length is not None:
            try:
                test_length = int(test_length)
            except (ValueError, TypeError):
                 print(f"[WARN] Invalid test_length '{test_length}' provided for sample {sample_id}. Ignoring.")
                 test_length = None

        code_file_path = self._get_data_path_for_sample(sample_id, test_length=test_length)
        original_code = self._read_python_code(code_file_path) # Read the code here

        prompt_lines = [
            "**Role:** Python Developer",
            "",
            "**Task:** You are given a Python code file that may contain syntax errors or violate style guidelines. Your goal is to fix the code so that it is **runnable** and complies with the following coding standards:",
            "",
            "**FLAKE8 CATEGORIES TO CHECK:**",
            "- **E / W – pycodestyle**  \n  Basic PEP 8 formatting errors (E) and warnings (W), such as inconsistent indentation (E111), extra spaces (E221), or line length violations (E501).",
            "- **F – Pyflakes**  \n  Potential runtime issues, e.g., undefined names (F821) or unused imports/variables (F401).",
            "- **B – flake8-bugbear**  \n  Code patterns prone to bugs or pitfalls, like modifying a list while iterating (B007) or using mutable default arguments (B008).",
            "- **N – pep8-naming**  \n  Naming convention violations, such as function names not in snake_case (N802) or class names not in CamelCase (N801).",
            "- **SIM – flake8-simplify**  \n  Suggestions to simplify and streamline code, for instance redundant `if x == True` checks (SIM102) or favoring `dict.get` over manual key checks (SIM108).",
            "- **C4 – flake8-comprehensions**  \n  Best practices around comprehensions: avoid unnecessary list() wrappers (C400) or use dict comprehensions instead of `dict()` calls with generator expressions (C401).",
            "",
            "**Input Python Code:**",
            "# --- START OF CODE ---",
            "```python",
            original_code.strip(), # Use the read code
            "```",
            "# --- END OF CODE ---",
            "",
            "**Instructions:**",
            "- **Fix Syntax Errors:** Ensure the code is valid Python.",
            "- **Correct Style Violations:** Fix all style issues under the categories above.",
            "- **Preserve Functionality:** Keep the original behavior, **keep the number of functions unchanged**, prioritize runnability.",
            "- **Output Only Code:** Return *only* the complete, corrected Python code within a single ```python block, without any explanations before or after.",
            "",
            "**Complete, Corrected Python Code:**",
             # Ensure the model starts its response correctly for extraction
            "```python"
        ]
        prompt = "\n".join(prompt_lines)

        metadata = {
            "original_file_path": code_file_path,
            "original_code": original_code, # Store original code for relevance check
            "selected_suffix": self._get_task_suffix_from_path() or "unknown", # Use helper
            "requested_test_length": test_length,
        }
        return prompt, metadata

    # Evaluation Helpers - _improvement_ratio and _compute_fix_ratio remain the same as original/previous
    def _improvement_ratio(self, remaining: int, initial: int) -> float:
        """Return (initial ‑ remaining) / initial when possible, else ‑1.0."""
        # Original logic:
        if initial <= 0 or remaining < 0:
            return -1.0
        return (initial - remaining) / initial
        # Note: This original version returns -1.0 if initial is 0, which might be okay depending on interpretation.
        # The _compute_fix_ratio handles this better.

    def _compute_fix_ratio(self, remaining: int, initial: int) -> float:
        """
        Maps improvement to [0,1]. Handles edge cases.
        - 1.0: Perfect fix (0 remaining) or initially 0 and still 0.
        - 0.5: No change in violation count.
        - <0.5: Code got worse.
        - 0.0: Calculation failed (negative counts) or initial=0 but remaining>0.
        """
        # Handle error cases / invalid inputs from flake8
        if initial < 0 or remaining < 0:
            print(f"[DEBUG] compute_fix_ratio received negative counts: initial={initial}, remaining={remaining}. Returning 0.0.")
            return 0.0

        # Handle perfect initial state or perfect fix
        if initial == 0:
            # If code was perfect, any added violations make it worse (score 0)
            # If it stays perfect, score is 1.0
            return 1.0 if remaining == 0 else 0.0
        if remaining == 0:
             # If remaining is 0, it's a perfect fix relative to initial > 0
             return 1.0

        # Normalize score based on relative change (scaled to 0-1, 0.5=no change)
        # (initial - remaining) / (initial + remaining) maps change to [-1, 1]
        score_raw = (initial - remaining) / (initial + remaining)
        score_scaled = 0.5 * score_raw + 0.5

        # Clamp score to [0, 1]
        return max(0.0, min(1.0, score_scaled))


    # --- Added: LLM as Judge for Relevance (Unchanged from previous version) ---
    def _parse_relevance_response(self, response: str, key: str = "is_relevant_and_complete") -> bool:
        """Parses the LLM response to extract the boolean judgment for the specified key."""
        try:
            # Attempt to find JSON object within the response
            pattern = r'\{.*?"' + re.escape(key) + r'"\s*:\s*(true|false).*?\}'
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                json_str = match.group(0)
                data = json.loads(json_str)
                result = data.get(key)
                if isinstance(result, bool):
                    return result
                else:
                    print(f"[WARN] Parsed JSON but '{key}' is not boolean: {data}")
            else:
                # Fallback: Check for simple true/false strings if JSON fails
                response_lower = response.lower()
                true_pattern = f'"{key}": true'
                false_pattern = f'"{key}": false'
                if true_pattern in response_lower: return True
                if false_pattern in response_lower: return False
                print(f"[WARN] Could not parse relevance/completeness JSON or find clear boolean fallback in response: {response[:100]}...")

        except json.JSONDecodeError as e:
            print(f"[WARN] JSONDecodeError parsing relevance/completeness response: {e} - Response: {response[:100]}...")
        except Exception as e:
            print(f"[WARN] Unexpected error parsing relevance/completeness response: {e} - Response: {response[:100]}...")

        print(f"[WARN] Defaulting {key} to True due to parsing failure.")
        return True # Default to relevant/complete if parsing fails

    def _check_relevance_with_llm(self, original_code: str, fixed_code: str) -> bool:
        """
        Uses an LLM to evaluate if the fixed_code is a relevant and complete code response
        attempting to modify/fix the original_code.
        Returns True if deemed relevant and complete, False otherwise or if the check fails.
        """
        if not self.judge_available:
            return True # Assume relevant if judge cannot run

        eval_prompt = f"""
        **Task:** Evaluate if 'FIXED CODE' is a relevant and complete code response to the 'ORIGINAL CODE'.

        **Evaluation Criteria:**

        1.  **Content Relevance (Answering the Request):**
            *   **Relevant (true):** The 'FIXED CODE' directly attempts to modify, fix, or refactor the provided 'ORIGINAL CODE'. It addresses the implicit request to correct or improve the original snippet.
            *   **Not Relevant (false):** The 'FIXED CODE' does not address the original code. It might be unrelated code, a refusal, a question, an explanation *instead* of code, or generic template/placeholder code not adapted to the original.

        2.  **Code Completeness (Providing Full Code):**
            *   **Complete (true):** The 'FIXED CODE' provides a full, runnable (or intended to be runnable) Python code snippet that represents the proposed modification or fix. It's not just a fragment, comment, or instruction.
            *   **Incomplete (false):** The 'FIXED CODE' is not a complete code solution. It might be:
                *   Only an explanation or commentary about the fix.
                *   A code fragment (e.g., only a single corrected line without context).
                *   Instructions on how to fix the code (e.g., "You should change line 5 to...").
                *   An empty response or placeholder like "# [Your corrected code here]".

        **Overall Judgment:**
        Return `true` ONLY IF BOTH criteria (Content Relevance AND Code Completeness) are met. Otherwise, return `false`.

        **ORIGINAL CODE:**
        ```python
        {original_code.strip()}
        ```

        **FIXED CODE:**
        ```python
        {fixed_code.strip()}
        ```

        **Output:** Respond ONLY with JSON: {{"is_relevant_and_complete": true/false}}
        """ # Changed key name for clarity

        try:
            eval_response = unified_call(
                backend=self.evaluation_model_config['backend'],
                model=self.evaluation_model_config['model'],
                prompt=eval_prompt,
                **self.evaluation_model_config.get('params', {})
            ).strip()

            # IMPORTANT: Update _parse_relevance_response to look for the new key
            # "is_relevant_and_complete" instead of "is_related".
            # Assuming self._parse_relevance_response is adapted for the new key:
            is_relevant_and_complete = self._parse_relevance_response(eval_response, key="is_relevant_and_complete")

            # print(f"[DEBUG] Relevance & Completeness check result: {is_relevant_and_complete}") # Optional debug
            return is_relevant_and_complete

        except Exception as e:
            print(f"[ERROR] LLM call failed during relevance/completeness check: {e}")
            print("[WARN] Assuming code is relevant/complete due to check LLM failure.")
            return True # Default to True if judge fails

    # Evaluation - Modified to include relevance check, uses restored run_flake8_check
    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        metadata = kwargs.get("metadata", {})
        sample_id = kwargs.get("sample_id", "N/A")
        original_file_path = metadata.get("original_file_path") # Still needed for context/debugging
        original_code = metadata.get("original_code")

        # -------- Initialise result dict with defaults for NEW metrics ---
        results = {metric: 0.0 for metric in self.registered_metrics}

        # --- Basic Checks (keep as is) ---
        if not original_file_path:
            print(f"[ERROR] Eval failed for {sample_id}: Original file path missing. Returning zero scores.")
            return results
        # Don't check if original file exists/readable here, as we don't run flake8 on it anymore
        # Check original_code exists for relevance check
        if original_code is None:
             print(f"[WARN] Original code missing in metadata for sample {sample_id}. Reading file again for relevance check.")
             if original_file_path: # Only attempt read if path exists
                 original_code = self._read_python_code(original_file_path)
             if not original_code or original_code.startswith("# ERROR"):
                  print(f"[ERROR] Eval failed for {sample_id}: Could not get original code for relevance check. Returning zero scores.")
                  return results

        # --- Extract Response Code (keep as is) ---
        cleaned_response = self.clean_code_extraction(response)
        if not cleaned_response:
            print(f"[INFO] No code found in response for sample {sample_id}. Assigning zero scores.")
            return results

        # --- Relevance Check (keep as is) ---
        is_relevant = self._check_relevance_with_llm(original_code, cleaned_response)
        if not is_relevant:
            print(f"[INFO] Response deemed irrelevant by judge LLM for sample {sample_id}. Assigning zero scores.")
            return results

        # --- Proceed with evaluation only if relevant ---
        # print(f"[INFO] Response relevant for sample {sample_id}. Proceeding...") # Optional

        # --- REMOVED: Initial style violations check ---
        # We no longer run flake8 on the original file

        # 1) runnable check (keep as is) -------------------------------------
        is_runnable = check_code_runnable(cleaned_response)
        results["runnable_ratio"] = 1.0 if is_runnable else 0.0

        # 2) remaining style violations (keep as is) -------------------------
        remaining_count = -1 # Default if not runnable or flake8 fails/unavailable
        remaining_details = []
        temp_file_path: Optional[str] = None

        if is_runnable and FLAKE8_AVAILABLE:
            try:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as tmp:
                    temp_file_path = tmp.name
                    tmp.write(cleaned_response)
                    tmp.flush()
                remaining_count, remaining_details = run_flake8_check(temp_file_path)
                if remaining_count < 0:
                     print(f"[WARN] Flake8 check failed on the *generated* code for sample {sample_id}.")
            except Exception as e:
                 print(f"[ERROR] Failed temp file/flake8 check for remaining violations (Sample: {sample_id}): {e}")
                 remaining_count = -1
            finally:
                if temp_file_path and os.path.exists(temp_file_path):
                    try: os.remove(temp_file_path)
                    except OSError as e_rem: print(f"[ERROR] Failed remove temp file {temp_file_path}: {e_rem}")
        # else: # Log if skipping (optional, can be verbose)
            # if not is_runnable: print(f"[DEBUG] Skipping remaining flake8 check for {sample_id} (not runnable).")
            # elif not FLAKE8_AVAILABLE: print(f"[DEBUG] Skipping remaining flake8 check for {sample_id} (flake8 unavailable).")

        # 3) --- NEW: Calculate Style Quality Score ---
        # Score = 1 / (1 + num_violations). 1.0 is perfect, decreases with more violations.
        # If flake8 failed (remaining_count == -1) or code wasn't runnable, score is 0.
        if remaining_count < 0:
            results["style_quality_score"] = 0.0
        else:
            k = 50.0                        # Empirical value: around 20 allows 0→1, 20→0.5, 100→0.167
            results["style_quality_score"] = 1.0 / (1.0 + remaining_count / k)

        # 4) Calculate Function Count Score (keep as is) ---------------------
        try:
            original_func_count = count_top_level_functions(original_code)
            fixed_func_count = count_top_level_functions(cleaned_response)
            func_diff = abs(fixed_func_count - original_func_count)
            # Allow ±25% difference, minimum scale of 1
            scale_func = max(1.0, float(original_func_count) * 0.25)
            # Sigmoid-like penalty: score = 1 / (1 + (diff/scale)^2)
            func_count_score = 1.0 / (1.0 + (func_diff / scale_func) ** 2)
            results["func_count_score"] = round(func_count_score, 4)
        except Exception as e:
            print(f"[ERROR] Failed to calculate function count score for sample {sample_id}: {e}")
            results["func_count_score"] = 0.0 # Default to 0 on error

        # 5) --- MODIFIED: Calculate Total Score (Harmonic Mean) ---
        # Uses runnable_ratio, style_quality_score, func_count_score
        runnable_r = results["runnable_ratio"]
        style_q = results["style_quality_score"] # Use the new score
        func_s = results["func_count_score"]

        # Calculate harmonic mean for three values: H = 3 / (1/a + 1/b + 1/c)
        # Avoid division by zero if any component score is very close to 0
        if runnable_r > 1e-9 and style_q > 1e-9 and func_s > 1e-9:
            try:
                inv_sum = (1.0 / runnable_r) + (1.0 / style_q) + (1.0 / func_s)
                results["total_score"] = 3.0 / inv_sum if inv_sum > 1e-9 else 0.0
            except ZeroDivisionError: # Should be caught by inv_sum check, but just in case
                print(f"[WARN] Division by zero avoided in harmonic mean calculation for sample {sample_id}.")
                results["total_score"] = 0.0
        else:
             results["total_score"] = 0.0 # If any component is effectively zero, H-mean is zero

        # Optional: Log detailed counts/scores for debugging
        # Note: initial_count is no longer available
        print(f"[DEBUG] Sample {sample_id}: RemainingFlake8={remaining_count}, OrigFunc={original_func_count}, FixedFunc={fixed_func_count}, Runnable={runnable_r:.2f}, StyleQuality={style_q:.2f}, FuncCount={func_s:.2f}, Total={results['total_score']:.2f}")
        # ----------------------------------------------------------------
        # Return dictionary containing only the registered metrics
        return {k: results.get(k, 0.0) for k in self.registered_metrics}

# ---------------------------------------------------------------------------
# Register the task
# ---------------------------------------------------------------------------
TaskFactory.register_task("CODE_FIXING", CODE_FIXING)