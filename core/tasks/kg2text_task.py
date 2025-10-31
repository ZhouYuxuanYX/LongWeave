# core/tasks/kg2text_task.py

import json
import re
import os
import csv
import glob
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

from core.tasks.base_task import BaseTask, TaskFactory
from core.serve.unified_api import unified_call

class KG2TextTask(BaseTask):
    # --- Metrics reflect SENTENCE coverage now ---
    registered_metrics = ['sentence_coverage_rate', 'words']

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.task_path = config.get('task_path', {})
        self.evaluation_model_config = config.get('evaluation_model')
        # --- Config key for batch size should reflect SENTENCE evaluation ---
        self.batch_size = config.get('sentence_evaluation_batch_size', 10)
        self.max_workers = config.get('evaluation_max_workers', 10)
        # --- Get test_length for generation target AND evaluation truncation ---
        self.test_length = config.get('test_length')

        self.data_base_paths = {
            "1k": config.get("kg2text_data_path_1k"),
            "2k": config.get("kg2text_data_path_2k"),
            "4k": config.get("kg2text_data_path_4k"),
            "8k": config.get("kg2text_data_path_8k"),
        }

        # --- Centralized Cache ---
        self._data_cache: Dict[str, Tuple[List[str],
                                           Dict[str, List[Tuple[str, str, str]]],
                                           Dict[str, List[str]]]] = {}
        self._cache_lock = threading.Lock()
        # --- End Cache ---

        if not self.evaluation_model_config:
            print("[WARN] Evaluation model configuration missing...")
            self.evaluation_model_config = {'backend':'dummy', 'model':'dummy', 'params':{}}

        # --- MODIFIED: Check test_length validity for evaluation truncation ---
        if not isinstance(self.test_length, int) or self.test_length <= 0:
             print(f"[WARN] Invalid 'test_length' ({self.test_length}). Defaulting to 512 for evaluation truncation.")
             self.test_length = 512 # Set a default if invalid or not provided
        else:
             print(f"[INFO] Will truncate LLM responses to the first {self.test_length} words for evaluation.")
        # --- END MODIFIED ---

    def _get_task_suffix(self) -> str:
        # ... (code remains the same) ...
        parts = self.task_path.split('/')
        return parts[-1] if len(parts) > 1 else None

    def _load_data_for_suffix(self, suffix: str) -> Tuple[List[str],
                                                          Dict[str, List[Tuple[str, str, str]]],
                                                          Dict[str, List[str]]]:
        if suffix in self._data_cache:
            return self._data_cache[suffix]

        with self._cache_lock:
            if suffix in self._data_cache:
                return self._data_cache[suffix]

            print(f"[INFO] Loading KG2Text data (Triples & Sentences) for suffix '{suffix}'...")
            base_path = self.data_base_paths.get(suffix)
            if not base_path or not os.path.isdir(base_path):
                print(f"[ERROR] Base data directory not found for suffix '{suffix}': {base_path}")
                self._data_cache[suffix] = ([], {}, {})
                return [], {}, {}

            triples_dir = os.path.join(base_path, "triples")
            sentences_dir = os.path.join(base_path, "sentences") # Path to sentences

            if not os.path.isdir(triples_dir):
                print(f"[WARN] Triples directory not found at {triples_dir}.")
                self._data_cache[suffix] = ([], {}, {})
                return [], {}, {}
            if not os.path.isdir(sentences_dir):
                 print(f"[WARN] Sentences directory not found at {sentences_dir}. Sentence evaluation will fail.")

            slugs = []
            slug_to_triples = {}
            slug_to_sentences = {}
            error_count_triples = 0
            error_count_sentences = 0

            try:
                # Discover slugs based on triples files (primary source)
                tsv_files = glob.glob(os.path.join(triples_dir, "*_triples.tsv"))
                print(f"[INFO] Found {len(tsv_files)} triple files in {triples_dir}.")
                if not tsv_files:
                     print(f"[WARN] No triple files found, cannot load data for suffix '{suffix}'.")
                     self._data_cache[suffix] = ([], {}, {})
                     return [], {}, {}

                # Populate slug list first
                for tsv_file_path in tsv_files:
                     filename = os.path.basename(tsv_file_path)
                     if filename.endswith("_triples.tsv"):
                         slug = filename[:-len("_triples.tsv")]
                         if slug: slugs.append(slug)

                # Sort slugs
                slugs.sort(key=lambda s: int(s.split('_')[0]) if s.split('_')[0].isdigit() else float('inf'))
                print(f"[INFO] Discovered and sorted {len(slugs)} slugs for suffix '{suffix}'.")

                # Now load triples and sentences for each slug
                for slug in tqdm(slugs, desc=f"Loading data ({suffix})", unit="slug"):
                    # Load Triples
                    tsv_file_path = os.path.join(triples_dir, f"{slug}_triples.tsv")
                    triples = []
                    try:
                        with open(tsv_file_path, 'r', encoding='utf-8', newline='') as f:
                            reader = csv.reader(f, delimiter='\t')
                            try: header = next(reader)
                            except StopIteration: continue # Skip empty
                            for i, row in enumerate(reader):
                                if len(row) == 3:
                                    subj, pred, obj = map(str.strip, row)
                                    if subj and pred and obj: triples.append((subj, pred, obj))
                        slug_to_triples[slug] = triples
                    except FileNotFoundError: # Should not happen if discovered via glob
                        print(f"[WARN] Triple file disappeared? {tsv_file_path}")
                        error_count_triples += 1
                        slug_to_triples[slug] = []
                    except Exception as e_read_t:
                        print(f"[ERROR] Failed to read/parse triples from {tsv_file_path}: {e_read_t}")
                        error_count_triples += 1
                        slug_to_triples[slug] = []

                    # Load Sentences
                    json_file_path = os.path.join(sentences_dir, f"{slug}_sentences.json")
                    sentences = []
                    try:
                        if os.path.exists(json_file_path):
                            with open(json_file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                # Ensure sentences are a list of strings
                                loaded_sentences = data.get("sentences", [])
                                if isinstance(loaded_sentences, list):
                                     sentences = [str(s) for s in loaded_sentences if isinstance(s, (str, int, float)) and str(s).strip()] # Convert to str, filter empty
                                else:
                                     print(f"[WARN] 'sentences' key in {json_file_path} is not a list.")
                        # else: # File doesn't exist, sentences remains []
                        #    print(f"[DEBUG] Sentences file not found: {json_file_path}")
                        slug_to_sentences[slug] = sentences
                    except Exception as e_read_s:
                        print(f"[ERROR] Failed to read/parse sentences from {json_file_path}: {e_read_s}")
                        error_count_sentences += 1
                        slug_to_sentences[slug] = [] # Store empty on error

                print(f"[INFO] Data loading complete for suffix '{suffix}'. Triple errors: {error_count_triples}, Sentence errors: {error_count_sentences}.")
                self._data_cache[suffix] = (slugs, slug_to_triples, slug_to_sentences)
                return slugs, slug_to_triples, slug_to_sentences

            except Exception as e_glob:
                print(f"[ERROR] Failed during data loading for suffix '{suffix}': {e_glob}")
                self._data_cache[suffix] = ([], {}, {})
                return [], {}, {}
    # --- End Core Data Loading Function ---

    def _get_slug_triples_and_sentences(self, sample_id: str) -> Tuple[str,
                                                                        List[Tuple[str, str, str]],
                                                                        List[str]]:
        try:
            base_id_part = sample_id.split('/')[-1]
            parts = base_id_part.split('_', 1)
            if len(parts) != 2: raise ValueError("Cannot split suffix and index/slug part.")
            suffix, index_or_slug_part = parts[0], parts[1]

            try: sample_idx = int(index_or_slug_part)
            except ValueError:
                 numeric_part = index_or_slug_part.split('_')[0]
                 if numeric_part.isdigit(): sample_idx = int(numeric_part)
                 else: raise ValueError(f"Cannot extract numeric index from part '{index_or_slug_part}'")

            if suffix not in self.data_base_paths: raise ValueError(f"Suffix '{suffix}' not valid.")
        except (IndexError, ValueError) as e:
            raise ValueError(f"Could not parse suffix and index from sample_id '{sample_id}'. Error: {e}")

        # Load data (uses cache)
        slug_list, slug_to_triples, slug_to_sentences = self._load_data_for_suffix(suffix)

        # Get slug
        if not slug_list: raise ValueError(f"No slugs loaded for suffix '{suffix}'.")
        if not (0 <= sample_idx < len(slug_list)):
            raise IndexError(f"Sample index {sample_idx} out of bounds for slugs (len {len(slug_list)}) for suffix '{suffix}'.")
        character_slug = slug_list[sample_idx]

        # Get triples and sentences using the slug
        triples = slug_to_triples.get(character_slug, [])
        sentences = slug_to_sentences.get(character_slug, [])

        if not triples: print(f"[WARN] No triples found for slug '{character_slug}' (Index: {sample_idx})")
        if not sentences: print(f"[WARN] No sentences found for slug '{character_slug}' (Index: {sample_idx})")

        return character_slug, triples, sentences
    # --- End MODIFIED Helper ---

    def _count_words(self, text: str) -> int:
        """Counts words in the given text using word boundary regex."""
        words = re.findall(r'\b\w+\b', text.lower())
        return len(words)

    # --- Helper function to get the first N words of text ---
    def _get_first_n_words_text(self, text: str, n: int) -> str:
        """
        Extracts the first N words from text and joins them back.
        Uses the same word definition as _count_words, but keeps original case for reconstruction.
        """
        # Use the same regex definition of a word as _count_words, but keep original case
        words = re.findall(r'\b\w+\b', text)
        if n <= 0:
            return ""
        truncated_words = words[:n]
        # Join the words with a single space
        return ' '.join(truncated_words)

    def generate_prompt(self, **kwargs) -> Tuple[str, Dict]:
        # ... (code remains the same, prompt still asks for test_length) ...
        sample_id = kwargs.get('sample_id')
        if sample_id is None:
            raise ValueError("KG2TextTask.generate_prompt requires 'sample_id'.")

        try:
            # Get slug, triples (for prompt), and sentences (for metadata)
            character_slug, input_triples, target_sentences_for_eval = self._get_slug_triples_and_sentences(str(sample_id))
        except (ValueError, IndexError) as e:
             print(f"[ERROR] Failed to get data for sample_id '{sample_id}': {e}")
             error_prompt = f"Error: Could not load input data for sample ID '{sample_id}'. Details: {e}"
             # Return empty sentences in metadata for consistency
             return error_prompt, {'target_sentences': []}

        if not input_triples:
            print(f"[WARN] No input triples loaded for sample_id '{sample_id}' (Slug: {character_slug}). Prompt facts will be empty.")
        # We also need the sentences for evaluation later
        if not target_sentences_for_eval:
             print(f"[WARN] No target sentences loaded for sample_id '{sample_id}' (Slug: {character_slug}). Evaluation coverage will be 0.")


        # Format *Triples* for the prompt
        formatted_triples = ""
        if input_triples:
            formatted_triples_list = [f"- {s} - {p} - {o}" for s, p, o in input_triples]
            formatted_triples = "\n".join(formatted_triples_list)
        else:
             formatted_triples = "- (No valid input facts provided)"

        # Construct the prompt (still based on triples)
        prompt = f"""\
**Role:** Biographer / Content Writer

**Task:** Write a coherent and readable biography about the entity associated with the slug '{character_slug}'.
Your biography must be based **exclusively** on the factual statements provided below in Subject-Predicate-Object (Triple) format. Combine the facts naturally into a narrative.

**Input Facts (Triples):**
{formatted_triples}

**Writing Style:**
Produce a well-structured paragraph or paragraphs. Ensure smooth transitions between facts where possible. The tone should be informative and neutral.

**Required Content:**
Ensure that the core information from *each* of the input triples is included in your generated biography.

**Length Specifications (TARGET WORD COUNT):**
- The biography should be **around {self.test_length} words**. Strive for this length, but prioritize covering all facts accurately.

You may now begin writing the biography based on the provided triples around {self.test_length} words:
"""

        # Store *Sentences* in metadata for evaluation
        metadata = { 'target_sentences': target_sentences_for_eval }
        return prompt, metadata

    def _parse_batch_coverage_response(self, response: str, batch_size: int) -> List[bool]:
        # ... (code remains the same) ...
        try:
            match = re.search(r'(\[[^\]\[]*\])', response.replace("'", '"'))
            if match:
                json_str = match.group(1)
                json_str = re.sub(r'^```json\s*|\s*```$', '', json_str).strip()
                parsed_list = json.loads(json_str)
                if isinstance(parsed_list, list) and all(isinstance(item, bool) for item in parsed_list):
                    if len(parsed_list) == batch_size: return parsed_list
                    elif len(parsed_list) < batch_size: return parsed_list + [False] * (batch_size - len(parsed_list))
                    else: return parsed_list[:batch_size]
                else: # Try converting string representations
                    bool_list = []
                    conv_ok = True
                    for item in parsed_list:
                        if isinstance(item, str):
                            item_lower = item.strip().lower()
                            if item_lower == 'true': bool_list.append(True)
                            elif item_lower == 'false': bool_list.append(False)
                            else: conv_ok = False; break
                        elif isinstance(item, bool): bool_list.append(item)
                        else: conv_ok = False; break
                    if conv_ok and bool_list:
                         if len(bool_list) == batch_size: return bool_list
                         elif len(bool_list) < batch_size: return bool_list + [False] * (batch_size - len(bool_list))
                         else: return bool_list[:batch_size]
                    else: raise ValueError(f"Parsed list contains non-booleans or unconvertible strings. Found: {parsed_list}")
            else: # Fallback: Check comma-separated
                 cleaned_response = response.strip()
                 if cleaned_response.lower().startswith('true') or cleaned_response.lower().startswith('false'):
                     potential_bools = []; parse_ok = True
                     for b in cleaned_response.split(','):
                         item_lower = b.strip().lower()
                         if item_lower == 'true': potential_bools.append(True)
                         elif item_lower == 'false': potential_bools.append(False)
                         else: parse_ok = False; break
                     if parse_ok and potential_bools:
                         if len(potential_bools) == batch_size: return potential_bools
                         elif len(potential_bools) < batch_size: return potential_bools + [False] * (batch_size - len(potential_bools))
                         else: return potential_bools[:batch_size]
                 raise ValueError(f"Could not find JSON boolean list or parse comma-sep booleans. Response prefix: '{response[:100]}...'")
        except (json.JSONDecodeError, ValueError, Exception) as e:
            print(f"[ERROR] Failed to parse batch coverage response: {e}. Response: '{response[:200]}...'. Returning all False for batch.")
            return [False] * batch_size

    def _check_sentence_coverage_batch(self, sentence_batch: List[str], biography_text: str) -> List[bool]:
        # ... (code remains the same, it takes the biography text to check against) ...
        if not sentence_batch:
            return []

        batch_size = len(sentence_batch)
        # Format SENTENCES with numbers for the prompt
        numbered_sentences = "\n".join([f"{i+1}. {sent}" for i, sent in enumerate(sentence_batch)])

        # Modify the evaluation prompt to ask about SENTENCES
        # Note: biography_text here is the (potentially truncated) text passed in
        eval_prompt = f"""
        **Task:** Evaluate if the core factual information conveyed by each numbered 'Target Sentence' below is accurately and adequately covered or represented, either directly or semantically, within the provided 'Generated Biography Text'.

        **Generated Biography Text:**
        --- START BIOGRAPHY ---
        {biography_text}
        --- END BIOGRAPHY ---

        **Target Sentences (Facts to find):**
        {numbered_sentences}

        **Evaluation Criteria:**
        For each numbered target sentence (from 1 to {batch_size}), determine if its essential factual statement is present in the 'Generated Biography Text'. Exact wording is not required, but the core fact must be included in the biography. Judge based *only* on the presence of the information, not the writing style or fluency. Answer 'true' if the fact is present, 'false' otherwise.

        **Output Format:**
        Respond ONLY with a single JSON list containing boolean values (true/false), corresponding *in order* to the numbered Target Sentences (1 to {batch_size}). The list must have exactly {batch_size} elements. Do not include any explanations or other text outside the JSON list.

        **Example Output (if batch_size was 3):**
        [true, false, true]

        **Your JSON Output:**
        """
        try:
            eval_response = unified_call(
                backend=self.evaluation_model_config.get('backend', 'dummy'),
                model=self.evaluation_model_config.get('model', 'dummy'),
                prompt=eval_prompt,
                **self.evaluation_model_config.get('params', {})
            ).strip()
            return self._parse_batch_coverage_response(eval_response, batch_size)
        except Exception as e:
            print(f"[ERROR] LLM call failed during batch sentence coverage check: {e}")
            return [False] * batch_size

    # --- evaluate uses target_sentences and sentence metrics, based on TRUNCATED response ---
    # --- MODIFIED: evaluate uses target_sentences and sentence metrics, based on TRUNCATED response ---
    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        """
        Evaluates the generated biography based on coverage of target **sentences**,
        using only the first `self.test_length` words of the response.
        """
        metadata = kwargs.get('metadata', {})
        # --- Get SENTENCES from metadata ---
        target_sentences = metadata.get('target_sentences', [])

        # --- Truncate response for evaluation ---
        original_word_count = self._count_words(response) # Count words in original response
        # Use the helper function to get the truncated text
        truncated_response = self._get_first_n_words_text(response, self.test_length)
        # Calculate word count based *only* on the truncated text that will be evaluated
        evaluated_word_count = self._count_words(truncated_response)

        print(f"[INFO] Evaluating response. Original word count: {original_word_count}. Truncated to {evaluated_word_count} words for evaluation (limit: {self.test_length}).")


        # --- Use SENTENCE metrics, based on the *TRUNCATED* response ---
        results = {
            "sentence_coverage_rate": 0.0,
            "words": original_word_count, 
            "covered_sentences_count": 0,
            "total_sentences_count": len(target_sentences),
        }

        if not target_sentences:
            print("[WARN] No target sentences found in evaluate metadata. Coverage is 0.")
            # Return metrics, ensuring 'words' reflects the (potentially zero) evaluated word count
            return { k: results.get(k, 0) for k in self.registered_metrics + ['covered_sentences_count', 'total_sentences_count']}

        covered_count = 0
        tasks = []
        # Create batches of SENTENCES
        for i in range(0, len(target_sentences), self.batch_size):
            batch = target_sentences[i : i + self.batch_size]
            if batch: tasks.append(batch)

        total_batches = len(tasks)
        if total_batches == 0:
             print("[WARN] No batches created for evaluation, though target sentences exist.")
             return { k: results.get(k, 0) for k in self.registered_metrics + ['covered_sentences_count', 'total_sentences_count']}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # --- <--- MODIFIED: Submit tasks using the SENTENCE checking function AND the *TRUNCATED* response ---> ---
            future_to_batch = {
                executor.submit(self._check_sentence_coverage_batch, batch, truncated_response): batch
                for batch in tasks
            }
            print(f"Evaluating {len(target_sentences)} sentences in {total_batches} batches against truncated response ({evaluated_word_count} words) using up to {self.max_workers} workers...")
            # Update tqdm description
            with tqdm(total=len(target_sentences), desc="Evaluating Sentence Coverage (Truncated)", unit="sentence") as pbar:
                for future in as_completed(future_to_batch):
                    batch_ref = future_to_batch[future]
                    try:
                        batch_results: List[bool] = future.result()
                        batch_covered_count = sum(1 for result in batch_results if result is True)
                        covered_count += batch_covered_count
                        pbar.update(len(batch_ref)) # Update by number of sentences
                    except Exception as exc:
                         print(f'[ERROR] Sentence coverage batch processing generated an exception: {exc}')
                         pbar.update(len(batch_ref)) # Still update progress

        # --- Update SENTENCE metrics ---
        if results["total_sentences_count"] > 0:
            coverage_rate = covered_count / results["total_sentences_count"]
            results["sentence_coverage_rate"] = round(coverage_rate, 4)
            results["covered_sentences_count"] = covered_count

        # --- <--- MODIFIED: Update log message to reflect truncated evaluation ---
        print(f"[INFO] Evaluation Complete (Truncated Response): {covered_count}/{results['total_sentences_count']} sentences covered ({results['sentence_coverage_rate']:.2%}). Evaluated word count: {results['words']}.")

        # --- Return SENTENCE metrics ---
        final_results = {k: results.get(k, 0) for k in self.registered_metrics + ['covered_sentences_count', 'total_sentences_count']}
        return final_results


# Register the task
TaskFactory.register_task('KG_TO_TEXT', KG2TextTask)