# core/tasks/news_AP_style_task.py

import json
import re
from pathlib import Path
from typing import Dict, Any, List
from core.tasks.base_task import BaseTask, TaskFactory
from core.seed import generate_seed_from_id
import random
from tqdm import tqdm
from core.serve.unified_api import unified_call
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


class APStyleTask(BaseTask):
    """AP Style News Writing Evaluation Task"""
    registered_metrics = ['ap_total_score', 'recall_rate', 'words', 'total_score']
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.rubric = self._load_rubric(config['rubric_path'])
        self.samples = self._load_samples(config['data_path'])
        self.test_length = config['test_length']
        self.current_sample_idx = -1
    
    def _load_rubric(self, path: str) -> Dict:
        """Load AP Style Scoring Rubric"""
        with open(path, 'r') as f:
            data = json.load(f)
        return {item['Category']: item for item in data['AP_Style_Scoring_Table']}
    
    def _load_samples(self, path: str) -> List[Dict]:
        """Load Writing Sample Data"""
        with open(path, 'r') as f:
            raw_data = json.load(f)
        return raw_data


    def _count_words(self, text: str) -> int:
            """
            Calculate the number of words in the given text.
            A word is defined as a sequence of characters consisting of letters, digits, or hyphens.
            """
            # Use regular expressions to match words
            words = re.findall(r'\b\w+\b', text)
            return len(words)

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
        
    def generate_prompt(self, **kwargs) -> (str, Dict):
        """Generate writing prompt with interleaved AP Style guidelines and corresponding statements"""
        if self.current_sample_idx + 1 >= len(self.samples):
            self.current_sample_idx = 0
        
        self.current_sample_idx += 1
        sample = self.samples[self.current_sample_idx]

        ALL_CATEGORIES = [
            'Capitalization Rules', 'Overall Consistency', 'Media References',
            'Titles and Positions', 'Addresses and Locations', 'Number Usage',
            'Technical Terms', 'Dates and Times', 'Clarity and Brevity', 'Punctuation'
        ]
        total_categories = len(ALL_CATEGORIES)

        if self.test_length == 8192:
            num_categories = total_categories
        elif self.test_length == 4096:
            num_categories = 8
        elif self.test_length == 2048:
            num_categories = 5
        elif self.test_length == 1024:
            num_categories = 2
        else:
            raise ValueError(f"Unsupported test_length: {self.test_length}")

        seed = generate_seed_from_id(kwargs['sample_id'])
        rng = random.Random(seed)
        # Preserve order of ALL_CATEGORIES if num_categories == total_categories for consistency
        if num_categories == total_categories:
            selected_categories = ALL_CATEGORIES[:] # Make a copy
        else:
            selected_categories = rng.sample(ALL_CATEGORIES, k=num_categories)


        selected_statements = [
            stmt for stmt in sample['statements']
            if stmt['Category'] in selected_categories
        ]
        selected_statements.sort(key=lambda x: sample['statements'].index(x))

        # Initial part of the prompt
        prompt_text = (
            "Instructions:\n"
            f"{sample['query']}\n\n"
            "**You MUST strictly adhere to the AP News Style guidelines provided below.\n"
            "Your article will be evaluated on two equally‑weighted dimensions: (1) **Recall** of ALL required information, and (2) **Compliance** with the AP Style rules.**\n\n"
            "**IMPORTANT: Each AP Style category includes example sentences that violate its rules. Rewrite and include all of them in your article, following AP Style and keeping their meaning. Missing or uncorrected items will reduce your score.**\n"
        )

        # Group statements by category for easy access while preserving their original relative order
        statements_by_category = {}
        for stmt in selected_statements:
            cat = stmt['Category']
            if cat not in statements_by_category:
                statements_by_category[cat] = []
            statements_by_category[cat].append(stmt)

        # Interleave guidelines and statements
        all_interleaved_blocks = []
        statement_global_index = 1
        # Iterate through selected_categories to maintain the chosen order of categories
        for category_name in selected_categories:
            if category_name not in self.rubric: # Should not happen if data is consistent
                continue

            # Part 1: Rubric for the category
            category_rubric_str = (
                f"=== {category_name.upper()} ===\n"
                f"Scoring Criteria:\n{self.rubric[category_name]['Scoring_Criteria']}\n\n"
                f"Incorrect Examples:\n{self.rubric[category_name]['Incorrect_Examples']}\n\n"
                f"Correct Examples:\n{self.rubric[category_name]['Correct_Examples']}"
            )
            all_interleaved_blocks.append(category_rubric_str)

            # Part 2: Statements for this category
            # These statements are already filtered to be in selected_categories and sorted
            category_specific_stmts = statements_by_category.get(category_name, [])
            if category_specific_stmts:
                statement_lines_for_category = [
                    f"**Content Requirements for '{category_name}': Include and Rewrite EACH of the following statements to comply with the '{category_name}' AP Style guidelines detailed above.**"
                ]
                for stmt_dict in category_specific_stmts:
                    statement_lines_for_category.append(f"{statement_global_index}. {stmt_dict['Statement']}")
                    statement_global_index += 1
                all_interleaved_blocks.append("\n".join(statement_lines_for_category))
        
        if all_interleaved_blocks:
             prompt_text += "\n\n" + "\n\n".join(all_interleaved_blocks)


        # Append length specifications and closing
        prompt_text += (
            "\n\nLength Specifications:\n"
            f"- **TARGET WORD COUNT:** Aim for **around {self.test_length} words**.\n\n"
            f"Begin writing your ~{self.test_length}-word AP‑style article below:\n"
        )

        return prompt_text, {
            'rubric': self.rubric,
            'statements': selected_statements, # Still provide all selected statements for metadata
            'selected_categories': selected_categories
        }


    def _evaluate_single_category(self, category: str, response: str, stmts: List[Dict], rubric: Dict) -> Dict:
        """
        Evaluate a single AP category (return score and reasons)
        
        Args:
            category (str): The name of the category being evaluated.
            response (str): Article content.
            stmts (List[Dict]): List of statements to evaluate, each containing original statement, correction rules, and correct expression.
            rubric (Dict): Scoring criteria and rules.
        
        Returns:
            Dict: Dictionary containing category score, recall rate, and reasoning results.
        """
        # Initialize statistics variables
        total_statements = len(stmts)
        correct_statements = 0
        found_statements = 0
        
        evaluation_prompt = (
            "Please read the article and complete the EVALUATION TASK below:\n\n"
            
            f"=== ARTICLE CONTENT ===\n{response}\n\n"
            
            f"=== SCORING CRITERIA FOR '{category}' ===\n"
            f"Scoring Criteria:\n{rubric['Scoring_Criteria']}\n"
            f"Incorrect Examples:\n{rubric['Incorrect_Examples']}\n"
            f"Correct Examples:\n{rubric['Correct_Examples']}\n\n"
            
            "EVALUATION TASK:\n"
            "For each statement listed below, perform the following evaluations:\n"
            "1. Determine whether the statement exists in the article (verbatim or semantically equivalent). If it exists, extract the exact matching content from the article.\n"
            "2. If the statement exists, determine whether it follows the AP rules as per the scoring criteria.\n"
            "3. Provide clear reasoning for your evaluation.\n\n"
            
            "Output format (JSON):\n"
            "{\n"
            '  {\n'
            '    "statement_id": "Unique ID of the statement",\n'
            '    "statement": "Original statement",\n'
            '    "matched_content": "Exact matching content from the article (or empty string if not found)",\n'
            '    "thinking": "Explanation of the evaluation process and reasoning",\n'
            '    "exists_in_article": true/false,\n'
            '    "follows_rules": true/false\n'
            '  },\n'
            '  ...\n'
            "}\n\n"
            "Do NOT include any additional text or explanations outside the JSON array.\n"
            "Ensure that the JSON is valid and can be directly parsed by a JSON parser.\n\n"

            "STATEMENTS TO EVALUATE:\n"
        )

        # Add all statements to be evaluated to the prompt and assign them numbers
        for i, stmt in enumerate(stmts, start=1):  # Use enumerate to auto-generate numbers
            evaluation_prompt += (
                f"Statement {i}: {stmt['Statement']}\n"
                f"   Why This Is Incorrect: {stmt['Reason_for_Deduction']}\n"
                f"   How It Should Be Written: {stmt['Correct_Expression']}\n\n"
            )

        evaluation_prompt += "Please provide the evaluation results."

        try:
            # Call LLM for reasoning
            eval_response = unified_call(
                backend=self.config['evaluation_model']['backend'],
                model=self.config['evaluation_model']['model'],
                prompt=evaluation_prompt,
                **self.config['evaluation_model']['params']
            )
            
            # Parse reasoning results
            results = self._parse_evaluation_results(eval_response)
            
            # Count results
            for result in results:
                if result["exists_in_article"]:
                    found_statements += 1
                    if result["follows_rules"]:
                        correct_statements += 1
            
            # Calculate recall rate and score
            recall = found_statements / total_statements if total_statements > 0 else 0
            ap_score = correct_statements / total_statements if total_statements > 0 else 0
            
            return {
                "category": category,
                "recall": recall,
                "ap_score": ap_score,
                "total_statements": total_statements,
                "found_statements": found_statements,
                "correct_statements": correct_statements,
                "results": results,
            }
        
        except Exception as e:
            return {
                "category": category,
                "recall": 0,
                "ap_score": 0,
                "total_statements": total_statements,
                "found_statements": 0,
                "correct_statements": 0,
                "error": f"Evaluation error: {str(e)}",
            }

    def _parse_evaluation_results(self, eval_response: str) -> List[Dict]:
        """
        Parse evaluation results returned by the LLM.
        
        Args:
            eval_response (str): String returned by the LLM, which may contain Markdown code block markers or other non-standard content.
        
        Returns:
            List[Dict]: Evaluation results for each statement.
        """
        try:
            # Step 1: Remove Markdown code block markers (if any)
            if eval_response.startswith("```") and eval_response.endswith("```"):
                eval_response = eval_response.strip("`").strip()
            
            # Step 2: Try to extract JSON part (even with additional text)
            # Use regular expression to match JSON array (outermost [...])
            match = re.search(r"\[.*\]", eval_response, re.DOTALL)
            if match:
                eval_response = match.group(0)
            else:
                raise ValueError("Unable to find valid JSON array")

            # Step 3: Parse JSON data
            results = json.loads(eval_response)
            
            # Step 4: Ensure the return value is a list
            if not isinstance(results, list):
                raise ValueError("Evaluation results must be a list")
            
            return results
        
        except json.JSONDecodeError as e:
            raise ValueError(f"Unable to parse evaluation results: {str(e)}")
        except Exception as e:
            raise ValueError(f"Parsing failed: {str(e)}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        """New evaluation process, only evaluate selected AP Style categories"""
        metadata = kwargs['metadata']
        results = {
            "ap_total_score": 0,
            "recall_rate": 0.0,
            "words": 0,
            "total_score": 0.0
        }
        rubric = metadata['rubric']
        statements = metadata['statements']
        selected_categories = metadata['selected_categories']  # Get the list of selected categories

        # Group statements by selected category (only keep statements of selected categories)
        category_statements = {}
        for stmt in statements:  # Note: statements here are already filtered selected_statements
            category = stmt['Category']
            if category in selected_categories:  # Double check to ensure only processing selected categories
                if category not in category_statements:
                    category_statements[category] = []
                category_statements[category].append(stmt)

        # Calculate truncated response text (keep original logic)
        original_word_count = self._count_words(response)
        truncated_response = self._get_first_n_words_text(response, self.test_length)
        evaluated_word_count = self._count_words(truncated_response)
        response = truncated_response

        # Only evaluate selected categories (key modification)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for category in selected_categories:  # Iterate through selected categories, not all rubric
                # Ensure the category exists in rubric (avoid data errors)
                if category not in rubric:
                    continue
                # Get statements corresponding to this category (may be empty, but guaranteed by upstream)
                stmts = category_statements.get(category, [])
                future = executor.submit(
                    self._evaluate_single_category, category, response, stmts, rubric[category]
                )
                futures.append(future)

            # Adjust progress bar total to the number of selected categories
            with tqdm(total=len(futures), desc="Evaluating Selected Categories", unit="category") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results["ap_total_score"] += result['ap_score']
                    results["recall_rate"] += result['recall']
                    pbar.update(1)

        # Calculate final score (denominator is the number of selected categories)
        num_selected_categories = len(selected_categories)
        if num_selected_categories > 0:
            results['recall_rate'] /= num_selected_categories
            results['ap_total_score'] /= num_selected_categories
        else:
            results['recall_rate'] = 0.0
            results['ap_total_score'] = 0.0

        # Calculate composite score (keep original logic)
        if results['ap_total_score'] > 1e-9 and results['recall_rate'] > 1e-9:
            results['total_score'] = 2.0 / (1.0/results['ap_total_score'] + 1.0/results['recall_rate'])
        else:
            results['total_score'] = 0.0

        # Record original word count (keep original logic)
        results['words'] = original_word_count

        return results



TaskFactory.register_task('AP_STYLE_WRITING', APStyleTask)
