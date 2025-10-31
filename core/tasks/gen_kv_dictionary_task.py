"""Implementation of Key-Value Dictionary Generation Task"""
from typing import Dict, Any
import uuid
import random
import re
from core.tasks.base_task import BaseTask, TaskFactory
from core.seed import generate_seed_from_id
import math
import time

class GenKvDictionaryTask(BaseTask):
    """Generate a dictionary containing specific key-value pairs and evaluate their positions"""
    registered_metrics = ['position_score', 'key_existence', 'entry_num_score', 'total_score', 'avg_length_score']

    def __init__(self, config: Dict[str, Any]):
        """Initialize the task
        
        Args:
            config: Task configuration dictionary, must contain:
                - num_entries: Total number of dictionary entries to generate (default 20)
                - key_length: Character length of target key (default 5)
                - value_length: Character length of target value (default 8)
        """
        super().__init__(config)
        self.num_entries = config.get("num_entries", 20)
        self.key_length = config.get("key_length", 32)
        self.value_length = config.get("value_length", 32)

        # Parameter validation
        if self.num_entries <= 1:
            raise ValueError("Number of dictionary entries must be greater than 1")
        if self.key_length < 1 or self.value_length < 1:
            raise ValueError("Key/value length must be a positive integer")

    def generate_prompt(self, **kwargs) -> str:
        """Generate a detailed prompt with dynamic parameters"""
        # Generate deterministic random seed
        seed = generate_seed_from_id(kwargs.get("sample_id"))
        rng = random.Random(seed)

        # Dynamically generate target key-value pair
        target_key = ''.join(rng.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=self.key_length))
        target_value = ''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=self.value_length))
        
        # Generate target position percentage (excluding extreme values)
        target_percent = rng.choice([
            *range(5, 96, 5),  # Main sampling at 5% intervals
            # *random.sample(range(100), 20)  # Add a few random points
        ])

        # Convert percentage to target index
        target_index = round((target_percent / 100) * (self.num_entries - 1))
        target_index = max(0, min(self.num_entries - 1, target_index))  # Ensure valid index

        # Standardized prompt template
        template = (
            "Generate a Python dictionary with the following requirements:\n"
            "- Total entries: {num}\n"
            "- MUST include the entry: '{key}': '{value}'\n"
            "- The special entry should be placed at index {index}\n"
            "- Other keys and values must follow these rules:\n"
            "  * Keys must be random strings of length {key_length}, consisting ONLY of uppercase letters (A-Z) and underscores (_)\n"
            "  * Values must be random strings of length {value_length}, consisting ONLY of lowercase letters (a-z) and digits (0-9)\n"
            "  * Keys and values MUST NOT contain any special characters (e.g., /, =, $, @, :, etc.) or spaces\n"
            "- Output ONLY the dictionary in the following format (as a single-line string):\n"
            "{{'...': '...', ..., '{key}': '{value}', ..., '...': '...'}}\n"
            "- Ensure the dictionary string is valid JSON and can be parsed by `json.loads()` without errors.\n"
            "- DO NOT include any code or explanations. Only return the dictionary string."
        )


        # Construct metadata for evaluation
        meta = {
            'target_key': target_key,
            'target_value': target_value,
            'target_index': target_index,  # Store target index
            'num_entries': self.num_entries
        }

        return template.format(
            num=self.num_entries,
            key=target_key,
            value=target_value,
            index=target_index,
            key_length=self.key_length,
            value_length=self.value_length
        ), meta

    def evaluate(self, response: str, **kwargs) -> Dict[str, float]:
        """Evaluate the key position and length accuracy of the dictionary

        Scoring rules:
            - key_existence: Whether the target key exists (0/1)
            - position_score: Score for target key near specified index (0~1)
            - entry_num_score: Score for returned dictionary entries close to expected value (0~1)
            - total_score: position_score * entry_num_score, for comprehensive evaluation
        """
        meta = kwargs["metadata"]
        target_key, target_value = meta["target_key"], meta["target_value"]
        target_index = meta["target_index"]
        expected_total = meta["num_entries"]

        # Initialize default results
        result = {
            "key_existence": 0.0,
            "position_score": 0.0,
            "entry_num_score": 0.0,
            "avg_length_score": 0.0,
            "total_score": 0.0,
        }

        # Locate dictionary boundaries
        start_idx = response.find("{")
        end_idx = response.rfind("}")
        if start_idx == -1 or end_idx == -1 or start_idx >= end_idx:
            return result

        try:
            import json

            # Extract and convert Python-style dictionary to JSON
            dict_str = response[start_idx : end_idx + 1]
            dict_str = dict_str.replace("'", '"')  # Convert single quotes
            dict_str = re.sub(r",\s*}", "}", dict_str)  # Fix trailing commas

            # Parse dictionary and maintain order
            parsed = json.loads(dict_str, object_pairs_hook=list)
            entries = dict(parsed)  # Python 3.7+ maintains insertion order
            # keys = list(entries.keys())

            keys_list = list(entries.keys())
            values_list = [str(v) for v in entries.values()]
            # --------------------------------------------------------------
            # 1. Key existence
            # --------------------------------------------------------------
            if target_key not in keys_list or target_value not in values_list:
                return result
            result["key_existence"] = 1.0

            # --------------------------------------------------------------
            # 2. Position score (sigmoid reduced penalty)
            # --------------------------------------------------------------
            actual_index = keys_list.index(target_key)
            position_diff = abs(actual_index - target_index)
            scale_pos = expected_total * 0.25  # Allow ±25% index error
            position_score = 1 / (1 + (position_diff / scale_pos) ** 2)
            result["position_score"] = round(position_score, 4)

            # --------------------------------------------------------------
            # 3. Dictionary count score (sigmoid reduced penalty)
            # --------------------------------------------------------------
            actual_total = len(keys_list)
            length_diff = abs(actual_total - expected_total)
            scale_len = max(1, expected_total * 0.25)  # Allow ±25% quantity error
            entry_num_score = 1 / (1 + (length_diff / scale_len) ** 2)
            result["entry_num_score"] = round(entry_num_score, 4)

            # 4. Average length score
            avg_key_len = sum(len(k) for k in keys_list) / actual_total if actual_total else 0
            avg_val_len = sum(len(str(v)) for v in values_list) / actual_total if actual_total else 0

            key_len_diff = abs(avg_key_len - self.key_length)
            val_len_diff = abs(avg_val_len - self.value_length)

            scale_key = max(1, self.key_length * 0.25)
            scale_val = max(1, self.value_length * 0.25)

            key_len_score = 1 / (1 + (key_len_diff / scale_key) ** 2)
            val_len_score = 1 / (1 + (val_len_diff / scale_val) ** 2)

            avg_length_score = round((key_len_score + val_len_score) / 2, 4)
            result["avg_length_score"] = avg_length_score

            # --------------------------------------------------------------
            # 5. Total score (weights can be adjusted as needed)
            # --------------------------------------------------------------

            if position_score == 0 and entry_num_score == 0:
                result["total_score"] = 0 
            else:
                result["total_score"] = 3 / (1 / position_score + 1 / entry_num_score + 1 / avg_length_score)

        except (json.JSONDecodeError, KeyError, TypeError):
            # Parsing failure keeps default 0 score
            pass

        return result

# Register task to factory
TaskFactory.register_task('GEN_KV_DICT', GenKvDictionaryTask)