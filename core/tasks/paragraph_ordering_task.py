# /mnt/data/zikai/longeval/core/tasks/paragraph_ordering_task.py

import re
import json
from typing import Dict, Any, List
from core.metrics.Kendalls_Tau import calculate_kendall_tau
from core.tasks.base_task import BaseTask, TaskFactory
from core.serve.unified_api import unified_call
from core.seed import generate_seed_from_id
import random
import math

class ParagraphOrderingTask(BaseTask):
    """Paragraph ordering task supporting multi-document testing"""
    registered_metrics = ['kendalls_tau']
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.samples = self.load_data(config['data_path'], config['test_length'])
        self.current_doc_index = -1
    
    def load_data(self, path: str, test_length: int) -> List[Dict]:
        """Load multi-document data"""
        length_to_key = {
            1024: '1k',
            2048: '2k',
            4096: '4k',
            8192: '8k'
        }

        # Check if test_length is within supported range
        if test_length not in length_to_key:
            raise ValueError(f"Unsupported test_length: {test_length}. Supported values are {list(length_to_key.keys())}.")

        with open(path, 'r') as f:
            raw_data = json.load(f)

        # Get subset according to test_length
        raw_data = raw_data[length_to_key[test_length]]

        samples = []
        for doc_entry in raw_data:  # Iterate through each document in the list
            doc_id = doc_entry.get('doc_id')  # Get document ID
            segments = doc_entry.get('segments', [])  # Get paragraph list

            # Validate data format
            if not doc_id or not segments:
                raise ValueError(f"Invalid document entry: {doc_entry}")

            # Generate random seed based on doc_id
            seed = generate_seed_from_id(doc_id)
            rng = random.Random(seed)  # Create independent random number generator instance

            # Construct original_segments and shuffled_segments
            original_segments = segments  # Original order
            shuffled_segments = segments[:]  # Copy for shuffling
            rng.shuffle(shuffled_segments)  # Shuffle based on rng

            # Record correct order (based on original indices)
            correct_order = [original_segments.index(segment) for segment in shuffled_segments]

            samples.append({
                'doc_id': doc_id,
                'original': original_segments,
                'shuffled': shuffled_segments,
                'correct_order': correct_order
            })
        return samples


    
    # def set_current_doc(self, index: int):
    #     """Set currently processed document"""
    #     if 0 <= index < len(self.samples):
    #         self.current_doc_index = index
    
    def generate_prompt(self, **kwargs) -> str:
        """Generate prompt for current document"""
        if self.current_doc_index + 1 >= len(self.samples):
            # If all documents have been processed, either reset index or throw exception
            # Option 1: Reset index
            # self.current_doc_index = -1
            # Option 2: Throw exception
            raise IndexError("No more documents to process. All samples have been used.")
    
        self.current_doc_index += 1
        current_doc = self.samples[self.current_doc_index]

        meta_data = current_doc

        return (
            "Please rearrange the following paragraphs into a logically coherent article:\n\n"
            + "\n\n".join([f"[[Segment {i}]]\n{text}" 
                            for i, text in enumerate(current_doc['shuffled'])])
            + "\n\nRequirements:\n"
            "1. Keep the original content of paragraphs unchanged, only adjust their order\n"
            "2. Use [[Segment X]] to identify original paragraph numbers, starting from 0 up to {}.\n"
            "3. Output the complete content in final order (include paragraph identifiers)\n"
            "4. The final output must contain exactly {} segments\n"
            "Example:\n"
            "[[Segment 0]]\nParagraph content\n[[Segment 1]]\nAnother paragraph content\n..."
            .format(len(current_doc['shuffled']) - 1, len(current_doc['shuffled']))
        ), meta_data
    
    def call_api(self, prompt: str) -> str:
        """Enhanced API calling method"""
        try:
            return unified_call(
                backend=self.config['api']['backend'],
                model=self.config['api']['model'],
                messages=[
                    {'role': 'user', 'content': prompt}
                ],
                **{**self.config['api']['params']}
            )
        except Exception as e:
            return f"API_ERROR: {str(e)}"

    def _extract_ordered_segments(self, response: str, current_doc: Dict) -> list:
        """Extract paragraphs from response (multi-document version support)"""
        # segment_pattern = r"\[\[Segment\s*(\d+)\]\]\n(.*?)(?=\n\[\[Segment|$)"
        segment_pattern = r"\[\[Segment\s*(\d+)\]\]\s*\n(.*?)(?=\n\[\[Segment|$)"
        matches = re.findall(segment_pattern, response, re.DOTALL)
        
        ordered_segments = []
        valid_indices = set(range(0, len(current_doc['original'])))
        
        for seg_num, content in matches:
            if seg_num.isdigit():
                num = int(seg_num)
                if num in valid_indices:
                    ordered_segments.append({
                        "original_index": num,
                        "content": content.strip()
                    })
                    valid_indices.remove(num)  # Prevent duplicates
        return ordered_segments
    
    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        """Multi-document evaluation method"""
        current_doc = kwargs['metadata']
        try:
            ordered_segments = self._extract_ordered_segments(response, current_doc)
            
            # Verify completeness
            if len(ordered_segments) != len(current_doc['original']):
                raise ValueError(
                    f"Mismatch in number of extracted paragraphs (expected {len(current_doc['original'])}, actual {len(ordered_segments)})"
                )
            
            # predicted_order = [seg["original_index"] for seg in ordered_segments]
            predicted_contents = [seg["content"] for seg in ordered_segments]
            
            # Calculate Kendall's Tau, handling possible None values
            kendalls_tau = calculate_kendall_tau(
                current_doc['original'], 
                predicted_contents
            )
            
            kendalls_tau = (1 + kendalls_tau) / 2

            if kendalls_tau is None or (isinstance(kendalls_tau, float) and math.isnan(kendalls_tau)):
                kendalls_tau = 0


            return {
                "doc_id": current_doc['doc_id'],
                "kendalls_tau": kendalls_tau,
                # "predicted_order": predicted_order,
                "gold_order": current_doc.get('correct_order', []),
                "status": "success",
                "missing_segments": list(set(range(len(current_doc['shuffled']))) - {s["original_index"] for s in ordered_segments})
            }
        except Exception as e:
            return {
                "doc_id": current_doc['doc_id'],
                "kendalls_tau": 0,  # Return 0 in case of exception
                "status": "error",
                "error_type": type(e).__name__,
                "error_detail": str(e),
                "raw_response": response
            }


TaskFactory.register_task('PARAGRAPH_ORDERING', ParagraphOrderingTask)