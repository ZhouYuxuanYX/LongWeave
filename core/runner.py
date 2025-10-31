# core/runner.py
from typing import Dict, Any
import time
from core.tasks.base_task import TaskFactory
from core.serve.unified_api import unified_call

class TaskRunner:
    def __init__(self, task_config: Dict, model_config: Dict):
        """
        初始化任务运行器

        Args:
            task_config: 任务配置字典
            model_config: 模型配置字典，包含backend、model和params等信息
        """
        self.model_config = model_config
        self.task = TaskFactory.create_task(
            task_name=task_config['task_name'],
            config={**task_config, 'model_config': self.model_config}
        )
        self.task_config = task_config

    def generate_prompt(self, **kwargs) -> str:
        """生成单个 prompt"""
        return self.task.generate_prompt(**kwargs)

    def call_api(self, prompt: str, eval_args: Dict) -> (str, float):
        """调用 API 并返回响应和延迟"""
        start = time.time()
        response = unified_call(
            backend=self.model_config.get('backend'),
            model=self.model_config.get('model'),
            prompt=prompt,
            **eval_args
        )
        latency = time.time() - start
        return response, latency

    def evaluate_response(self, response: str, kwargs: Dict) -> Dict:
        """评估 API 响应并返回评估结果"""
        return self.task.evaluate(response, **kwargs)
