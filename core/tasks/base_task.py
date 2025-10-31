"""Base task class and interface definition"""
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseTask(ABC):
    """Base class for all tasks"""

    def __init__(self, config: Dict[str, Any]):
        """Initialize task
        
        Args:
            config: Task configuration dictionary
        """
        if self.__class__.registered_metrics == []:
            raise NotImplementedError(
                f"Task class {self.__class__.__name__} must define registered_metrics"
            )
        self.config = config

    registered_metrics = []
    @classmethod
    def get_registered_metrics(cls):
        """Get the metrics registered for this task that need to be统计"""
        return cls.registered_metrics
    
    @abstractmethod
    def generate_prompt(self, **kwargs) -> str:
        """Generate task prompt
        
        Args:
            **kwargs: Additional parameters for prompt generation
            
        Returns:
            str: Generated prompt text
        """
        pass
    
    @abstractmethod
    def evaluate(self, response: str, **kwargs) -> Dict[str, Any]:
        """Evaluate the generated result
        
        Args:
            response: Generated response
            **kwargs: Additional evaluation parameters
            
        Returns:
            Dict[str, Any]: Evaluation results
        """
        pass

class TaskFactory:
    """Task factory class for creating task instances"""
    
    _tasks = {}
    
    @classmethod
    def register_task(cls, task_name: str, task_class: type):
        """Register a new task type
        
        Args:
            task_name: Task name
            task_class: Task class to register
        """
        cls._tasks[task_name] = task_class
    
    @classmethod
    def create_task(cls, task_name: str, config: Dict) -> BaseTask:
        """Create a task instance
        
        Args:
            task_name: Task name
            config: Task configuration dictionary
            
        Returns:
            BaseTask: Created task instance
            
        Raises:
            ValueError: Thrown when task type is not supported
        """
        task_class = cls._tasks.get(task_name)
        if task_class is None:
            raise ValueError(f"Unknown task type: {task_name}. "
                           f"Supported task types include: {list(cls._tasks.keys())}")
        return task_class(config)