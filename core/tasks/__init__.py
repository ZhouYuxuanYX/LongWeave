# from .story_generation_task import StoryGenerationTask
# from .atomic_statements_task import AtomicStatementTask
# from .gen_kv_dictionary_task import GenKvDictionaryTask
# from .kg2text_task import KG2TextTask
# from .math_calc_task import MathCalcTask
# from .paragraph_ordering_task import ParagraphOrderingTask
# from .state_machine_task import STATE_MACHINE


# 取消注释，使能动态注册
import importlib
from pathlib import Path

for task_file in Path(__file__).parent.glob("*.py"):
    if task_file.name != "__init__.py" and task_file.name != "base_task.py":
        module_name = task_file.stem
        importlib.import_module(f".{module_name}", package=__name__)