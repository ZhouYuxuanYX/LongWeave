import random
import numpy as np
import torch
import hashlib


def set_global_seed(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)  
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

def generate_unique_id(task_path: str, sample_index: int) -> str:
    """
    根据任务路径和样本索引生成唯一编码
    
    Args:
        task_path (str): 任务路径（如 "longeval/task_name"）
        sample_index (int): 样本索引
    
    Returns:
        str: 唯一编码
    """
    # 构造编码字符串
    unique_id = f"{task_path}_{sample_index}"
    return unique_id


def generate_seed_from_id(unique_id: str) -> int:
    """
    根据唯一编码生成随机种子（SHA-256 版本）
    
    Args:
        unique_id (str): 唯一标识符
    
    Returns:
        int: 随机种子
    """
    # 使用 SHA-256 哈希函数生成固定长度的哈希值
    hash_object = hashlib.sha256(unique_id.encode())
    return int(hash_object.hexdigest(), 16) % (2**32)  # 转换为 32 位整数
