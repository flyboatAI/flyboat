import psutil

from error.memory_overflow_error import MemoryOverflowError


def memory_overflow(pid, max_memory):
    if not max_memory:
        return
    process = psutil.Process(pid)
    mem_info = process.memory_info()
    mem = mem_info.rss / 1024 / 1024  # MB
    if mem > max_memory:
        raise MemoryOverflowError("使用的内存超出了配置的最大内存, 如要使用, 请调高该流水线最大可用内存") from None
