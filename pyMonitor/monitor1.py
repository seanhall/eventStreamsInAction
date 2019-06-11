import os

def get_filesystem_metrics(path):
  stats = os.statvfs(path)
  block_size = stats.f_frsize
  return (block_size * stats.f_blocks, # Filesystem size in bytes
    block_size * stats.f_bfree,         # Free bytes
    block_size * stats.f_bavail)       # Free bytes excl. reserved space

s, f, a = get_filesystem_metrics("/")
print(f"size: {s}, free: {f}, available: {a}")