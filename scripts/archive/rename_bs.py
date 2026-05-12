import ctypes, os, shutil
from ctypes import wintypes
from pathlib import Path

def get_short_path(long_path):
    buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
    ctypes.windll.kernel32.GetShortPathNameW(str(long_path), buf, wintypes.MAX_PATH)
    return Path(buf.value)

base = Path(r'B:\EduvateHub\CourseOnboarding\storage\uploads')
long_dir = base / 'BS Computer Science'

if not long_dir.exists():
    print('Source dir not found')
    exit(1)

short_dir = get_short_path(long_dir)
print(f'Short path: {short_dir}')

new_dir = base / 'BS_Computer_Science'
shutil.move(str(long_dir), str(new_dir))
print(f'Renamed to: {new_dir}')
print('Success')
