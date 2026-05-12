import ctypes
from ctypes import wintypes

def get_short_path(long_path):
    buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
    ctypes.windll.kernel32.GetShortPathNameW(str(long_path), buf, wintypes.MAX_PATH)
    return buf.value

base = r'B:\EduvateHub\CourseOnboarding\storage\uploads'
# Get short for the folder
long_folder = f'{base}\\BS Computer Science'
short_folder = get_short_path(long_folder)
print('Short folder:', short_folder)

# Now check if a zip inside exists using short path
import os
zips = os.listdir(short_folder)
print('Zips in short folder:', zips[:3])
