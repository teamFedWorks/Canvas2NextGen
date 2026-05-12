import ctypes
from ctypes import wintypes
print("test start")
def get_short(long):
    buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
    ctypes.windll.kernel32.GetShortPathNameW(str(long), buf, wintypes.MAX_PATH)
    return buf.value
long = r'B:\EduvateHub\CourseOnboarding\storage\uploads\BS Computer Science'
try:
    short = get_short(long)
    print('Short:', short)
except Exception as e:
    print('Error:', e)
print("test end")