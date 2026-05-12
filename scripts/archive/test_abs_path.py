import sys
sys.path.insert(0, 'src')
import os

path = r'B:\EduvateHub\CourseOnboarding\storage\uploads\BS_Computer_Science\01_-_PHI-1114_Logic_and_Argumentation.zip'
print('Path:', path)
print('Exists?', os.path.exists(path))
