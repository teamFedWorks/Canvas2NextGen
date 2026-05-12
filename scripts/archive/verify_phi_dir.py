import sys
sys.path.insert(0, 'src')
import os

extracted_dir = 'storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation'
print('Path exists?', os.path.exists(extracted_dir))
print('Is dir?', os.path.isdir(extracted_dir))
print('Contents:', os.listdir(extracted_dir)[:8])
