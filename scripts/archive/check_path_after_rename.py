import os
p = r'storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation.zip'
print('Exists?', os.path.exists(p))
p2 = r'B:\EduvateHub\CourseOnboarding\storage\uploads\BS_Computer_Science\01_-_PHI-1114_Logic_and_Argumentation.zip'
print('Exists (abs)?', os.path.exists(p2))
