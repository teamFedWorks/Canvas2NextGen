import subprocess, os, tempfile, shutil

# Create a junction (directory symlink) with a short name pointing to the spaced folder
junction_dir = r'C:\Temp\bs_cs_junc'
long_target = r'B:\EduvateHub\CourseOnboarding\storage\uploads\BS Computer Science'

# Ensure C:\Temp exists
os.makedirs(os.path.dirname(junction_dir), exist_ok=True)

# Remove existing if any
if os.path.exists(junction_dir):
    shutil.rmtree(junction_dir, ignore_errors=True)

# Use mklink /J to create a directory junction
result = subprocess.run(
    ['cmd', '/c', 'mklink', '/J', junction_dir, long_target],
    capture_output=True, text=True
)
print('mklink stdout:', result.stdout)
print('mklink stderr:', result.stderr)
print('returncode:', result.returncode)

if result.returncode == 0:
    print('Junction created successfully')
    # Now test accessing via junction
    test_file = os.path.join(junction_dir, '01 - PHI-1114 Logic and Argumentation.zip')
    print('Test file exists?', os.path.exists(test_file))
    # Clean up
    # os.rmdir(junction_dir)  # remove junction
else:
    print('Failed to create junction')
