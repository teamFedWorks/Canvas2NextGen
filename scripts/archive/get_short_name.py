import subprocess, sys

# Use cmd to get the short (8.3) name for the source folder
result = subprocess.run(
    ['cmd', '/c', 'for', '%I', 'in', ('"storage\\uploads\\BS Computer Science"'), 'do', '@echo', '%~sI'],
    capture_output=True, text=True, cwd='B:/EduvateHub/CourseOnboarding'
)
print('stdout:', result.stdout)
print('stderr:', result.stderr)
print('returncode:', result.returncode)
