from pathlib import Path
p = Path('storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.html')
lines = p.read_text(encoding='utf-8').splitlines()
for i, line in enumerate(lines, 1):
    if 'Canvas' in line:
        print(f'{i}: {line}')