import sys
sys.path.insert(0, 'src')
from pathlib import Path
import json

report_path = Path('storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.json')
data = json.loads(report_path.read_text())

# Redact S3 URLs in asset_results
for asset in data.get('asset_results', []):
    if 'url' in asset and asset['url']:
        # Replace with placeholder that doesn't expose bucket details
        asset['url'] = '/assets/' + asset['name']  # generic path placeholder

# Also redact any S3 URL in featuredImage if present
if 'featuredImage' in data:
    data['featuredImage'] = ''

# Save sanitized JSON
sanitized_path = Path('storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.sanitized.json')
sanitized_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
print(f'Sanitized JSON saved: {sanitized_path}')

# Now regenerate HTML from sanitized data using the report generator
from scripts.validate_ingestion import save_report
html_path = save_report(data, report_path.parent, emit_json=False)  # data is already dict
print(f'HTML regenerated: {html_path}')
print('Regeneration complete — S3 URLs redacted.')
