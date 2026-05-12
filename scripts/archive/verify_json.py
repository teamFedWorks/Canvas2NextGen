import json, sys
p = r'storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.json'
try:
    data = json.loads(open(p).read())
    print('JSON is valid')
    print('asset_results count:', len(data.get('asset_results',[])))
    for a in data['asset_results']:
        print(f"  {a['name']}: {a['url']}")
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)