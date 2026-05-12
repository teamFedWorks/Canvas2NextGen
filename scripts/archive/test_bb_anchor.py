import sys
sys.path.insert(0, 'src')
from bs4 import BeautifulSoup

# Simulate one body from the parsed course
body = '<div data-layout-column="a3ca8295-b82c-49d6-b9b8-86f503e6c42e" data-layout-column-width="12"><a data-bbid="bbml-editor-id_1a5d3477-7276-419b-baf5-db0d4bf2839f" data-bbfile="{&quot;linkName&quot;:&quot;MGMT 5306 VC01 SP1 26.pdf&quot;,&quot;displayName&quot;:&quot;MGMT 5306 VC01 SP1 26.pdf&quot;,&quot;mimeType&quot;:&quot;application/pdf&quot;,&quot;alternativeText&quot;:&quot;MGMT 5306 VC01 SP1 26.pdf&quot;,&quot;render&quot;:&quot;inline&quot;}" href="bbcswebdav/xid-41796952_1"></a></div>'

# This is likely what the raw .dat provides: &quot; inside data-bbfile.
# After _clean_bb_html, which does html.unescape, it becomes double quotes.
soup = BeautifulSoup(body, 'html.parser')
for a in soup.find_all('a'):
    raw = a.get('data-bbfile')
    print('Got data-bbfile raw:', repr(raw))
    # Try parse as JSON
    if raw:
        import json
        try:
            meta = json.loads(raw)
            print('Parsed metadata:', meta)
        except Exception as e:
            print('JSON parse error:', e)
