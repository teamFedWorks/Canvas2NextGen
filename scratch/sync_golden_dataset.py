import json
import os
from pathlib import Path

DATASET_PATH = 'tests/golden_dataset.json'
RESULTS_PATH = 'validation/golden_results.json'

# 1. Add IT-3301 if missing
with open(DATASET_PATH, 'r') as f:
    dataset = json.load(f)

if not any(c['id'] == 'IT-3301' for c in dataset['courses']):
    dataset['courses'].append({
        'id': 'IT-3301',
        'name': 'IT-3301 Project Management',
        'path': 'storage/uploads/BS Information Technology/IT-3301 Project Management',
        'expected': {}
    })
    with open(DATASET_PATH, 'w') as f:
        json.dump(dataset, f, indent=2)

print("Golden dataset updated with IT-3301.")
