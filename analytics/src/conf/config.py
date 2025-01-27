import os

from typing import Dict

def get_config() -> Dict:
    res = {}

    res['DB_HOST'] = os.environ.get('POSTGRES_HOST', 'localhost')
    res['DB_PORT'] = os.environ.get('POSTGRES_PORT', '5432')
    res['DB_USER'] = os.environ.get('POSTGRES_USER', 'user')
    res['DB_PASSWORD'] = os.environ.get('POSTGRES_PASSWORD', 'pass')
    res['DB_NAME'] = os.environ.get('POSTGRES_DB', 'vehicles_db')
    
    return res

config = get_config()
