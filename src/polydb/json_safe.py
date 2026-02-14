import json
from datetime import datetime

def json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

