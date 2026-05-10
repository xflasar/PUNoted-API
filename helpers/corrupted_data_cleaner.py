from datetime import datetime


def clean_corrupted_record(record):
    cleaned = {}
    for k, v in record.items():
        # 1. Remove wrapping single quotes from strings
        if isinstance(v, str) and v.startswith("'") and v.endswith("'"):
            v = v.strip("'")

        # 2. Fix Boolean strings ("False" -> False)
        if v == "False":
            v = False
        elif v == "True":
            v = True

        # 3. Fix "datetime.datetime(...)" strings
        if isinstance(v, str) and v.startswith("datetime.datetime"):
            try:
                # Dangerous but effective for this specific format:
                # Parses the string "datetime.datetime(2026, ...)" back into an object
                # Better approach: Fix the upstream converter that produced this string.
                # For now, we manually parse the numbers:
                import re
                nums = [int(n) for n in re.findall(r'\d+', v)]
                v = datetime(*nums)
            except Exception:
                pass # Keep original if parse fails

        cleaned[k] = v
    return cleaned
