import re

def clean_entry(entry):
    if not isinstance(entry, list):
        entry = [str(entry)]
    cleaned_list = []
    for item in entry:
        clean_item = re.sub(r'\s*\(.*?\)', '', str(item)).strip()
        if clean_item:
            cleaned_list.append(clean_item)
    return cleaned_list

def create_combination_key(cleaned_list):
    if not cleaned_list: return "Unknown"
    return ", ".join(sorted(cleaned_list))