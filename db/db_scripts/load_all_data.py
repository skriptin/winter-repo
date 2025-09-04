import os
import json
import sqlite3 as sql

# -- Fetch the Paths needed --

script_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.dirname(script_dir)

db_file_path = os.path.join(db_dir, 'database.db')

jsons_path = os.path.join(db_dir, 'jsons')
JSON_DIR = os.path.join(jsons_path, 'lhir_json')
print(JSON_DIR)
json_to_parse = os.path.join(JSON_DIR, '2024-000052252 Silver Mountain Resources Inc-f1-06149229_2-1-1.json')


# --- Load disk → RAM ---
disk_conn = sql.connect(db_file_path)
ram_conn = sql.connect(":memory:")
disk_conn.backup(ram_conn)
disk_conn.close()

ram_conn.execute("""
CREATE TABLE IF NOT EXISTS parameters (
    parameter_id TEXT PRIMARY KEY,
    parameter_desc TEXT,
    conf_upper REAL,
    conf_lower REAL,
    prob_correct REAL,
    samples_checked INTEGER
);
""")
ram_conn.execute("""
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    report_type TEXT,
    sedar_year TEXT,
    is_new BOOLEAN,
    pages INTEGER
);
""")
ram_conn.execute("""
CREATE TABLE IF NOT EXISTS main (
    report_id TEXT,
    parameter_id TEXT,
    value BLOB,
    flagged BOOLEAN,
    PRIMARY KEY (report_id, parameter_id),
    FOREIGN KEY (report_id) REFERENCES reports(report_id),
    FOREIGN KEY (parameter_id) REFERENCES parameters(parameter_id)
);
""")
ram_conn.commit()

def parse_params(json_to_parse):
    with open(json_to_parse, "r", encoding="utf-8") as f:
        data = json.load(f)

    faf_section = data.get("faf", {})

    for category_key in faf_section:
        category_data = faf_section[category_key]

        if "final_values" in category_data and isinstance(category_data["final_values"], dict):
            final_values = category_data["final_values"]
            for param_key in final_values.keys():
                ram_conn.execute(
                    "INSERT OR IGNORE INTO parameters (parameter_id) VALUES (?)",
                    (param_key,)
                )
    ram_conn.commit()

def parse_files(JSON_DIR):
    for filename in os.listdir(JSON_DIR):
        file_pth = os.path.join(JSON_DIR, filename)
        with open(file_pth, "r", encoding='utf-8') as f:
            data = json.load(f)

            metadata = data.get("metadata", {})
            report_id_full = metadata.get("pdf_filename", "")
            report_id, _ = os.path.splitext(report_id_full)
            sedar_year = metadata.get("sedar_year")
            pages = metadata.get("final_page_index")
            is_new = 1 if metadata.get("43_101_era") == "new" else 0

            ram_conn.execute(
                "INSERT OR REPLACE INTO reports (report_id, report_type, sedar_year, is_new, pages) "
                "VALUES (?, ?, ?, ?, ?)",
                (report_id, "lhir", sedar_year, is_new, pages)
            )
    ram_conn.commit()

def insert_data(JSON_DIR):
    for filename in os.listdir(JSON_DIR):
        file_pth = os.path.join(JSON_DIR, filename)
        with open(file_pth, "r", encoding='utf-8') as f:
            data = json.load(f)
            
            metadata = data.get("metadata", {})
            report_id_full = metadata.get("pdf_filename", "")
            report_id, _ = os.path.splitext(report_id_full)

            faf_section = data.get("faf", {})
            for category_key in faf_section:
                category_data = faf_section[category_key]

                if "final_values" in category_data and isinstance(category_data["final_values"], dict):
                    final_values = category_data["final_values"]
                    for param_key, value in final_values.items():
                        
                        insert_value = value
                        if isinstance(value, (dict, list)):
                            insert_value = json.dumps(value)

                        ram_conn.execute(
                            "INSERT OR IGNORE INTO main (report_id, parameter_id, value, flagged) "
                            "VALUES (?, ?, ?, ?)",
                            (report_id, param_key, insert_value, 0)
                        )
    ram_conn.commit()




parse_params(json_to_parse)
parse_files(JSON_DIR)
insert_data(JSON_DIR)


# --- Write RAM → disk ---
disk_conn = sql.connect(db_file_path)
ram_conn.backup(disk_conn)
disk_conn.close()

# --- Verify ---
disk_conn = sql.connect(db_file_path)
print("\nData in DISK DB:")
for row in disk_conn.execute("SELECT * FROM parameters;"):
    print(row)
for row in disk_conn.execute("SELECT * FROM reports;"):
    print(row)
disk_conn.close()

ram_conn.close()
