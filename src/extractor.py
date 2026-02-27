import pdfplumber
import pandas as pd
import os
import re
import csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FOLDER = os.path.join(BASE_DIR, "data", "input")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "data", "output")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_next_version(directory, base_name, extension):
    counter = 1
    while True:
        file_name = f"{base_name}_v{counter}.{extension}"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            return file_path
        counter += 1

# define the correct output file name with versioning to prevent overwriting existing files
OUTPUT_FILE = get_next_version(OUTPUT_FOLDER, "final_dataset", "csv")

data_rows = []

print("--- Initial Batch Processing ---")
print(f"Input Folder: {INPUT_FOLDER}")

# list all PDF files in the input directory
pdf_files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.pdf')])

if not pdf_files:
    print("❌ No PDF files found in the input directory.")
    exit(1)
else:
    print(f"✅ PDF files found ({len(pdf_files)}): {pdf_files}")  

# organize extracted data into structured csv files
for pdf_file in pdf_files:
    print(f"Processed File: {pdf_file}")
    pdf_path = os.path.join(INPUT_FOLDER, pdf_file)
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            raw_text = page.extract_text() or ""
            # split page into lines and sanitize each line separately
            lines = raw_text.splitlines()
            for line_num, line in enumerate(lines, start=1):
                line_text = re.sub(r"\s+", " ", line).strip()
                if not line_text:
                    continue
                print(f"Extracted Text from {pdf_file}, Page {page.page_number}, Line {line_num}:")
                print(line_text)
                data_rows.append({
                    "file": pdf_file,
                    "page": page.page_number,
                    "line": line_num,
                    "text": line_text,
                })

df = pd.DataFrame(data_rows)
df.to_csv(OUTPUT_FILE, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)
print(f"Data successfully saved to {OUTPUT_FILE}")