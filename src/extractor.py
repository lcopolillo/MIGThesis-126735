"""
Data Extractor for PDF Files
Extracts text from PDF files and saves it in CSV format for further data processing.
"""
import pdfplumber
import pandas as pd
import os
import re
import csv
import logging
from dotenv import load_dotenv


# environment setup
load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_ROOT = os.getenv("INPUT_ROOT", os.path.join(os.getcwd(), "data", "input"))
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", os.path.join(os.getcwd(), "data", "output"))
LOG_ROOT = os.getenv("LOG_ROOT", os.path.join(os.getcwd(), "logs"))

os.makedirs(OUTPUT_ROOT, exist_ok=True)
os.makedirs(LOG_ROOT, exist_ok=True)

# configure logging
LOG_FILE = os.path.join(LOG_ROOT, "extraction.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# helper function to generate the next available versioned file name
def get_next_version(directory, base_name, extension):
    counter = 1
    while True:
        file_name = f"{base_name}_v{counter}.{extension}"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            return file_path
        counter += 1

# helper function to check if any extracted (raw) version already exists for a given base name
def get_existing_extraction(directory, base_name, extension):
    counter = 1
    while True:
        file_name = f"{base_name}_v{counter}.{extension}"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            # no more versions exist beyond this point
            break
        counter += 1
    # if counter never advanced past 1, no version was found
    if counter == 1:
        return None
    # return the latest existing version (counter - 1)
    latest = f"{base_name}_v{counter - 1}.{extension}"
    return os.path.join(directory, latest)


logger.info("--- Starting PDF text extraction process ---")
logger.info(f"Input Folder: {INPUT_ROOT}")

# list all PDF files in the input directory
pdf_files = sorted([f for f in os.listdir(INPUT_ROOT) if f.lower().endswith('.pdf')])

if not pdf_files:
    logger.error("No PDF files found in the input directory.")
    exit(1)
else:
    logger.info(f"({len(pdf_files)}) PDF files found: {pdf_files}")  

# ========= Main Extraction Loop =========
for pdf_file in pdf_files:
    logger.info(f"--- Processing file: {pdf_file} ---")
    pdf_path = os.path.join(INPUT_ROOT, pdf_file)
    
    # create target directory based on the prefix of the file name
    prefix = pdf_file[:5]
    target_dir = os.path.join(OUTPUT_ROOT, prefix)
    os.makedirs(target_dir, exist_ok=True)
    
    base_name_clean = os.path.splitext(pdf_file)[0]

    # skip extraction if a raw CSV for this PDF already exists — go straight to cleaning
    existing = get_existing_extraction(target_dir, base_name_clean, "csv")
    if existing is not None:
        logger.info(f"Skipping extraction for {pdf_file}: already extracted as {os.path.basename(existing)}")
        continue

    data_rows = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                raw_text = page.extract_text() or ""
                lines = raw_text.splitlines()

                for line_num, line in enumerate(lines, start=1):
                    line_text = re.sub(r"\s+", " ", line).strip()
                    if not line_text:
                        continue

                    # log the extracted line for debugging purposes
                    logger.debug(f"[{pdf_file}] PAGE {page.page_number}, LINE {line_num} - Extracted Text: {line_text}")

                    data_rows.append({
                        "file": pdf_file,
                        "page": page.page_number,
                        "line": line_num,
                        "text": line_text,
                    })

        # define the output file path with versioning to prevent overwriting existing files
        output_file_path = get_next_version(target_dir, base_name_clean, "csv")

        # save the extracted data to a CSV file when data exists
        if data_rows:
            df = pd.DataFrame(data_rows)
            df.to_csv(output_file_path, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)
            logger.info(f"Success: {pdf_file} -> {os.path.basename(output_file_path)}")
        else:
            logger.warning(f"No text content extracted from file {pdf_file}.")
    except Exception as e:
        logger.error(f"Error processing file {pdf_file}: {str(e)}")

logger.info("--- PDF text extraction process completed ---")


# ========= Data Cleaning =========
# iterate over the generated CSV files and perform cleaning operations
logger.info("--- Starting CSV cleaning process ---")

# collect only the latest raw extracted CSV for each PDF (ignore older versions and already cleaned files)
csv_files = []
for root, dirs, files in os.walk(OUTPUT_ROOT):
    # group raw (non-cleaned) CSVs by their base name (without the _vN suffix)
    raw_versions: dict[str, list[tuple[int, str]]] = {}
    for file in files:
        if not file.lower().endswith('.csv') or '_cleaned' in file:
            continue
        # parse the version number from the file name pattern: <base>_v<N>.csv
        match = re.match(r'^(.+)_v(\d+)\.csv$', file, re.IGNORECASE)
        if not match:
            continue
        base, version = match.group(1), int(match.group(2))
        raw_versions.setdefault(base, []).append((version, os.path.join(root, file)))

    # keep only the highest-versioned file for each base name
    for base, versions in raw_versions.items():
        latest_path = max(versions, key=lambda x: x[0])[1]
        csv_files.append(latest_path)

csv_files = sorted(csv_files)

if not csv_files:
    logger.error("No CSV files found in the output directory for cleaning.")
else:  
    logger.info(f"CSV files found for cleaning ({len(csv_files)}): {[os.path.basename(f) for f in csv_files]}")

for csv_path in csv_files:
    csv_file = os.path.basename(csv_path)
    logger.info(f"--- Cleaning file: {csv_file} ---")
    
    try:
        df = pd.read_csv(csv_path)
        initial_count = len(df)

        # 'text' column should be string and handle nulls
        df['text'] = df['text'].astype(str).fillna('')

        # remove empty lines or lines with only spaces
        df = df[df['text'].str.strip() != ""]

        # remove duplicate lines (keep only the first occurrence)
        df = df.drop_duplicates(subset=['text'], keep='first')

        # remove lines that contain *only* punctuation
        regex_punctuation = r'^[^\w\s]+$'
        df = df[~df['text'].str.contains(regex_punctuation, case=False, na=False)]

        # remove lines that contain isolated single characters (e.g., "a", "I") that are not part of a larger word
        regex_isolated_letters = r'^(\b[a-zA-Z]\b[\s,.()]*)+$'
        df = df[~df['text'].str.strip().str.contains(regex_isolated_letters, na=False)]  

        # remove lines that contain isolated numbers (e.g., "123", "1,000") that are not part of a larger word
        regex_isolated_numbers = r'^[\d\s,.()]+$'
        df = df[~df['text'].str.contains(regex_isolated_numbers, case=False, na=False)]

        # strip links from lines with links (http, https, www)
        regex_links = r'https?://\S+|www\.\S+'
        df['text'] = df['text'].str.replace(regex_links, '', regex=True).str.strip()
 
        # strip emails from lines with email addresses
        regex_emails = r'\b[\w._%+-]+@[\w.-]+\.[\w]{2,}\b'
        df['text'] = df['text'].str.replace(regex_emails, '', regex=True).str.strip()

        # remove lines that became empty after stripping emails/URLs
        df = df[df['text'].str.strip() != ""]

        final_count = len(df)
        # save the cleaned data to a new versioned file alongside the source raw file,
        # so each cleaning run produces a new _cleaned_vN.csv and older cleaned versions are preserved
        base_name_clean = os.path.splitext(csv_file)[0]
        cleaned_file_path = get_next_version(os.path.dirname(csv_path), f"{base_name_clean}_cleaned", "csv")
        df.to_csv(cleaned_file_path, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)

        logger.info(f"Cleaned {csv_file}: {initial_count} -> {final_count} lines remaining.")
    except Exception as e:
        logger.error(f"Error cleaning file {csv_file}: {str(e)}")
logger.info("--- CSV cleaning process completed ---")