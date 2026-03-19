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
        logging.StreamHandler() # Mantém o feedback visual no terminal
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


logger.info("--- Starting PDF text extraction process ---")
logger.info(f"Input Folder: {INPUT_ROOT}")

# list all PDF files in the input directory
pdf_files = sorted([f for f in os.listdir(INPUT_ROOT) if f.lower().endswith('.pdf')])

if not pdf_files:
    logger.error("❌ No PDF files found in the input directory.")
    exit(1)
else:
    logger.info(f"✅ PDF files found ({len(pdf_files)}): {pdf_files}")  

# ========= Main Extraction Loop =========
for pdf_file in pdf_files:
    logger.info(f"--- Processing file: {pdf_file} ---")
    pdf_path = os.path.join(INPUT_ROOT, pdf_file)
    
    # create target directory based on the prefix of the file name
    prefix = pdf_file[:5]
    target_dir = os.path.join(OUTPUT_ROOT, prefix)
    os.makedirs(target_dir, exist_ok=True)
    
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
        base_name_clean = os.path.splitext(pdf_file)[0]
        output_file_path = get_next_version(target_dir, base_name_clean, "csv")

        # save the extracted data to a CSV file when data exists
        if data_rows:
            df = pd.DataFrame(data_rows)
            df.to_csv(output_file_path, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)
            logger.info(f"✅ Success: {pdf_file} -> {os.path.basename(output_file_path)}")
        else:
            logger.warning(f"⚠️ No text content extracted from file {pdf_file}.")
    except Exception as e:
        logger.error(f"❌ Error processing file {pdf_file}: {str(e)}")

logger.info("--- PDF text extraction process completed ---")


# ========= Data Cleaning =========
# iterate over the generated CSV files and perform cleaning operations
logger.info("--- Starting CSV cleaning process ---")

# find all CSV files in subdirectories of OUTPUT_ROOT
csv_files = []
for root, dirs, files in os.walk(OUTPUT_ROOT):
    for file in files:
        if file.lower().endswith('.csv'):
            csv_files.append(os.path.join(root, file))

csv_files = sorted(csv_files)

if not csv_files:
    logger.error("❌ No CSV files found in the output directory for cleaning.")
else:  
    logger.info(f"✅ CSV files found for cleaning ({len(csv_files)}): {[os.path.basename(f) for f in csv_files]}")

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

        # remove lines with links (http, https, www)
        regex_links = r'https?://\S+|www\.\S+'
        df = df[~df['text'].str.contains(regex_links, case=False, na=False)]
 
        # remove lines with email addresses
        regex_emails = r'\b[\w._%+-]+@[\w.-]+\.[\w]{2,}\b'
        df = df[~df['text'].str.contains(regex_emails, case=False, na=False)]

        final_count = len(df)
        # save the cleaned data back to a different file to preserve the original raw data
        cleaned_file_path = csv_path.replace(".csv", "_cleaned.csv")
        df.to_csv(cleaned_file_path, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)

        # improvements: remover o so email/url e naoo a linha toda
        # comecar com lda simpkles
        # analise exploratoria: wordcloud, frequencia de palavras
        logger.info(f"✅ Cleaned {csv_file}: {initial_count} -> {final_count} lines remaining.")
    except Exception as e:
        logger.error(f"❌ Error cleaning file {csv_file}: {str(e)}")
logger.info("--- CSV cleaning process completed ---")