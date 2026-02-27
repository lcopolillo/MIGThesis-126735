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

# individual file processing and logging
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