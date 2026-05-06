"""
Preprocessing utilities shared across the main pipeline and other analysis notebooks.
"""
import glob
import logging
import os
import re
from collections import defaultdict

import pandas as pd
import spacy
from dotenv import load_dotenv
from spacy.lang.en import STOP_WORDS as _EN_STOP_WORDS
from spacy.lang.pt import STOP_WORDS as _PT_STOP_WORDS

load_dotenv()

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _resolve(env_var: str, default: str) -> str:
    """Return an absolute path: resolve relative values from .env against PROJECT_ROOT."""
    raw = os.getenv(env_var, default)
    return raw if os.path.isabs(raw) else os.path.join(PROJECT_ROOT, raw)

OUTPUT_ROOT = _resolve('OUTPUT_ROOT', os.path.join(PROJECT_ROOT, 'data', 'output'))


# stopwords (from library and custom)

STOP_WORDS: set[str] = set(_EN_STOP_WORDS) | set(_PT_STOP_WORDS)

CUSTOM_STOPWORDS: set[str] = {
    'slide', 'slides', 'figure', 'fig', 'table', 'source',
    'example', 'note', 'notes', 'page', 'chapter', 'section',
    'lecture', 'module', 'unit', 'course', 'week', 'ref',
    'aula', 'figura', 'tabela', 'exemplo', 'nota', 'cada',
    'vs', 'ie', 'eg', 'et', 'al', 'orange',
    # author names
    'ana', 'rita', 'peixoto', 'martim', 'santos', 'santo', 'miguel', 'teodoro',
    'teresa', 'carlos', 'pedro', 'paulo', 'luis', 'rui',
    # spaCy applies PT morphology to EN words producing invalid lemmas
    'datar',     # "data" (EN) → lemmatised as PT verb "datar"
    'sciencer',  # "science" (EN) → lemmatised incorrectly
    # pt_core_news_sm lemmatisation artifacts
    'variávei',
    # adjectives that add noise to topic distributions
    'necessário', 'novo', 'nova', 'bom', 'boa', 'grande', 'pequeno',
    'diferente', 'possível', 'geral', 'simples', 'mesmo', 'próprio',
}

STOP_WORDS |= CUSTOM_STOPWORDS

# Only nouns and adjectives carry topic signal.
# Verbs, determiners, adverbs and pronouns are dropped before LDA.
KEEP_POS: set[str] = {'NOUN', 'PROPN', 'ADJ'}


# spaCy model (lazy singleton)

_nlp = None

def get_nlp() -> spacy.language.Language:
    """Return the shared spaCy model, loading it on first call."""
    global _nlp
    if _nlp is None:
        _nlp = spacy.load('pt_core_news_sm', disable=['parser', 'senter', 'ner'])
        logger.info("spaCy model loaded: %s", _nlp.meta['name'])
    return _nlp


# tokenizer

def tokenize(text: str) -> list[str]:
    """
    Lemmatise and filter one text string.

    Returns lowercase lemmas that are NOUN or ADJ, ≥ 3 chars, not in STOP_WORDS.
    Uses pt_core_news_sm; falls back to the original token text when the model
    produces a truncated lemma (known limitation of the small model).
    """
    nlp = get_nlp()
    doc = nlp(text.lower())
    result = []
    for token in doc:
        if not token.is_alpha or token.pos_ not in KEEP_POS:
            continue
        # lowercase always: spaCy capitalises proper noun lemmas (e.g. "Martim")
        lemma = token.lemma_.lower()
        # skip expanded contractions: do→"de o", no→"em o", ao→"a o"
        if ' ' in lemma:
            continue
        # fall back to token text when the model truncates the lemma incorrectly
        if len(lemma) < len(token.text) - 1:
            lemma = token.text
        if len(lemma) < 3 or lemma in STOP_WORDS:
            continue
        result.append(lemma)
    return result


# corpus loading

def _latest_cleaned_csvs(output_root: str) -> list[str]:
    """
    For each document subfolder, return the path to the highest-versioned
    _cleaned_vN.csv file. Avoids hardcoding the version number.
    """
    by_base: dict[str, list[tuple[int, str]]] = defaultdict(list)
    pattern = os.path.join(output_root, '**', '*_cleaned_v*.csv')
    for p in glob.glob(pattern, recursive=True):
        m = re.match(r'^(.+_cleaned)_v(\d+)\.csv$', os.path.basename(p), re.IGNORECASE)
        if m:
            base, ver = m.group(1), int(m.group(2))
            by_base[base].append((ver, p))
    return sorted(max(versions, key=lambda x: x[0])[1] for versions in by_base.values())


def load_corpus(output_root: str = OUTPUT_ROOT) -> pd.DataFrame:
    """
    Load all cleaned slide CSVs and return a single line-level DataFrame.

    Columns: file, page, line, text, source_file
    Automatically selects the latest cleaned version for each document.
    """
    paths = _latest_cleaned_csvs(output_root)
    if not paths:
        raise FileNotFoundError(f'No cleaned CSV files found under {output_root}')

    frames = []
    for p in paths:
        df = pd.read_csv(p)
        df['source_file'] = os.path.basename(p)
        frames.append(df)

    corpus = pd.concat(frames, ignore_index=True)
    corpus['text'] = corpus['text'].astype(str).str.strip()
    corpus = corpus[corpus['text'] != '']
    logger.info('Corpus loaded: %d files, %d lines.', len(frames), len(corpus))
    return corpus


# token aggregation

def tokenize_corpus(corpus: pd.DataFrame) -> pd.DataFrame:
    """
    Apply tokenize() to every line. Adds 'tokens' and 'token_count' columns.
    Returns the augmented DataFrame (does not modify the input).
    """
    out = corpus.copy()
    out['tokens']      = out['text'].apply(tokenize)
    out['token_count'] = out['tokens'].apply(len)
    return out


def build_slide_page_docs(corpus: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tokenised lines into page-level documents for LDA. 
    
    Each (source_file, page) pair becomes one document. Pages with zero tokens
    after preprocessing are dropped.
    """
    if 'tokens' not in corpus.columns:
        corpus = tokenize_corpus(corpus)

    docs = (
        corpus.groupby(['source_file', 'page'])['tokens']
        .apply(lambda rows: [t for toks in rows for t in toks])
        .reset_index()
    )
    docs.columns = ['source_file', 'page', 'tokens']
    docs['n_tokens'] = docs['tokens'].apply(len)

    empty = (docs['n_tokens'] == 0).sum()
    if empty:
        logger.warning('Dropping %d empty pages after tokenisation.', empty)
        docs = docs[docs['n_tokens'] > 0].reset_index(drop=True)

    logger.info('Page-level corpus: %d documents.', len(docs))
    return docs


def build_slide_deck_docs(corpus: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tokenised lines into deck-level documents (one per slide .pdf file).
    """
    if 'tokens' not in corpus.columns:
        corpus = tokenize_corpus(corpus)

    docs = (
        corpus.groupby('source_file')['tokens']
        .apply(lambda rows: [t for toks in rows for t in toks])
        .reset_index()
    )
    docs.columns = ['source_file', 'tokens']
    docs['n_tokens'] = docs['tokens'].apply(len)
    logger.info('Slide deck-level corpus: %d documents.', len(docs))
    return docs
