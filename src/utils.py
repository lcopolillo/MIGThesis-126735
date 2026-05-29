"""
Preprocessing utilities shared across the main pipeline and other analysis notebooks.
"""
import glob
import logging
import os
import re
from collections import defaultdict

import numpy as np
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
    # high-frequency generic terms that appear in every topic and dilute separation
    'dado', 'dados',  # "data" in PT — too ubiquitous to carry topic signal
    'data',           # "data" in EN — same issue; mixed-language slides surface both forms
    # spaCy applies PT morphology to EN words producing invalid lemmas
    'datar',     # "data" (EN) → lemmatised as PT verb "datar"
    'sciencer',  # "science" (EN) → lemmatised incorrectly
    # pt_core_news_sm lemmatisation artifacts
    'variávei',
    'atividader',  # "atividade" lemmatised incorrectly by the small model
    # single-slide proper nouns / brand names that carry no topic signal
    'facebook', 'bia',
    # English abbreviations from confusion-matrix and metric slides
    'cat',   # confusion-matrix column header
    'plan',  # EN word surfacing in CRISP-DM slides; not semantically stable
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


def generate_topic_labels(model, num_topics: int, topn: int = 3) -> dict[int, str]:
    """
    Generate a label for each topic from its top-weighted terms.
    
    Both return (word, prob) tuples in descending weight order.
    """
    labels: dict[int, str] = {}
    for i in range(num_topics):
        if hasattr(model, 'show_topic'):
            terms = model.show_topic(i, topn=topn)         # gensim
        else:
            terms = model.get_topic_words(i, top_n=topn)   # tomotopy
        labels[i] = ' · '.join(w.capitalize() for w, _ in terms)
    return labels


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


# evaluation helpers (used by lda_v1..v4 and lda_comparison_v2)

def _top_words(model, topic_id: int, n: int) -> list[str]:
    """Return the top-n words for a topic, library-agnostic via duck-typing."""
    if hasattr(model, 'show_topic'):
        terms = model.show_topic(topic_id, topn=n)         # gensim
    else:
        terms = model.get_topic_words(topic_id, top_n=n)   # tomotopy
    return [w for w, _ in terms]


def compute_topic_diversity(
    model,
    num_topics: int,
    n: int = 25,
    vocab_size: int | None = None,
) -> dict:
    """
    Topic diversity per Dieng et al. 2020: |unique words across top-N of all topics| / (K * N).

    Higher = less redundancy between topics. Range [0, 1].
    Auto-clamps n if K*n > vocab_size to avoid an upper-bound below 1 by construction:
    effective n = max(5, vocab_size // num_topics). Logs a warning when clamped.
    """
    effective_n = n
    if vocab_size is not None and num_topics * n > vocab_size:
        effective_n = max(5, vocab_size // num_topics)
        logger.warning(
            'compute_topic_diversity: K*n=%d > vocab=%d; clamping n from %d to %d',
            num_topics * n, vocab_size, n, effective_n,
        )

    seen: set[str] = set()
    total = 0
    for k in range(num_topics):
        words = _top_words(model, k, effective_n)
        seen.update(words)
        total += len(words)

    diversity = len(seen) / total if total > 0 else 0.0
    return {'n': effective_n, 'unique': len(seen), 'total': total, 'diversity': diversity}


def _topic_word_matrix(model, num_topics: int) -> np.ndarray:
    """Return the (num_topics, vocab_size) word-distribution matrix for any supported model."""
    if hasattr(model, 'get_topics'):
        phi = np.asarray(model.get_topics(), dtype=float)
    else:
        phi = np.vstack([
            np.asarray(model.get_topic_word_dist(k), dtype=float) for k in range(num_topics)
        ])
    row_sums = phi.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1.0
    return phi / row_sums


def compute_topic_cosine_similarity(model, num_topics: int) -> dict:
    """
    Mean pairwise cosine similarity across topic word distributions.

    For each unordered pair (i, j), i != j, compute cos(phi_i, phi_j) and average.
    Lower values indicate more distinct topics; values close to 1 indicate redundancy.
    Returns the mean, the per-pair matrix, and min/max for diagnosis.
    """
    if num_topics < 2:
        return {'mean': 0.0, 'min': 0.0, 'max': 0.0, 'matrix': np.zeros((num_topics, num_topics))}

    phi = _topic_word_matrix(model, num_topics)
    norms = np.linalg.norm(phi, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    phi_unit = phi / norms
    sim = phi_unit @ phi_unit.T

    iu = np.triu_indices(num_topics, k=1)
    pairs = sim[iu]
    return {
        'mean': float(pairs.mean()),
        'min':  float(pairs.min()),
        'max':  float(pairs.max()),
        'matrix': sim,
    }


def compute_topic_entropy(model, num_topics: int) -> dict:
    """
    Per-topic Shannon entropy H(phi_z) = -sum_w phi(w) * log(phi(w)) in nats.

    Low entropy: peaked, focused topic concentrated on a small set of words.
    High entropy: diffuse distribution spread across the vocabulary.
    Reported alongside max entropy log(V) for normalised interpretation.
    """
    phi = _topic_word_matrix(model, num_topics)
    safe = np.where(phi > 0, phi, 1.0)
    entropies = -np.sum(phi * np.log(safe), axis=1)
    vocab_size = phi.shape[1]
    h_max = float(np.log(vocab_size)) if vocab_size > 0 else 0.0
    return {
        'per_topic':   [float(h) for h in entropies],
        'mean':        float(entropies.mean()),
        'min':         float(entropies.min()),
        'max':         float(entropies.max()),
        'h_max':       h_max,
        'mean_norm':   float(entropies.mean() / h_max) if h_max > 0 else 0.0,
    }


def unify_perplexity(model, bow_corpus=None) -> dict:
    """
    Return both raw and log perplexity on a unified scale to compare results from both libraries.

    gensim and tomotopy expose perplexity differently:
      - gensim's `lda.log_perplexity(corpus)` returns the mean log per-word probability
        per document (a negative number); raw perplexity = exp(-log_perplexity).
      - tomotopy's `lda.perplexity` is already raw perplexity.

    The returned `log_perplexity` follows gensim's sign convention (negative = better),
    derived from the raw value as `-log(raw)` for tomotopy. The returned `raw_perplexity`
    is comparable across libraries (lower = better).
    """
    if hasattr(model, 'log_perplexity'):
        if bow_corpus is None:
            raise ValueError('unify_perplexity: gensim model requires bow_corpus.')
        log_pp = float(model.log_perplexity(bow_corpus))
        raw_pp = float(np.exp(-log_pp))
        library = 'gensim'
    else:
        raw_pp = float(model.perplexity)
        log_pp = float(-np.log(raw_pp)) if raw_pp > 0 else float('nan')
        library = 'tomotopy'
    return {'log_perplexity': log_pp, 'raw_perplexity': raw_pp, 'library': library}


# def get_doc_topic_matrix(model, num_topics: int, bow_corpus=None) -> np.ndarray:
#     """
#     Return the (n_docs, num_topics) row-stochastic document-topic distribution.

#     gensim: iterates `bow_corpus`, calls `model.get_document_topics(b, minimum_probability=0.0)`,
#             and explicitly zero-pads to length `num_topics` (gensim occasionally drops
#             near-zero topics even with `minimum_probability=0`, so we don't rely on it).
#     tomotopy: stacks `np.array(d.get_topic_dist())` across `model.docs`.
#     """
#     if hasattr(model, 'get_document_topics'):
#         if bow_corpus is None:
#             raise ValueError('get_doc_topic_matrix: gensim model requires bow_corpus.')
#         rows = []
#         for bow in bow_corpus:
#             row = np.zeros(num_topics, dtype=float)
#             for tid, p in model.get_document_topics(bow, minimum_probability=0.0):
#                 if 0 <= tid < num_topics:
#                     row[tid] = p
#             rows.append(row)
#         theta = np.vstack(rows)
#     else:
#         theta = np.vstack([np.asarray(d.get_topic_dist(), dtype=float) for d in model.docs])

#     sums = theta.sum(axis=1, keepdims=True)
#     sums[sums == 0] = 1.0
#     return theta / sums
