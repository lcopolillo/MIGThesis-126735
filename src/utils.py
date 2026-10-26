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
KEEP_POS: set[str] = {'NOUN', 'PROPN', 'ADJ'}

# spaCy model (lazy singleton)
_nlp = None

def get_nlp() -> spacy.language.Language:
    """
    Return the shared spaCy model, loading it on first call.
    """
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

# topic label generation
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


IDS_COURSE_TOPICS: dict[str, str] = {
    "ICD1": "Introdução à Ciência de Dados",
    "ICD2": "Ética nos dados",
    "ICD3": "Tarefas de Ciência de Dados",
    "ICD4": "Visualização de Dados",
    "ICD5": "Ambientação à ferramenta Orange",
    "ICD6": "Preparação de dados",
    "ICD7": "Modelação Supervisionada",
    "ICD8": "Modelação Não Supervisionada",
    "ICD9": "Dados não estruturados",
}

def _llm_call(message: str, system: str | None = None, timeout: int = 60,
                thread_id: str | None = None) -> str:
    """
    Send one message to an OpenAI-compatible LLM proxy and return the assistant reply.

    Reads credentials from the environment (.env):
      LITELLM_BASE_URL   base URL of the proxy (e.g. https://…/v1)
      LITELLM_API_KEY    Bearer token
      AICORE_MODEL       model name (default: gpt-4.1-mini)

    Raises on missing config or transport error so callers can fall back gracefully.
    The `thread_id` parameter is accepted for API compatibility but unused — each
    call is stateless (system + user message only).
    """
    import requests
    import json as _json

    load_dotenv(override=True)

    base_url = os.getenv('LITELLM_BASE_URL', '').rstrip('/')
    api_key  = os.getenv('LITELLM_API_KEY')
    model    = os.getenv('AICORE_MODEL', 'gpt-4.1-mini')

    missing = [name for name, val in (
        ('LITELLM_BASE_URL', base_url), ('LITELLM_API_KEY', api_key),
    ) if not val]
    if missing:
        raise RuntimeError(
            f"LLM proxy config missing from environment/.env: {', '.join(missing)}"
        )

    messages = []
    if system:
        messages.append({'role': 'system', 'content': system})
    messages.append({'role': 'user', 'content': message})

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type':  'application/json',
    }
    payload = {'model': model, 'messages': messages, 'max_tokens': 200}

    resp = requests.post(
        f'{base_url}/chat/completions',
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    return data['choices'][0]['message']['content'].strip()


def generate_topic_labels_llm(
    model,
    num_topics: int,
    topn: int = 10,
    context: str = "Introduction to Data Science university course slides",
    use_course_topics: bool = True,
    course_topics: dict[str, str] | None = IDS_COURSE_TOPICS,
    topic_ids: list[int] | None = None,
) -> dict[int, str]:
    """
    Generate a concise 2-4 word topic label for each topic using the iaedu LLM agent.

    Calls the iaedu agent once per topic, passing the top-N words as input.
    Returns the same dict[int, str] shape as generate_topic_labels() so it
    is a drop-in replacement for axis labels and legends.

    Requires the IAEDU_* variables in the environment (or .env file).
    Falls back to the keyword-join label on any API error.

    Args:
        model:              Trained gensim or tomotopy LDA model.
        num_topics:         Number of topics in the model (used when topic_ids is None).
        topn:               Number of top words sent to the LLM per topic.
        context:            One-line description of the corpus, included in the prompt.
        use_course_topics:  When True (few-shot), anchor labels to the known
                            curriculum via `course_topics`. When False (zero-shot),
                            labels are derived from the topic keywords alone.
        course_topics:      Mapping of course unit codes to titles used as anchors
                            in few-shot mode. Ignored when use_course_topics is False.
        topic_ids:          Optional explicit list of topic ids to label. For sparse
                            models (e.g. hLDA live topics), pass this to skip dead
                            topics and avoid wasted API calls. Defaults to
                            range(num_topics).
    """
    ids = topic_ids if topic_ids is not None else range(num_topics)
    curriculum_block = ""
    if use_course_topics and course_topics:
        lines = "\n".join(f"  {code}: {title}" for code, title in course_topics.items())
        curriculum_block = (
            f"\nThe course covers the following units — use these as semantic anchors "
            f"when the keywords clearly align, but do not force a match:\n{lines}\n"
        )

    system_prompt = (
        f"You are a topic-modelling assistant. "
        f"The corpus consists of {context}. "
        f"{curriculum_block}"
        "Given a list of keywords from a topic model, respond with a concise label "
        "of 2–4 words that captures the theme. "
        "Reply with the label only — no punctuation, no explanation."
    )

    labels: dict[int, str] = {}
    for i in ids:
        if hasattr(model, 'show_topic'):
            terms = model.show_topic(i, topn=topn)
        else:
            terms = model.get_topic_words(i, top_n=topn)
        keywords = ', '.join(w for w, _ in terms)

        try:
            label = _llm_call(f"Keywords: {keywords}", system=system_prompt)
            if not label:
                raise ValueError("empty response from iaedu agent")
        except Exception as exc:
            logger.warning("LLM label failed for topic %d (%s); using keyword fallback.", i, exc)
            label = ' · '.join(w.capitalize() for w, _ in terms[:3])

        labels[i] = label
        logger.debug("Topic %d → %r", i, label)

    return labels


def align_topics_to_units(
    model,
    num_topics: int,
    labels: dict[int, str] | None = None,
    topn: int = 10,
    course_topics: dict[str, str] = IDS_COURSE_TOPICS,
    topic_ids: list[int] | None = None,
) -> dict[int, dict]:
    """
    Classify each topic to the IDS course unit (ICD1–ICD9) it best matches.

    Asks the iaedu agent, per topic, which unit the topic's keywords (and optional
    label) correspond to, restricting the answer to a unit code or NONE. Used to
    measure whether the discovered topics recover the real curriculum structure,
    and to compare zero-shot vs few-shot label alignment.

    Args:
        model:          Trained gensim or tomotopy model.
        num_topics:     Number of topics to classify (used when topic_ids is None).
        labels:         Optional {topic_id: label} to include as extra context.
        topn:           Number of top words sent to the agent per topic.
        course_topics:  Unit code → title mapping the topic is aligned against.
        topic_ids:      Optional explicit list of topic ids (e.g. hLDA live topics)
                        to skip dead topics. Defaults to range(num_topics).

    Returns:
        {topic_id: {'unit': 'ICD4'|None, 'unit_title': str|None}}
    """
    ids = topic_ids if topic_ids is not None else range(num_topics)
    unit_lines = "\n".join(f"  {code}: {title}" for code, title in course_topics.items())
    valid_codes = set(course_topics)
    system_prompt = (
        "You are a topic-modelling assistant for an Introduction to Data Science "
        "university course. Given the keywords of a topic, decide which single course "
        f"unit it best corresponds to. The units are:\n{unit_lines}\n"
        "Reply with the unit code only (e.g. ICD4), or NONE if no unit clearly matches. "
        "No explanation."
    )

    alignment: dict[int, dict] = {}
    for i in ids:
        if hasattr(model, 'show_topic'):
            terms = model.show_topic(i, topn=topn)
        else:
            terms = model.get_topic_words(i, top_n=topn)
        keywords = ', '.join(w for w, _ in terms)
        label_hint = f" (candidate label: {labels[i]})" if labels and i in labels else ""

        unit = None
        try:
            reply = _llm_call(f"Keywords: {keywords}{label_hint}", system=system_prompt)
            match = re.search(r'ICD[1-9]', reply.upper())
            if match and match.group(0) in valid_codes:
                unit = match.group(0)
        except Exception as exc:
            logger.warning("Unit alignment failed for topic %d (%s).", i, exc)

        alignment[i] = {
            'unit':       unit,
            'unit_title': course_topics.get(unit) if unit else None,
        }
        logger.debug("Topic %d → unit %r", i, unit)

    return alignment


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
