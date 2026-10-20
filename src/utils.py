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


def compute_holdout_perplexity(
    model,
    test_texts: list,
    infer_iter: int = 100,
) -> dict:
    """
    Compute holdout perplexity on unseen documents via model.make_doc() + model.infer().

    Documents are created without updating model parameters (read-only inference).
    Returns the same {raw_perplexity, log_perplexity} schema as unify_perplexity(),
    plus n_docs, n_words, and total_log_likelihood for transparency.

    Only supports tomotopy models (LDAModel, HLDAModel).

    Denominator: len(doc.words) summed over all test docs — the number of tokens
    actually presented to infer(). make_doc() silently drops tokens that are
    completely unknown to the model (not in vocabs at all), so using the raw
    input token count would inflate the denominator and deflate perplexity.
    """
    if not test_texts:
        return {
            'raw_perplexity': float('nan'), 'log_perplexity': float('nan'),
            'n_docs': 0, 'n_words': 0, 'total_log_likelihood': float('nan'),
        }

    test_docs = [model.make_doc(tokens) for tokens in test_texts]
    # infer() returns (topic_dists, list[log_likelihood_per_doc])
    _, log_likelihoods = model.infer(test_docs, iterations=infer_iter, workers=1)
    total_ll = sum(log_likelihoods)

    # Use the number of words actually presented to infer(), not the raw input
    # token count — make_doc() drops tokens unknown to the model entirely.
    total_words = sum(len(doc.words) for doc in test_docs)
    if total_words == 0:
        return {
            'raw_perplexity': float('nan'), 'log_perplexity': float('nan'),
            'n_docs': len(test_docs), 'n_words': 0,
            'total_log_likelihood': float(total_ll),
        }

    # log_perplexity follows gensim sign convention: negative = better
    log_pp = float(total_ll) / total_words   # negative (mean log-prob per word)
    raw_pp = float(np.exp(-log_pp))          # > 1, lower is better
    return {
        'raw_perplexity':       raw_pp,
        'log_perplexity':       log_pp,
        'n_docs':               len(test_docs),
        'n_words':              total_words,
        'total_log_likelihood': float(total_ll),
    }


def compute_holdout_perplexity_gensim(model, test_bow: list) -> dict:
    """
    Holdout perplexity for gensim LDA models.

    gensim's log_perplexity() uses the variational lower bound which is unreliable
    on out-of-sample data. This function infers per-document topic distributions
    (theta) on the test BoW, then scores each token as P(w|theta,phi) = sum_k theta_k * phi_kw,
    matching the conditional estimator used by compute_holdout_perplexity() for tomotopy.

    Returns the same schema as compute_holdout_perplexity().
    """
    if not test_bow:
        return {
            'raw_perplexity': float('nan'), 'log_perplexity': float('nan'),
            'n_docs': 0, 'n_words': 0, 'total_log_likelihood': float('nan'),
        }

    phi = model.get_topics()  # shape (K, V)
    total_ll = 0.0
    total_words = 0

    for doc_bow in test_bow:
        if not doc_bow:
            continue
        theta, _ = model.inference([doc_bow])
        theta = theta[0]
        s = theta.sum()
        if s > 0:
            theta = theta / s
        for word_id, count in doc_bow:
            pw = float(np.dot(theta, phi[:, word_id]))
            if pw > 0:
                total_ll += count * np.log(pw)
        total_words += sum(cnt for _, cnt in doc_bow)

    if total_words == 0:
        return {
            'raw_perplexity': float('nan'), 'log_perplexity': float('nan'),
            'n_docs': len(test_bow), 'n_words': 0,
            'total_log_likelihood': float(total_ll),
        }

    log_pp = total_ll / total_words   # negative (mean log-prob per word)
    raw_pp = float(np.exp(-log_pp))
    return {
        'raw_perplexity':       raw_pp,
        'log_perplexity':       log_pp,
        'n_docs':               len(test_bow),
        'n_words':              total_words,
        'total_log_likelihood': float(total_ll),
    }


def compute_topic_exclusivity(model, num_topics: int, n: int = 10) -> dict:
    """
    Topic exclusivity: how concentrated a topic's top words are in that topic alone.

    For the top-n words of each topic, averages P(topic | word) — obtained by
    column-normalising the topic-word matrix so each word's mass sums to 1 across
    topics. A score near 1 means the topic's leading words rarely appear in other
    topics (highly exclusive); a low score means they are shared vocabulary.

    Complements coherence (which measures within-topic word relatedness) with a
    between-topic distinctiveness view. Range [0, 1].

    Returns {'per_topic': {tid: score}, 'mean', 'min', 'max', 'n'}.
    """
    phi = _topic_word_matrix(model, num_topics)          # (K, V), rows sum to 1
    col_sums = phi.sum(axis=0, keepdims=True)             # per-word mass across topics
    col_sums[col_sums == 0] = 1.0
    p_topic_given_word = phi / col_sums                  # (K, V) = P(topic | word)

    per_topic: dict[int, float] = {}
    for k in range(num_topics):
        top_idx = np.argsort(phi[k])[::-1][:n]
        per_topic[k] = float(p_topic_given_word[k, top_idx].mean())

    scores = np.array(list(per_topic.values()), dtype=float)
    return {
        'per_topic': per_topic,
        'mean': float(scores.mean()) if scores.size else 0.0,
        'min':  float(scores.min())  if scores.size else 0.0,
        'max':  float(scores.max())  if scores.size else 0.0,
        'n': n,
    }