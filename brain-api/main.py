import io
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import psycopg
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from openai import OpenAI
from pydantic import BaseModel

try:
    from docx import Document
except Exception:
    Document = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


load_dotenv()

app = FastAPI(title="Office365 Brain API")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

ROUTE_SOURCE_TYPES: dict[str, list[str]] = {
    "admin": ["m365-admin", "outlook-admin", "copilot", "sharepoint", "teams"],
    "developer": ["graph", "outlook-developer"],
    "training": ["outlook-training", "teams", "sharepoint"],
    "sharepoint": ["sharepoint"],
    "teams": ["teams"],
    "copilot": ["copilot"],
    "ambiguous": [],
}

OFFICIAL_WEB_DOMAINS = [
    "learn.microsoft.com",
    "support.microsoft.com",
    "www.microsoft.com",
    "microsoft.com",
    "techcommunity.microsoft.com",
]


# --------------------------------------------------------------------
# Request models
# --------------------------------------------------------------------

class ChatRequest(BaseModel):
    question: str
    top_k: int = 5


class OpenAIChatMessage(BaseModel):
    role: str
    content: Any


class OpenAIChatRequest(BaseModel):
    model: str
    messages: list[OpenAIChatMessage]
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: Optional[bool] = False


# --------------------------------------------------------------------
# General utilities
# --------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_db_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg.connect(database_url)


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY")

    base_url = os.getenv("OPENAI_BASE_URL")
    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)

    return OpenAI(api_key=api_key)


def get_chat_model() -> str:
    return (
        os.getenv("OPENAI_CHAT_MODEL")
        or os.getenv("CHAT_MODEL")
        or "gpt-4.1-mini"
    )


def get_embedding_model() -> str:
    return (
        os.getenv("OPENAI_EMBEDDING_MODEL")
        or os.getenv("EMBEDDING_MODEL")
        or "text-embedding-3-small"
    )


def get_public_model_id() -> str:
    return os.getenv("PUBLIC_MODEL_ID", "office365-assistant")


def get_web_fallback_enabled() -> bool:
    value = (os.getenv("WEB_FALLBACK_ENABLED") or "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_web_fallback_provider() -> str:
    return (os.getenv("WEB_FALLBACK_PROVIDER") or "ollama").strip().lower()


def get_web_fallback_model() -> str:
    return os.getenv("WEB_FALLBACK_CHAT_MODEL") or "gpt-oss:120b-cloud"


def get_web_fallback_client() -> OpenAI:
    provider = get_web_fallback_provider()

    if provider == "ollama":
        api_key = (
            os.getenv("WEB_FALLBACK_API_KEY")
            or os.getenv("OLLAMA_API_KEY")
            or "ollama"
        )
        base_url = (
            os.getenv("WEB_FALLBACK_BASE_URL")
            or os.getenv("OLLAMA_BASE_URL")
            or "http://localhost:11434/v1"
        )
        return OpenAI(api_key=api_key, base_url=base_url)

    api_key = os.getenv("WEB_FALLBACK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing WEB_FALLBACK_API_KEY and OPENAI_API_KEY")

    base_url = os.getenv("WEB_FALLBACK_BASE_URL") or os.getenv("OPENAI_BASE_URL")
    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)

    return OpenAI(api_key=api_key)


def get_ollama_auth_key() -> str:
    api_key = os.getenv("WEB_FALLBACK_API_KEY") or os.getenv("OLLAMA_API_KEY")
    if not api_key:
        raise RuntimeError("Missing WEB_FALLBACK_API_KEY and OLLAMA_API_KEY for Ollama fallback")
    return api_key


def get_ollama_openai_base_url() -> str:
    return (
        os.getenv("WEB_FALLBACK_BASE_URL")
        or os.getenv("OLLAMA_BASE_URL")
        or "http://localhost:11434/v1"
    ).rstrip("/")


def get_ollama_api_base_url() -> str:
    base_url = get_ollama_openai_base_url()
    parsed = urlparse(base_url)
    path = (parsed.path or "").rstrip("/")

    if path.endswith("/v1"):
        path = path[:-3]
    if not path.endswith("/api"):
        path = f"{path}/api" if path else "/api"

    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", "")).rstrip("/")


def chunk_text(text: str, chunk_size: int = 2000, overlap: int = 200) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []

    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    chunks: list[str] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= text_len:
            break
        start = max(end - overlap, start + 1)

    return chunks


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(str(x) for x in values) + "]"


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def extract_user_question_from_messages(messages: list[OpenAIChatMessage]) -> str:
    if not messages:
        return ""

    for msg in reversed(messages):
        if msg.role != "user":
            continue

        content = msg.content

        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_value = item.get("text")
                    if text_value:
                        text_parts.append(str(text_value))
            return "\n".join(text_parts).strip()

        return str(content).strip()

    return ""


# --------------------------------------------------------------------
# Retrieval deduplication helpers
# --------------------------------------------------------------------

def normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_title(title: str | None) -> str:
    value = normalize_whitespace(title).lower()
    value = re.sub(r"\s*\|\s*microsoft learn\s*$", "", value)
    value = re.sub(r"\s*-\s*microsoft graph\s*\|\s*microsoft learn\s*$", "", value)
    value = re.sub(r"\s*-\s*microsoft teams\s*\|\s*microsoft learn\s*$", "", value)
    value = re.sub(r"\s*-\s*microsoft 365 copilot connectors\s*\|\s*microsoft learn\s*$", "", value)
    value = re.sub(r"[\|\-–—:]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def canonicalize_url(url: str | None) -> str:
    if not url:
        return ""

    try:
        parsed = urlparse(url.strip())
        scheme = (parsed.scheme or "https").lower()
        netloc = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()

        path = re.sub(r"/+", "/", path)
        if path.endswith("/") and path != "/":
            path = path[:-1]

        return f"{scheme}://{netloc}{path}"
    except Exception:
        value = url.strip().lower()
        value = value.split("#", 1)[0]
        value = value.split("?", 1)[0]
        value = re.sub(r"/+", "/", value)
        if value.endswith("/") and not value.endswith("://"):
            value = value[:-1]
        return value


def doc_family_key(chunk: dict[str, Any]) -> str:
    canonical_url = canonicalize_url(chunk.get("web_url"))
    normalized_title = normalize_title(chunk.get("name"))
    microsoft_file_id = (chunk.get("microsoft_file_id") or "").strip()

    if canonical_url:
        return f"url:{canonical_url}"
    if normalized_title:
        return f"title:{normalized_title}"
    if microsoft_file_id:
        return f"id:{microsoft_file_id}"

    return f"fallback:{chunk.get('file_id')}:{chunk.get('chunk_index')}"


def dedupe_chunks(chunks: list[dict[str, Any]], top_k: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_families: set[str] = set()

    for chunk in chunks:
        family_key = doc_family_key(chunk)
        if family_key in seen_families:
            continue

        seen_families.add(family_key)
        deduped.append(chunk)

        if len(deduped) >= top_k:
            break

    return deduped


def candidate_pool_size(top_k: int) -> int:
    top_k = max(1, min(top_k, 20))
    return min(max(top_k * 6, 18), 80)


# --------------------------------------------------------------------
# Microsoft Graph helpers
# --------------------------------------------------------------------

def get_graph_token() -> str:
    tenant_id = get_required_env("MICROSOFT_TENANT_ID")
    client_id = get_required_env("MICROSOFT_CLIENT_ID")
    client_secret = get_required_env("MICROSOFT_CLIENT_SECRET")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(
        token_url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Microsoft token response did not include access_token")
    return access_token


def graph_get_json(url: str, token: str) -> dict[str, Any]:
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def graph_get_bytes(url: str, token: str) -> bytes:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(url, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.content


def list_drive_children(
    site_id: str,
    drive_id: str,
    item_id: str | None,
    token: str,
) -> list[dict[str, Any]]:
    if item_id:
        url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/items/{item_id}/children"
    else:
        url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/root/children"

    items: list[dict[str, Any]] = []

    while url:
        payload = graph_get_json(url, token)
        items.extend(payload.get("value", []))
        url = payload.get("@odata.nextLink")

    return items


def walk_drive_files(
    site_id: str,
    drive_id: str,
    token: str,
    folder_id: str | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for item in list_drive_children(site_id, drive_id, folder_id, token):
        if "folder" in item:
            results.extend(walk_drive_files(site_id, drive_id, token, item.get("id")))
        elif "file" in item:
            results.append(item)

    return results


# --------------------------------------------------------------------
# File extraction
# --------------------------------------------------------------------

def extract_text_from_docx_bytes(data: bytes) -> str:
    if Document is None:
        return ""
    try:
        doc = Document(io.BytesIO(data))
        parts: list[str] = []
        for para in doc.paragraphs:
            text = (para.text or "").strip()
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    except Exception:
        return ""


def extract_text_from_pdf_bytes(data: bytes) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(io.BytesIO(data))
        pages: list[str] = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        return "\n".join(pages).strip()
    except Exception:
        return ""


def extract_text_from_plain_bytes(data: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return data.decode(encoding, errors="ignore").strip()
        except Exception:
            pass
    return ""


def extract_text_from_file_bytes(data: bytes, mime_type: str | None, name: str) -> str:
    mime_type = mime_type or ""
    lower_name = (name or "").lower()

    if "wordprocessingml.document" in mime_type or lower_name.endswith(".docx"):
        return extract_text_from_docx_bytes(data)

    if "pdf" in mime_type or lower_name.endswith(".pdf"):
        return extract_text_from_pdf_bytes(data)

    if (
        mime_type.startswith("text/")
        or lower_name.endswith(".txt")
        or lower_name.endswith(".md")
        or lower_name.endswith(".csv")
        or lower_name.endswith(".json")
        or lower_name.endswith(".html")
        or lower_name.endswith(".xml")
    ):
        return extract_text_from_plain_bytes(data)

    return ""


# --------------------------------------------------------------------
# Embeddings and retrieval
# --------------------------------------------------------------------

def embed_text(text: str) -> list[float]:
    text = (text or "").strip()
    if not text:
        raise RuntimeError("Cannot embed empty text")

    client = get_openai_client()
    resp = client.embeddings.create(
        model=get_embedding_model(),
        input=text,
    )
    return resp.data[0].embedding


def upsert_file_record(
    cur,
    microsoft_file_id: str,
    owner_microsoft_user_id: str | None,
    source_type: str,
    name: str,
    web_url: str | None,
    mime_type: str | None,
    last_modified_at: datetime | None,
    raw_metadata: dict[str, Any],
    extracted_text: str,
):
    cur.execute(
        """
        INSERT INTO office365.files
        (
            microsoft_file_id,
            owner_microsoft_user_id,
            source_type,
            name,
            web_url,
            mime_type,
            last_modified_at,
            raw_metadata,
            extracted_text,
            created_at,
            updated_at
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, now(), now()
        )
        ON CONFLICT (microsoft_file_id)
        DO UPDATE SET
            owner_microsoft_user_id = EXCLUDED.owner_microsoft_user_id,
            source_type = EXCLUDED.source_type,
            name = EXCLUDED.name,
            web_url = EXCLUDED.web_url,
            mime_type = EXCLUDED.mime_type,
            last_modified_at = EXCLUDED.last_modified_at,
            raw_metadata = EXCLUDED.raw_metadata,
            extracted_text = EXCLUDED.extracted_text,
            updated_at = now()
        RETURNING id
        """,
        (
            microsoft_file_id,
            owner_microsoft_user_id,
            source_type,
            name,
            web_url,
            mime_type,
            last_modified_at,
            psycopg.types.json.Jsonb(raw_metadata),
            extracted_text,
        ),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Failed to upsert office365.files for {microsoft_file_id}")
    return row[0]


def upsert_file_chunks(cur, file_id: int, extracted_text: str) -> int:
    cur.execute(
        """
        DELETE FROM office365.file_chunks
        WHERE file_id = %s
        """,
        (file_id,),
    )

    chunks = chunk_text(extracted_text)
    if not chunks:
        return 0

    for idx, chunk in enumerate(chunks):
        embedding = vector_literal(embed_text(chunk))
        cur.execute(
            """
            INSERT INTO office365.file_chunks
            (file_id, chunk_index, content, embedding)
            VALUES (%s, %s, %s, %s::vector)
            """,
            (file_id, idx, chunk, embedding),
        )

    return len(chunks)


def _filtered_source_clause(source_types: list[str] | None) -> tuple[str, tuple[Any, ...]]:
    if not source_types:
        return "", tuple()
    return " AND f.source_type = ANY(%s)", (source_types,)


def search_similar_chunks_vector(
    question: str,
    top_k: int = 5,
    source_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    top_k = max(1, min(top_k, 20))
    initial_limit = candidate_pool_size(top_k)
    question_embedding = vector_literal(embed_text(question))
    source_clause, source_params = _filtered_source_clause(source_types)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    f.id,
                    f.microsoft_file_id,
                    f.source_type,
                    f.name,
                    f.web_url,
                    fc.chunk_index,
                    fc.content
                FROM office365.file_chunks fc
                JOIN office365.files f
                  ON f.id = fc.file_id
                WHERE fc.embedding IS NOT NULL
                {source_clause}
                ORDER BY fc.embedding <-> %s::vector
                LIMIT %s
                """,
                (*source_params, question_embedding, initial_limit),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "file_id": row[0],
                "microsoft_file_id": row[1],
                "source_type": row[2],
                "name": row[3],
                "web_url": row[4],
                "chunk_index": row[5],
                "content": row[6],
                "retrieval_mode": "vector",
            }
        )

    return dedupe_chunks(results, top_k)


def search_similar_chunks_keyword(
    question: str,
    top_k: int = 5,
    source_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    top_k = max(1, min(top_k, 20))
    initial_limit = candidate_pool_size(top_k)
    query = f"%{question.strip()}%"
    source_clause, source_params = _filtered_source_clause(source_types)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    f.id,
                    f.microsoft_file_id,
                    f.source_type,
                    f.name,
                    f.web_url,
                    fc.chunk_index,
                    fc.content
                FROM office365.file_chunks fc
                JOIN office365.files f
                  ON f.id = fc.file_id
                WHERE fc.content ILIKE %s
                {source_clause}
                ORDER BY f.updated_at DESC, fc.chunk_index ASC
                LIMIT %s
                """,
                (query, *source_params, initial_limit),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "file_id": row[0],
                "microsoft_file_id": row[1],
                "source_type": row[2],
                "name": row[3],
                "web_url": row[4],
                "chunk_index": row[5],
                "content": row[6],
                "retrieval_mode": "keyword",
            }
        )

    return dedupe_chunks(results, top_k)


def search_similar_chunks(
    question: str,
    top_k: int = 5,
    source_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    try:
        results = search_similar_chunks_vector(question, top_k, source_types=source_types)
        if results:
            return results
    except Exception:
        pass

    return search_similar_chunks_keyword(question, top_k, source_types=source_types)


# --------------------------------------------------------------------
# Routing and quality heuristics
# --------------------------------------------------------------------

def classify_question(question: str) -> str:
    q = normalize_whitespace(question).lower()

    if not q:
        return "ambiguous"

    developer_terms = [
        " api", " graph", " endpoint", " sdk", " auth", " oauth", " token",
        " schema", " extension", " manifest", " add-in", " add in", " developer",
        " rest ", "http ", "json ", "permission scope", "webhook"
    ]
    admin_terms = [
        " admin", " tenant", " policy", " policies", " compliance", " security",
        " license", " licensing", " governance", " retention", " audit",
        " configure", " enable", " disable", " org", " organization", " role",
        " sharepoint admin", "teams admin", "entra", "exchange online powershell"
    ]
    teams_terms = [
        "teams", "team ", "channel", "guest access", "external access",
        "meeting", "chat", "lobby", "shared channel", "private channel"
    ]
    sharepoint_terms = [
        "sharepoint", "onedrive", "site", "document library", "library",
        "hub site", "page", "permissions", "sharing link", "browser idle sign out"
    ]
    copilot_terms = ["copilot", "agent", "grounding", "pay-as-you-go", "pay as you go"]
    training_terms = [
        "how do i", "how to", "steps", "walk me through", "where do i",
        "click", "show me how", "create a", "add a", "set up my", "organize"
    ]

    if any(term in q for term in developer_terms):
        return "developer"
    if any(term in q for term in copilot_terms):
        return "copilot"
    if any(term in q for term in teams_terms):
        return "teams"
    if any(term in q for term in sharepoint_terms):
        return "sharepoint"
    if any(term in q for term in admin_terms):
        return "admin"
    if any(term in q for term in training_terms):
        return "training"

    return "ambiguous"


def is_process_question(question: str) -> bool:
    q = normalize_whitespace(question).lower()
    process_markers = [
        "how do i", "how to", "steps", "walk me through", "click", "set up",
        "configure", "enable", "disable", "create", "add", "remove", "change",
        "update", "turn on", "turn off"
    ]
    return any(marker in q for marker in process_markers)


def search_similar_chunks_routed(question: str, top_k: int = 5) -> tuple[list[dict[str, Any]], str, list[str], bool]:
    route = classify_question(question)
    preferred_source_types = ROUTE_SOURCE_TYPES.get(route, [])

    chunks: list[dict[str, Any]] = []
    used_fallback = False

    if preferred_source_types:
        chunks = search_similar_chunks(question, top_k=top_k, source_types=preferred_source_types)

    if len(chunks) < min(3, top_k):
        fallback_chunks = search_similar_chunks(question, top_k=top_k, source_types=None)
        if fallback_chunks:
            chunks = fallback_chunks
            used_fallback = bool(preferred_source_types)

    return chunks, route, preferred_source_types, used_fallback


def route_sources_match_user_intent(route: str, chunks: list[dict[str, Any]]) -> bool:
    source_types = [str(c.get("source_type") or "").strip() for c in chunks]

    if route == "training":
        return any(s in {"outlook-training", "teams", "sharepoint"} for s in source_types)

    if route == "developer":
        return any(s in {"graph", "outlook-developer"} for s in source_types)

    if route == "admin":
        return any(s in {"m365-admin", "outlook-admin", "copilot"} for s in source_types)

    if route == "teams":
        return any(s == "teams" for s in source_types)

    if route == "sharepoint":
        return any(s == "sharepoint" for s in source_types)

    if route == "copilot":
        return any(s == "copilot" for s in source_types)

    return True


def assess_retrieval_quality(chunks: list[dict[str, Any]], route: str, used_fallback: bool) -> str:
    if not chunks:
        return "none"

    source_types = {str(chunk.get("source_type") or "").strip() for chunk in chunks if chunk.get("source_type")}
    if len(chunks) < 2:
        return "weak"

    if route != "ambiguous":
        preferred = set(ROUTE_SOURCE_TYPES.get(route, []))
        if preferred and not any(source in preferred for source in source_types):
            return "weak"

    if used_fallback and len(source_types) >= 3:
        return "mixed"

    if len(source_types) >= 4:
        return "mixed"

    return "grounded"


def chunks_are_summary_only(chunks: list[dict[str, Any]]) -> bool:
    if not chunks:
        return True

    summary_markers = [
        "learning objectives",
        "module",
        "modules",
        "units",
        "learning path",
        "summary",
        "overview",
        "prerequisites",
        "feedback beginner",
        "this module is part of",
    ]

    summary_hits = 0
    for chunk in chunks[:3]:
        content = str(chunk.get("content") or "").lower()
        if any(marker in content for marker in summary_markers):
            summary_hits += 1

    return summary_hits >= 2


def chunks_have_step_support(chunks: list[dict[str, Any]]) -> bool:
    if not chunks:
        return False

    step_markers = [
        "1.",
        "2.",
        "step",
        "select ",
        "click ",
        "open ",
        "choose ",
        "go to ",
        "type ",
        "save",
        "signatures",
        "settings",
        "options",
        "message tab",
        "file,",
    ]

    for chunk in chunks[:3]:
        content = str(chunk.get("content") or "").lower()
        if sum(1 for marker in step_markers if marker in content) >= 2:
            return True

    return False


def question_requires_web_fallback(question: str, chunks: list[dict[str, Any]], route: str, retrieval_quality: str) -> bool:
    if retrieval_quality in {"none", "weak"}:
        return True

    q = normalize_whitespace(question).lower()
    top_contents = " ".join(str(c.get("content") or "").lower() for c in chunks[:3])
    top_titles = " ".join(str(c.get("name") or "").lower() for c in chunks[:3])

    if is_process_question(question):
        if chunks_are_summary_only(chunks):
            return True
        if not chunks_have_step_support(chunks):
            return True

    if "outlook on the web" in q or "outlook web" in q or "outlook on web" in q:
        if "new outlook for windows" in top_contents or "new outlook for windows" in top_titles:
            return True

    if "on the web" in q or "browser" in q:
        desktop_markers = ["new outlook for windows", "windows", "desktop", "message tab"]
        if any(marker in top_contents or marker in top_titles for marker in desktop_markers):
            if not any("outlook on the web" in str(c.get("content") or "").lower() for c in chunks[:3]):
                return True

    if route == "training" and is_process_question(question):
        generic_training_markers = ["create and manage signatures", "customize", "learning objectives"]
        generic_hits = 0
        for chunk in chunks[:3]:
            content = str(chunk.get("content") or "").lower()
            if any(marker in content for marker in generic_training_markers):
                generic_hits += 1
        if generic_hits >= 2 and not chunks_have_step_support(chunks):
            return True

    return False


def detect_requested_product(question: str) -> str | None:
    q = normalize_whitespace(question).lower()

    product_map = {
        "word": ["word", "microsoft word"],
        "excel": ["excel", "microsoft excel"],
        "powerpoint": ["powerpoint", "ppt", "microsoft powerpoint"],
        "outlook": ["outlook", "outlook on the web", "owa", "new outlook"],
        "teams": ["teams", "microsoft teams"],
        "sharepoint": ["sharepoint"],
        "onedrive": ["onedrive"],
        "exchange": ["exchange", "exchange online", "mail flow", "quarantine"],
        "azure": ["azure", "azure portal", "azureportal"],
        "entra": ["entra", "microsoft entra", "azure active directory", "azuread"],
        "purview": ["purview", "microsoft purview", "compliance center", "compliancecenterv2"],
        "security": ["security", "microsoft 365 defender", "defender"],
        "power platform": ["power platform", "power apps", "power automate", "power pages", "dynamics 365"],
        "search": ["microsoft search", "search & intelligence", "search and intelligence", "bing for business"],
        "copilot": ["copilot", "microsoft 365 copilot"],
        "loop": ["loop", "microsoft loop"],
        "forms": ["forms", "microsoft forms"],
        "planner": ["planner", "microsoft planner"],
        "to do": ["to do", "todo", "microsoft to do"],
        "onenote": ["onenote", "one note"],
        "lists": ["lists", "microsoft lists"],
        "stream": ["stream", "microsoft stream"],
        "sway": ["sway", "microsoft sway"],
        "visio": ["visio", "microsoft visio"],
        "whiteboard": ["whiteboard", "microsoft whiteboard"],
        "people": ["people", "contacts"],
        "clipchamp": ["clipchamp"],
        "engage": ["engage", "viva engage", "yammer"],
        "yammer": ["yammer", "viva engage"],
        "connections": ["connections", "viva connections"],
        "insights": ["insights", "viva insights"],
        "learning": ["learning", "viva learning"],
        "learning activities": ["learning activities"],
        "kaizala": ["kaizala"],
        "viva": ["viva", "microsoft viva"],
        "power apps": ["power apps"],
        "power automate": ["power automate"],
        "power pages": ["power pages"],
        "m365 apps": ["microsoft 365 apps", "office apps", "microsoft 365 app"],
        "graph": ["graph", "microsoft graph", "webhook", "subscription", "delta query", "api"],
        "office": ["office", "microsoft 365", "office 365", "m365"],
    }

    for product, terms in product_map.items():
        if any(term in q for term in terms):
            return product

    return None


def retrieved_chunks_match_product(requested_product: str | None, chunks: list[dict[str, Any]]) -> bool:
    if not requested_product:
        return True

    top_text = " ".join(
        (
            str(chunk.get("source_type") or "") + " "
            + str(chunk.get("name") or "") + " "
            + str(chunk.get("content") or "")
        ).lower()
        for chunk in chunks[:3]
    )

    product_terms = {
        "word": ["word", "microsoft word", "document", "autoformat", "autocorrect"],
        "excel": ["excel", "worksheet", "workbook", "formula", "spreadsheet"],
        "powerpoint": ["powerpoint", "presentation", "slide", "slides"],
        "outlook": ["outlook", "mailbox", "signature", "email", "owa"],
        "teams": ["teams", "meeting", "channel", "chat"],
        "sharepoint": ["sharepoint", "site", "document library", "list"],
        "onedrive": ["onedrive", "sync", "files on-demand", "sharing"],
        "exchange": ["exchange", "exchange online", "mail flow", "quarantine", "transport"],
        "azure": ["azure", "subscription", "resource group", "virtual machine", "portal.azure.com"],
        "entra": ["entra", "azure active directory", "identity", "conditional access", "directory"],
        "purview": ["purview", "compliance", "risk", "data loss prevention", "retention"],
        "security": ["security", "defender", "incident", "threat", "secure score"],
        "power platform": ["power platform", "power apps", "power automate", "power pages", "dataverse", "dynamics 365"],
        "search": ["microsoft search", "search", "bing", "search & intelligence"],
        "copilot": ["copilot", "prompt", "grounding", "agent"],
        "loop": ["loop", "workspace", "component"],
        "forms": ["forms", "survey", "quiz", "response"],
        "planner": ["planner", "task", "plan", "bucket"],
        "to do": ["to do", "task", "list"],
        "onenote": ["onenote", "notebook", "section", "page"],
        "lists": ["lists", "list item", "column formatting"],
        "stream": ["stream", "video", "meeting recording"],
        "sway": ["sway", "interactive report"],
        "visio": ["visio", "diagram", "flowchart"],
        "whiteboard": ["whiteboard", "canvas", "ink"],
        "people": ["people", "contact", "contacts"],
        "clipchamp": ["clipchamp", "video editing"],
        "engage": ["engage", "yammer", "community", "conversation"],
        "yammer": ["yammer", "community", "conversation"],
        "connections": ["connections", "dashboard", "viva"],
        "insights": ["insights", "viva insights", "productivity", "wellbeing"],
        "learning": ["learning", "viva learning", "course"],
        "learning activities": ["learning activities"],
        "kaizala": ["kaizala", "mobile chat"],
        "viva": ["viva", "insights", "connections", "learning", "engage"],
        "power apps": ["power apps", "canvas app", "model-driven"],
        "power automate": ["power automate", "flow", "automation"],
        "power pages": ["power pages", "website", "portal"],
        "m365 apps": ["microsoft 365 apps", "office apps", "click-to-run"],
        "graph": ["graph", "subscription", "webhook", "permission", "api", "delta query"],
        "office": [
            "office",
            "microsoft 365",
            "word",
            "excel",
            "powerpoint",
            "outlook",
            "teams",
            "sharepoint",
            "onedrive",
        ],
    }

    expected_terms = product_terms.get(requested_product, [requested_product])
    return any(term in top_text for term in expected_terms)


def should_force_web_fallback_for_product_mismatch(question: str, chunks: list[dict[str, Any]]) -> bool:
    requested_product = detect_requested_product(question)
    if not requested_product:
        return False

    return not retrieved_chunks_match_product(requested_product, chunks)


# --------------------------------------------------------------------
# Prompt construction and answer repair
# --------------------------------------------------------------------

def build_context_text(chunks: list[dict[str, Any]]) -> str:
    context_parts = []
    for i, chunk in enumerate(chunks, start=1):
        context_parts.append(
            f"[Indexed Source {i}] Source Type: {chunk.get('source_type', 'unknown')}\n"
            f"File: {chunk['name']}\n"
            f"URL: {chunk['web_url']}\n"
            f"Chunk Index: {chunk['chunk_index']}\n"
            f"Retrieval Mode: {chunk.get('retrieval_mode', 'unknown')}\n"
            f"Content:\n{chunk['content']}"
        )
    return "\n\n---\n\n".join(context_parts)


def build_system_prompt(question: str, route: str, retrieval_quality: str) -> str:
    answer_mode = "process" if is_process_question(question) else "concept"

    if retrieval_quality == "weak":
        uncertainty_instruction = (
            "Retrieval quality is weak. You must explicitly say that you cannot fully confirm the answer from the indexed Office365 sources."
        )
    elif retrieval_quality == "mixed":
        uncertainty_instruction = (
            "Retrieval quality is mixed. You must explicitly mention ambiguity, blended sourcing, or missing confirmation."
        )
    else:
        uncertainty_instruction = (
            "Retrieval quality is grounded. Answer directly but stay strictly within the evidence."
        )

    process_instruction = (
        "This is a process-oriented question. Under 'Recommendation / next step', use a numbered list only if the retrieved context supports an ordered procedure. Do not use bullet points for ordered steps."
        if answer_mode == "process"
        else
        "This is a concept-oriented question. Under 'Recommendation / next step', use bullet points, not numbered steps, unless the retrieved context clearly requires a sequence."
    )

    return f"""
You are an Office365 consultant assistant.

You must answer using ONLY the provided retrieved context.
Do not use outside knowledge.
Do not fill gaps with generic Microsoft product knowledge.
If the context is insufficient, say so explicitly.

Primary route: {route}
Retrieval quality: {retrieval_quality}
Answer mode: {answer_mode}

You MUST output exactly these 5 sections in this exact order:

Direct answer
Key details
Recommendation / next step
Risks / limitations
Source basis

Formatting rules:
- Direct answer:
  Write 1 to 3 plain sentences only.
  Do not use bullets.
  Do not use numbering.
- Key details:
  Use bullet points only.
  Include 2 to 5 bullets.
- Recommendation / next step:
  If the question is process-oriented and the context supports steps, use a numbered list only.
  Otherwise use bullet points.
- Risks / limitations:
  Use bullet points only.
- Source basis:
  Use bullet points only.
  Each bullet must be short.
  Each bullet must use this format: source_type - file title
  Do not include direct quotes.
  Do not include copied sentences from the source text.
  Do not include URLs.

Hard rules:
- Do not omit any section.
- Do not rename any section.
- Do not use bullets in Direct answer.
- Do not use numbered steps unless the question is process-oriented and the context supports a sequence.
- If the answer is not supported, say exactly: "I cannot confirm this from the indexed Office365 sources."

Behavior rules:
- Never imply certainty beyond the evidence.
- Never invent UI paths, commands, or settings that are not supported in the context.
- If the sources conflict or do not cleanly match the route, say so.
- Prefer being too strict over being too speculative.
- {process_instruction}
- {uncertainty_instruction}
""".strip()


def build_user_prompt(question: str, context_text: str) -> str:
    return f"""Question:
{question}

Context:
{context_text}
"""


def build_repair_prompt(original_answer: str, question: str) -> str:
    answer_mode = "process" if is_process_question(question) else "concept"
    return f"""Rewrite the answer so it exactly follows the required format.
Do not add new facts.
Do not remove supported facts unless necessary to fix formatting.
Keep the same meaning.
Fix only section compliance and formatting.

Question type: {answer_mode}

Required sections in exact order:
Direct answer
Key details
Recommendation / next step
Risks / limitations
Source basis

Rules:
- Direct answer must be 1 to 3 plain sentences only.
- Direct answer must not contain bullets or numbering.
- Key details must use bullets.
- Risks / limitations must use bullets.
- Source basis must use short bullets only.
- Source basis bullets must use this exact format: source_type - file title
- Source basis must not contain quotation marks.
- Source basis must not contain copied source sentences.
- Recommendation / next step must use numbered steps only for process questions and only if the answer already contains a supported procedure.

Original answer:
{original_answer}
"""


def extract_section_block(answer: str, section_name: str, next_section_name: str | None = None) -> str:
    if next_section_name:
        pattern = rf"{re.escape(section_name)}\s*(.*?)(?:\n\s*{re.escape(next_section_name)}\b)"
    else:
        pattern = rf"{re.escape(section_name)}\s*(.*)$"

    match = re.search(pattern, answer, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def has_required_sections(answer: str) -> bool:
    required = [
        "Direct answer",
        "Key details",
        "Recommendation / next step",
        "Risks / limitations",
        "Source basis",
    ]
    return all(section in answer for section in required)


def direct_answer_has_bullets(answer: str) -> bool:
    block = extract_section_block(answer, "Direct answer", "Key details")
    if not block:
        return True

    lines = [line.strip() for line in block.splitlines() if line.strip()]
    return any(line.startswith("-") or re.match(r"^\d+\.", line) for line in lines)


def recommendation_has_numbered_steps(answer: str) -> bool:
    block = extract_section_block(answer, "Recommendation / next step", "Risks / limitations")
    if not block:
        return False

    lines = [line.strip() for line in block.splitlines() if line.strip()]
    return any(re.match(r"^\d+\.", line) for line in lines)


def source_basis_needs_repair(answer: str) -> bool:
    block = extract_section_block(answer, "Source basis", None)
    if not block:
        return True

    lines = [line.strip() for line in block.splitlines() if line.strip()]
    if not lines:
        return True

    bullet_lines = [line for line in lines if line.startswith("-")]
    if not bullet_lines:
        return True

    for line in bullet_lines:
        if '"' in line or "'" in line:
            return True
        if "http://" in line or "https://" in line:
            return True
        if "Source " in line:
            return True
        if len(line) > 180:
            return True
        body = line[1:].strip()
        if " - " not in body:
            return True

    return False


def answer_needs_repair(answer: str, question: str) -> bool:
    if not has_required_sections(answer):
        return True

    if direct_answer_has_bullets(answer):
        return True

    if is_process_question(question):
        if not recommendation_has_numbered_steps(answer):
            return True

    if source_basis_needs_repair(answer):
        return True

    return False


def generate_indexed_answer_text(question: str, chunks: list[dict[str, Any]], route: str, retrieval_quality: str) -> str:
    context_text = build_context_text(chunks)
    system_prompt = build_system_prompt(question, route, retrieval_quality)
    user_prompt = build_user_prompt(question, context_text)

    client = get_openai_client()

    resp = client.chat.completions.create(
        model=get_chat_model(),
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )

    answer = (resp.choices[0].message.content or "").strip()

    if answer_needs_repair(answer, question):
        repair_prompt = build_repair_prompt(answer, question)
        repair_resp = client.chat.completions.create(
            model=get_chat_model(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": repair_prompt},
            ],
            temperature=0.0,
        )
        repaired = (repair_resp.choices[0].message.content or "").strip()
        if repaired:
            answer = repaired

    return answer


# --------------------------------------------------------------------
# Web fallback
# --------------------------------------------------------------------

def sanitize_web_query(question: str, route: str) -> str:
    cleaned = normalize_whitespace(question)
    if route == "admin":
        return f"Microsoft 365 admin {cleaned}"
    if route == "developer":
        return f"Microsoft Graph developer {cleaned}"
    if route == "teams":
        return f"Microsoft Teams {cleaned}"
    if route == "sharepoint":
        return f"SharePoint Online {cleaned}"
    if route == "copilot":
        return f"Microsoft 365 Copilot {cleaned}"
    if route == "training":
        return f"Microsoft 365 how to {cleaned}"
    return f"Microsoft 365 {cleaned}"


def filter_official_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    official: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []

    for result in results:
        url = str(result.get("url") or "")
        netloc = urlparse(url).netloc.lower()
        if any(netloc == domain or netloc.endswith(f".{domain}") for domain in OFFICIAL_WEB_DOMAINS):
            official.append(result)
        else:
            other.append(result)

    return official if official else other


def ollama_web_search(query: str, max_results: int = 5) -> list[dict[str, Any]]:
    api_key = get_ollama_auth_key()
    api_base = get_ollama_api_base_url()

    resp = requests.post(
        f"{api_base}/web_search",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "max_results": max(1, min(max_results, 10)),
        },
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("results", []) or []


def ollama_web_fetch(url: str) -> dict[str, Any]:
    api_key = get_ollama_auth_key()
    api_base = get_ollama_api_base_url()

    resp = requests.post(
        f"{api_base}/web_fetch",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"url": url},
        timeout=90,
    )
    resp.raise_for_status()
    return resp.json()


def build_ollama_web_context(question: str, route: str, retrieval_quality: str, chunks: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    query = sanitize_web_query(question, route)
    search_results = ollama_web_search(query, max_results=5)
    ranked_results = filter_official_results(search_results)[:3]

    fetched_pages: list[dict[str, Any]] = []
    for result in ranked_results:
        url = str(result.get("url") or "").strip()
        if not url:
            continue
        try:
            page = ollama_web_fetch(url)
            fetched_pages.append(
                {
                    "title": page.get("title") or result.get("title") or url,
                    "url": url,
                    "content": str(page.get("content") or "")[:12000],
                    "snippet": result.get("content") or "",
                }
            )
        except Exception:
            fetched_pages.append(
                {
                    "title": result.get("title") or url,
                    "url": url,
                    "content": str(result.get("content") or "")[:4000],
                    "snippet": result.get("content") or "",
                }
            )

    indexed_context = build_context_text(chunks) if chunks else "No indexed context available."

    web_blocks: list[str] = []
    for i, page in enumerate(fetched_pages, start=1):
        web_blocks.append(
            f"[Web Source {i}]\n"
            f"Title: {page['title']}\n"
            f"URL: {page['url']}\n"
            f"Snippet: {page['snippet']}\n"
            f"Content:\n{page['content']}"
        )

    web_context = "\n\n---\n\n".join(web_blocks) if web_blocks else "No web results available."

    prompt = f"""
You are an Office365 consultant assistant.

Task:
Answer the user's question using indexed Office365 context first when useful, then current web evidence gathered from Ollama web search and Ollama web fetch.

Rules:
- Prefer Microsoft official sources when available.
- Do not claim certainty beyond the evidence.
- If web evidence is missing or conflicting, say so explicitly.
- Do not invent UI paths, policies, or capabilities.
- Treat indexed context as internal supporting evidence and web context as current external evidence.

Question route: {route}
Indexed retrieval quality: {retrieval_quality}

You MUST output exactly these 5 sections in this exact order:

Direct answer
Key details
Recommendation / next step
Risks / limitations
Source basis

Formatting rules:
- Direct answer: 1 to 3 plain sentences only, no bullets, no numbering.
- Key details: bullet points only.
- Recommendation / next step:
  - numbered steps only for process questions
  - bullet points otherwise
- Risks / limitations: bullet points only.
- Source basis:
  - bullet points only
  - short entries only
  - use this format:
    - web - source title
    - indexed - file title
  - do not include raw URLs
  - do not include quotation marks
  - do not copy long source text verbatim

Question:
{question}

Indexed Office365 context:
{indexed_context}

Web context:
{web_context}
""".strip()

    return prompt, fetched_pages


def run_openai_web_fallback(question: str, route: str, retrieval_quality: str, chunks: list[dict[str, Any]]) -> str:
    client = get_web_fallback_client()
    model = get_web_fallback_model()
    prompt = f"""
You are an Office365 consultant assistant.

Task:
Answer the user's question using web search because the indexed Office365 corpus is weak, incomplete, missing, stale, or not sufficiently procedural.

Source priority:
1. Use indexed Office365 context first if it is materially helpful.
2. Use live web search to fill missing or current product details.
3. Prefer official Microsoft sources:
   - learn.microsoft.com
   - support.microsoft.com
   - microsoft.com
   - techcommunity.microsoft.com only when necessary
4. Do not rely on unstated background knowledge if indexed and web sources do not support the answer.

Question route: {route}
Indexed retrieval quality: {retrieval_quality}

Formatting rules:
You MUST output exactly these 5 sections in this exact order:

Direct answer
Key details
Recommendation / next step
Risks / limitations
Source basis

Additional formatting rules:
- Direct answer: 1 to 3 plain sentences only, no bullets, no numbering.
- Key details: bullet points only.
- Recommendation / next step:
  - numbered steps only for process questions
  - bullet points otherwise
- Risks / limitations: bullet points only.
- Source basis:
  - bullet points only
  - short entries only
  - use this format:
    - web - source title
    - indexed - file title
  - do not include raw URLs
  - do not include quotation marks
  - do not copy long source text verbatim

Question:
{question}

Indexed Office365 context:
{build_context_text(chunks) if chunks else 'No indexed context available.'}
""".strip()

    resp = client.responses.create(
        model=model,
        tools=[{"type": "web_search"}],
        input=prompt,
    )

    output_text = (getattr(resp, "output_text", None) or "").strip()
    if not output_text:
        raise RuntimeError("Web fallback returned empty output_text")

    return output_text


def run_ollama_web_fallback(question: str, route: str, retrieval_quality: str, chunks: list[dict[str, Any]]) -> str:
    client = get_web_fallback_client()
    model = get_web_fallback_model()
    prompt, fetched_pages = build_ollama_web_context(question, route, retrieval_quality, chunks)

    if not fetched_pages:
        raise RuntimeError("Ollama web search returned no usable results")

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an Office365 consultant assistant. "
                    "Use the provided indexed context and fetched web context only. "
                    "Do not invent facts. Follow the formatting rules exactly."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    output_text = (resp.choices[0].message.content or "").strip()
    if not output_text:
        raise RuntimeError("Ollama fallback returned empty content")

    return output_text


def run_web_fallback(question: str, route: str, retrieval_quality: str, chunks: list[dict[str, Any]]) -> str:
    provider = get_web_fallback_provider()

    if provider == "openai":
        return run_openai_web_fallback(question, route, retrieval_quality, chunks)

    if provider == "ollama":
        return run_ollama_web_fallback(question, route, retrieval_quality, chunks)

    raise RuntimeError(f"Unsupported WEB_FALLBACK_PROVIDER: {provider}")


def build_weak_training_answer(chunks: list[dict[str, Any]]) -> str:
    source_lines = []
    for chunk in chunks[:3]:
        source_type = str(chunk.get("source_type") or "unknown").strip()
        name = str(chunk.get("name") or "Untitled").strip()
        source_lines.append(f"- {source_type} - {name}")

    if not source_lines:
        source_lines.append("- none - no supporting sources were retrieved")

    return (
        "Direct answer\n"
        "I cannot confirm the end-user Outlook steps from the indexed Office365 sources.\n\n"
        "Key details\n"
        "- The current retrieved results are developer, admin, or summary-level training content rather than concrete user interface steps.\n"
        "- Answering with click-by-click Outlook steps here would require unsupported assumptions.\n\n"
        "Recommendation / next step\n"
        "1. Add end-user Outlook documentation with concrete procedures to the training corpus.\n"
        "2. Re-ask after the training source is indexed.\n\n"
        "Risks / limitations\n"
        "- The current corpus appears to contain related signature material, but not the user-facing steps needed for a reliable training answer.\n"
        "- A more specific answer would risk mixing summary content with unsupported assumptions.\n\n"
        "Source basis\n"
        + "\n".join(source_lines)
    )


# --------------------------------------------------------------------
# Answer flow
# --------------------------------------------------------------------

def answer_question(question: str, top_k: int = 5) -> dict[str, Any]:
    question = (question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    chunks, route, preferred_source_types, used_fallback = search_similar_chunks_routed(question, top_k)

    if not chunks:
        retrieval_quality = "none"

        if get_web_fallback_enabled():
            try:
                web_answer = run_web_fallback(question, route, retrieval_quality, [])
                return {
                    "answer": web_answer,
                    "sources": [],
                    "route": route,
                    "preferred_source_types": preferred_source_types,
                    "retrieval_quality": retrieval_quality,
                    "used_fallback": used_fallback,
                    "answer_origin": "web_fallback",
                }
            except Exception as exc:
                return {
                    "answer": (
                        "Direct answer\n"
                        "I cannot confirm this from the indexed Office365 sources, and web fallback also failed.\n\n"
                        "Key details\n"
                        f"- Web fallback error: {exc}\n"
                        "- No relevant indexed context was returned.\n\n"
                        "Recommendation / next step\n"
                        "1. Verify WEB_FALLBACK environment variables.\n"
                        "2. Retry the question after confirming API access.\n\n"
                        "Risks / limitations\n"
                        "- No supporting indexed evidence was found.\n"
                        "- Web fallback did not complete successfully.\n\n"
                        "Source basis\n"
                        "- none - no supporting sources were retrieved"
                    ),
                    "sources": [],
                    "route": route,
                    "preferred_source_types": preferred_source_types,
                    "retrieval_quality": retrieval_quality,
                    "used_fallback": used_fallback,
                    "answer_origin": "fallback_error",
                }

        return {
            "answer": (
                "Direct answer\n"
                "I cannot confirm this from the indexed Office365 sources.\n\n"
                "Key details\n"
                "- No relevant retrieved context was returned.\n"
                "- The current index did not provide supporting evidence for this question.\n\n"
                "Recommendation / next step\n"
                "1. Add or refresh documentation for this topic in the indexed corpus.\n"
                "2. Retry the question with more specific product or admin terms.\n\n"
                "Risks / limitations\n"
                "- The current index did not return supporting evidence.\n"
                "- Any stronger answer would require unsupported assumptions.\n\n"
                "Source basis\n"
                "- none - no supporting sources were retrieved"
            ),
            "sources": [],
            "route": route,
            "preferred_source_types": preferred_source_types,
            "retrieval_quality": retrieval_quality,
            "used_fallback": used_fallback,
            "answer_origin": "indexed_only",
        }

    retrieval_quality = assess_retrieval_quality(chunks, route, used_fallback)

    if not route_sources_match_user_intent(route, chunks):
        retrieval_quality = "weak"

    if should_force_web_fallback_for_product_mismatch(question, chunks):
        retrieval_quality = "weak"

    if question_requires_web_fallback(question, chunks, route, retrieval_quality):
        retrieval_quality = "weak"

    if route == "training" and retrieval_quality == "weak" and not get_web_fallback_enabled():
        return {
            "answer": build_weak_training_answer(chunks),
            "sources": chunks,
            "route": route,
            "preferred_source_types": preferred_source_types,
            "retrieval_quality": retrieval_quality,
            "used_fallback": used_fallback,
            "answer_origin": "indexed_only",
        }

    if retrieval_quality in {"weak", "none"} and get_web_fallback_enabled():
        try:
            web_answer = run_web_fallback(question, route, retrieval_quality, chunks)
            return {
                "answer": web_answer,
                "sources": chunks,
                "route": route,
                "preferred_source_types": preferred_source_types,
                "retrieval_quality": retrieval_quality,
                "used_fallback": used_fallback,
                "answer_origin": "web_fallback",
            }
        except Exception as exc:
            if route == "training":
                fallback_answer = build_weak_training_answer(chunks)
                fallback_answer = fallback_answer.strip() + f"\n- Web fallback failed: {exc}"
                return {
                    "answer": fallback_answer,
                    "sources": chunks,
                    "route": route,
                    "preferred_source_types": preferred_source_types,
                    "retrieval_quality": retrieval_quality,
                    "used_fallback": used_fallback,
                    "answer_origin": "fallback_error_then_indexed",
                }

            indexed_answer = generate_indexed_answer_text(question, chunks, route, retrieval_quality)
            indexed_answer = indexed_answer.strip() + f"\n\nRisks / limitations\n- Web fallback failed: {exc}\n"
            return {
                "answer": indexed_answer,
                "sources": chunks,
                "route": route,
                "preferred_source_types": preferred_source_types,
                "retrieval_quality": retrieval_quality,
                "used_fallback": used_fallback,
                "answer_origin": "fallback_error_then_indexed",
            }

    try:
        answer = generate_indexed_answer_text(question, chunks, route, retrieval_quality)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"OpenAI chat failed: {exc}") from exc

    return {
        "answer": answer,
        "sources": chunks,
        "route": route,
        "preferred_source_types": preferred_source_types,
        "retrieval_quality": retrieval_quality,
        "used_fallback": used_fallback,
        "answer_origin": "indexed_only",
    }


# --------------------------------------------------------------------
# API routes
# --------------------------------------------------------------------

@app.get("/")
def root():
    return {"ok": True, "service": "office365-brain-api"}


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "office365-brain-api",
        "web_fallback_enabled": get_web_fallback_enabled(),
        "web_fallback_provider": get_web_fallback_provider(),
        "web_fallback_model": get_web_fallback_model() if get_web_fallback_enabled() else None,
        "ollama_api_base_url": get_ollama_api_base_url() if get_web_fallback_enabled() and get_web_fallback_provider() == "ollama" else None,
    }


@app.get("/files")
def list_files(limit: int = 100):
    limit = max(1, min(limit, 500))

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    microsoft_file_id,
                    source_type,
                    name,
                    web_url,
                    mime_type,
                    last_modified_at,
                    updated_at
                FROM office365.files
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    files = []
    for row in rows:
        files.append(
            {
                "microsoft_file_id": row[0],
                "source_type": row[1],
                "name": row[2],
                "web_url": row[3],
                "mime_type": row[4],
                "last_modified_at": row[5].isoformat() if row[5] else None,
                "updated_at": row[6].isoformat() if row[6] else None,
            }
        )

    return {"ok": True, "files": files}


@app.get("/file-chunks")
def list_file_chunks(limit: int = 100):
    limit = max(1, min(limit, 500))

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.microsoft_file_id,
                    f.source_type,
                    f.name,
                    f.web_url,
                    fc.chunk_index,
                    fc.content,
                    fc.embedding
                FROM office365.file_chunks fc
                JOIN office365.files f
                  ON f.id = fc.file_id
                ORDER BY f.updated_at DESC, fc.chunk_index ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    chunks = []
    for row in rows:
        chunks.append(
            {
                "microsoft_file_id": row[0],
                "source_type": row[1],
                "name": row[2],
                "web_url": row[3],
                "chunk_index": row[4],
                "content": row[5],
                "embedding": row[6],
            }
        )

    return {"ok": True, "chunks": chunks}


@app.post("/chat")
def chat(req: ChatRequest):
    return answer_question(req.question, top_k=req.top_k)


@app.post("/sync/sharepoint/{site_id}/{drive_id}")
def sync_sharepoint_drive(site_id: str, drive_id: str):
    try:
        token = get_graph_token()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Graph auth failed: {exc}") from exc

    try:
        items = walk_drive_files(site_id, drive_id, token)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Drive walk failed: {exc}") from exc

    inserted_or_updated = 0
    processed_files = 0
    chunk_count_total = 0
    skipped_files = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for item in items:
                microsoft_file_id = item.get("id")
                name = item.get("name") or "unknown"
                web_url = item.get("webUrl")
                last_modified_at = parse_iso_datetime(item.get("lastModifiedDateTime"))
                raw_metadata = item
                file_facet = item.get("file") or {}
                mime_type = file_facet.get("mimeType")
                owner_microsoft_user_id = site_id

                download_url = item.get("@microsoft.graph.downloadUrl")
                extracted_text = ""

                if download_url:
                    try:
                        file_bytes = graph_get_bytes(download_url, token="")
                    except Exception:
                        try:
                            resp = requests.get(download_url, timeout=120)
                            resp.raise_for_status()
                            file_bytes = resp.content
                        except Exception:
                            file_bytes = b""

                    if file_bytes:
                        extracted_text = extract_text_from_file_bytes(file_bytes, mime_type, name)

                try:
                    file_id = upsert_file_record(
                        cur=cur,
                        microsoft_file_id=microsoft_file_id,
                        owner_microsoft_user_id=owner_microsoft_user_id,
                        source_type="sharepoint",
                        name=name,
                        web_url=web_url,
                        mime_type=mime_type,
                        last_modified_at=last_modified_at,
                        raw_metadata=raw_metadata,
                        extracted_text=extracted_text,
                    )

                    num_chunks = upsert_file_chunks(cur, file_id, extracted_text)
                    inserted_or_updated += 1
                    processed_files += 1
                    chunk_count_total += num_chunks
                except Exception as exc:
                    skipped_files += 1
                    print(f"Failed processing file {name} ({microsoft_file_id}): {exc}")

            conn.commit()

    return {
        "ok": True,
        "site_id": site_id,
        "drive_id": drive_id,
        "fetched": len(items),
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "files_upserted": inserted_or_updated,
        "chunks_upserted": chunk_count_total,
    }


# --------------------------------------------------------------------
# OpenAI-compatible routes for Open WebUI
# --------------------------------------------------------------------

@app.get("/v1/models")
def v1_models():
    return {
        "object": "list",
        "data": [
            {
                "id": get_public_model_id(),
                "object": "model",
                "owned_by": "office365-brain-api",
            }
        ],
    }


@app.post("/v1/chat/completions")
def v1_chat_completions(req: OpenAIChatRequest):
    question = extract_user_question_from_messages(req.messages)
    top_k = 5

    result = answer_question(question, top_k=top_k)
    answer = result["answer"]

    created = int(utcnow().timestamp())

    return {
        "id": f"chatcmpl-{created}",
        "object": "chat.completion",
        "created": created,
        "model": get_public_model_id(),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": answer,
                },
                "finish_reason": "stop",
            }
        ],
    }
