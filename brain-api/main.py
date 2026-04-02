import io
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

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
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
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


def search_similar_chunks_vector(question: str, top_k: int = 5) -> list[dict[str, Any]]:
    top_k = max(1, min(top_k, 20))
    question_embedding = vector_literal(embed_text(question))

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.id,
                    f.microsoft_file_id,
                    f.name,
                    f.web_url,
                    fc.chunk_index,
                    fc.content
                FROM office365.file_chunks fc
                JOIN office365.files f
                  ON f.id = fc.file_id
                WHERE fc.embedding IS NOT NULL
                ORDER BY fc.embedding <-> %s::vector
                LIMIT %s
                """,
                (question_embedding, top_k),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "file_id": row[0],
                "microsoft_file_id": row[1],
                "name": row[2],
                "web_url": row[3],
                "chunk_index": row[4],
                "content": row[5],
                "retrieval_mode": "vector",
            }
        )
    return results


def search_similar_chunks_keyword(question: str, top_k: int = 5) -> list[dict[str, Any]]:
    top_k = max(1, min(top_k, 20))
    query = f"%{question.strip()}%"

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.id,
                    f.microsoft_file_id,
                    f.name,
                    f.web_url,
                    fc.chunk_index,
                    fc.content
                FROM office365.file_chunks fc
                JOIN office365.files f
                  ON f.id = fc.file_id
                WHERE fc.content ILIKE %s
                ORDER BY f.updated_at DESC, fc.chunk_index ASC
                LIMIT %s
                """,
                (query, top_k),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "file_id": row[0],
                "microsoft_file_id": row[1],
                "name": row[2],
                "web_url": row[3],
                "chunk_index": row[4],
                "content": row[5],
                "retrieval_mode": "keyword",
            }
        )
    return results


def search_similar_chunks(question: str, top_k: int = 5) -> list[dict[str, Any]]:
    try:
        results = search_similar_chunks_vector(question, top_k)
        if results:
            return results
    except Exception:
        # Fall through to keyword search
        pass

    return search_similar_chunks_keyword(question, top_k)


def answer_question(question: str, top_k: int = 5) -> dict[str, Any]:
    question = (question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    chunks = search_similar_chunks(question, top_k)

    if not chunks:
        return {
            "answer": "I do not know based on the currently indexed documents.",
            "sources": [],
        }

    context_parts = []
    for i, chunk in enumerate(chunks, start=1):
        context_parts.append(
            f"[Source {i}] File: {chunk['name']}\n"
            f"URL: {chunk['web_url']}\n"
            f"Chunk Index: {chunk['chunk_index']}\n"
            f"Retrieval Mode: {chunk.get('retrieval_mode', 'unknown')}\n"
            f"Content:\n{chunk['content']}"
        )

    context_text = "\n\n---\n\n".join(context_parts)

    system_prompt = (
        "You answer questions using only the provided SharePoint document context. "
        "If the answer is not supported by the context, say you do not know. "
        "Prefer concise factual answers and include bullet points when asked to summarize."
    )

    user_prompt = f"""Question:
{question}

Context:
{context_text}
"""

    try:
        client = get_openai_client()
        resp = client.chat.completions.create(
            model=get_chat_model(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"OpenAI chat failed: {exc}") from exc

    answer = resp.choices[0].message.content or ""

    return {
        "answer": answer,
        "sources": chunks,
    }


# --------------------------------------------------------------------
# API routes
# --------------------------------------------------------------------

@app.get("/")
def root():
    return {"ok": True, "service": "office365-brain-api"}


@app.get("/health")
def health():
    return {"ok": True, "service": "office365-brain-api"}


@app.get("/files")
def list_files(limit: int = 100):
    limit = max(1, min(limit, 500))

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    microsoft_file_id,
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
                "name": row[1],
                "web_url": row[2],
                "mime_type": row[3],
                "last_modified_at": row[4].isoformat() if row[4] else None,
                "updated_at": row[5].isoformat() if row[5] else None,
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
                "name": row[1],
                "web_url": row[2],
                "chunk_index": row[3],
                "content": row[4],
                "embedding": row[5],
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
