import io
import os
import re
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from fastapi import FastAPI, HTTPException

try:
    from docx import Document
except Exception:
    Document = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


app = FastAPI(title="Office365 Brain API")


GRAPH_BASE = "https://graph.microsoft.com/v1.0"


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


def graph_get_json(url: str, token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def graph_get_bytes(url: str) -> bytes:
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    return resp.content


def parse_graph_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def extract_text_from_bytes(filename: str, mime_type: str | None, content: bytes) -> str:
    mime_type = (mime_type or "").lower()
    filename = (filename or "").lower()

    if not content:
        return ""

    text_like = (
        mime_type.startswith("text/")
        or filename.endswith(".txt")
        or filename.endswith(".md")
        or filename.endswith(".csv")
        or filename.endswith(".json")
        or filename.endswith(".xml")
        or filename.endswith(".html")
        or filename.endswith(".htm")
    )
    if text_like:
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return content.decode("latin-1", errors="ignore")

    if (
        mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        or filename.endswith(".docx")
    ):
        if Document is None:
            return ""
        try:
            doc = Document(io.BytesIO(content))
            parts: list[str] = []

            for para in doc.paragraphs:
                text = (para.text or "").strip()
                if text:
                    parts.append(text)

            for table in doc.tables:
                for row in table.rows:
                    row_cells = []
                    for cell in row.cells:
                        cell_text = (cell.text or "").strip()
                        if cell_text:
                            row_cells.append(cell_text)
                    if row_cells:
                        parts.append(" | ".join(row_cells))

            return "\n".join(parts).strip()
        except Exception:
            return ""

    if mime_type == "application/pdf" or filename.endswith(".pdf"):
        if PdfReader is None:
            return ""
        try:
            reader = PdfReader(io.BytesIO(content))
            pages: list[str] = []
            for page in reader.pages:
                pages.append(page.extract_text() or "")
            return "\n".join(pages).strip()
        except Exception:
            return ""

    return ""


def list_drive_children(site_id: str, drive_id: str, item_id: str | None, token: str) -> list[dict[str, Any]]:
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


def walk_drive_files(site_id: str, drive_id: str, token: str, folder_id: str | None = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for item in list_drive_children(site_id, drive_id, folder_id, token):
        if "folder" in item:
            results.extend(walk_drive_files(site_id, drive_id, token, item.get("id")))
        elif "file" in item:
            results.append(item)

    return results


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


def upsert_file_chunks(cur, file_id, extracted_text: str) -> int:
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
        cur.execute(
            """
            INSERT INTO office365.file_chunks
            (file_id, chunk_index, content)
            VALUES (%s, %s, %s)
            """,
            (file_id, idx, chunk),
        )

    return len(chunks)


@app.get("/")
def root():
    return {"ok": True, "service": "office365-brain-api"}


@app.get("/health")
def health():
    return {"ok": True, "service": "office365-brain-api"}


@app.post("/sync/sharepoint/{site_id}/{drive_id}")
def sync_sharepoint_drive(site_id: str, drive_id: str):
    try:
        token = get_graph_token()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Microsoft auth failed: {exc}") from exc

    try:
        files = walk_drive_files(site_id, drive_id, token)
    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=500, detail=f"Graph list failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Graph list failed: {exc}") from exc

    processed_files = 0
    inserted_chunks_total = 0
    skipped_files = 0
    errors: list[dict[str, Any]] = []

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for item in files:
                try:
                    microsoft_file_id = item.get("id")
                    if not microsoft_file_id:
                        skipped_files += 1
                        continue

                    name = item.get("name") or "Unnamed file"
                    web_url = item.get("webUrl")
                    mime_type = ((item.get("file") or {}).get("mimeType")) or item.get("mimeType")
                    owner_microsoft_user_id = (
                        (((item.get("createdBy") or {}).get("user") or {}).get("id"))
                        or (((item.get("lastModifiedBy") or {}).get("user") or {}).get("id"))
                    )
                    last_modified_at = parse_graph_datetime(item.get("lastModifiedDateTime"))

                    download_url = item.get("@microsoft.graph.downloadUrl")
                    extracted_text = ""

                    if download_url:
                        try:
                            file_bytes = graph_get_bytes(download_url)
                            extracted_text = extract_text_from_bytes(name, mime_type, file_bytes)
                        except requests.HTTPError as exc:
                            errors.append(
                                {
                                    "file": name,
                                    "microsoft_file_id": microsoft_file_id,
                                    "error": f"Download failed: {exc}",
                                }
                            )
                        except Exception as exc:
                            errors.append(
                                {
                                    "file": name,
                                    "microsoft_file_id": microsoft_file_id,
                                    "error": f"Extraction failed: {exc}",
                                }
                            )

                    file_id = upsert_file_record(
                        cur=cur,
                        microsoft_file_id=microsoft_file_id,
                        owner_microsoft_user_id=owner_microsoft_user_id,
                        source_type="sharepoint_drive",
                        name=name,
                        web_url=web_url,
                        mime_type=mime_type,
                        last_modified_at=last_modified_at,
                        raw_metadata=item,
                        extracted_text=extracted_text,
                    )

                    inserted_chunks = upsert_file_chunks(cur, file_id, extracted_text)
                    inserted_chunks_total += inserted_chunks
                    processed_files += 1

                except Exception as exc:
                    errors.append(
                        {
                            "file": item.get("name"),
                            "microsoft_file_id": item.get("id"),
                            "error": str(exc),
                        }
                    )

        conn.commit()

    return {
        "ok": True,
        "site_id": site_id,
        "drive_id": drive_id,
        "processed_files": processed_files,
        "inserted_chunks": inserted_chunks_total,
        "skipped_files": skipped_files,
        "errors": errors[:25],
        "error_count": len(errors),
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
                    name,
                    web_url,
                    mime_type,
                    last_modified_at,
                    updated_at
                FROM office365.files
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    items = []
    for row in rows:
        items.append(
            {
                "microsoft_file_id": row[0],
                "name": row[1],
                "web_url": row[2],
                "mime_type": row[3],
                "last_modified_at": row[4].isoformat() if row[4] else None,
                "updated_at": row[5].isoformat() if row[5] else None,
            }
        )

    return {"ok": True, "files": items}


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
                ORDER BY f.updated_at DESC NULLS LAST, fc.chunk_index ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "microsoft_file_id": row[0],
                "name": row[1],
                "web_url": row[2],
                "chunk_index": row[3],
                "content": row[4],
                "embedding": row[5],
            }
        )

    return {"ok": True, "chunks": results}
