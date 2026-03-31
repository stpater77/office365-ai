import os
import io
import re
import json
import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from docx import Document
from pypdf import PdfReader

app = FastAPI(title="office365-brain-api")


class ChatRequest(BaseModel):
    message: str


class RetrieveRequest(BaseModel):
    query: str
    limit: int = 5


CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200
STOP_WORDS = {
    "what", "does", "the", "a", "an", "and", "or", "but", "about", "say",
    "is", "are", "was", "were", "to", "for", "of", "in", "on", "at", "by",
    "with", "from", "that", "this", "it", "as", "be", "into", "yet"
}


def get_db_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=500, detail="Missing DATABASE_URL")
    return psycopg.connect(database_url)


def get_graph_token():
    tenant_id = os.getenv("MICROSOFT_TENANT_ID")
    client_id = os.getenv("MICROSOFT_CLIENT_ID")
    client_secret = os.getenv("MICROSOFT_CLIENT_SECRET")

    if not tenant_id or not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Missing Microsoft credentials")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(token_url, data=data)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=response.text)

    return response.json()["access_token"]


def graph_get(url: str):
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}

    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url, headers=headers)

    return response


def fetch_graph_users():
    response = graph_get("https://graph.microsoft.com/v1.0/users?$top=100")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_graph_messages(user_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/users/{user_id}/messages?$top=25")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_graph_events(user_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/users/{user_id}/events?$top=25")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_graph_contacts(user_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/users/{user_id}/contacts?$top=100")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_sharepoint_sites():
    response = graph_get("https://graph.microsoft.com/v1.0/sites/getAllSites")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_site_drives(site_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_drive_root(drive_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


def fetch_drive_root_children(drive_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$top=200")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def fetch_drive_item(drive_id: str, item_id: str):
    response = graph_get(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


def fetch_drive_item_children(drive_id: str, item_id: str):
    response = graph_get(
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children?$top=200"
    )
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json().get("value", [])


def collect_all_drive_items(drive_id: str):
    all_items = []
    stack = fetch_drive_root_children(drive_id)

    while stack:
        item = stack.pop()
        all_items.append(item)

        if item.get("folder"):
            child_id = item.get("id")
            if child_id:
                children = fetch_drive_item_children(drive_id, child_id)
                stack.extend(children)

    return all_items


def download_file_bytes(download_url: str):
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(download_url)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to download SharePoint file")

    return response.content


def extract_text_from_txt(file_bytes: bytes):
    return file_bytes.decode("utf-8", errors="ignore")


def extract_text_from_docx(file_bytes: bytes):
    doc = Document(io.BytesIO(file_bytes))
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
    return "\n".join(paragraphs).strip()


def extract_text_from_pdf(file_bytes: bytes):
    reader = PdfReader(io.BytesIO(file_bytes))
    pages = []

    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            pages.append(text.strip())

    return "\n\n".join(pages).strip()


def extract_text_for_item(item: dict):
    if item.get("folder"):
        return None

    download_url = item.get("@microsoft.graph.downloadUrl")
    if not download_url:
        return None

    file_info = item.get("file") or {}
    mime_type = file_info.get("mimeType")
    name = (item.get("name") or "").lower()

    try:
        file_bytes = download_file_bytes(download_url)

        if name.endswith(".txt") or name.endswith(".md"):
            return extract_text_from_txt(file_bytes)

        if name.endswith(".docx") or mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            return extract_text_from_docx(file_bytes)

        if name.endswith(".pdf") or mime_type == "application/pdf":
            return extract_text_from_pdf(file_bytes)

        return None
    except Exception:
        return None


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP):
    if not text:
        return []

    cleaned = " ".join(text.split())
    if not cleaned:
        return []

    chunks = []
    start = 0
    text_len = len(cleaned)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = cleaned[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end >= text_len:
            break

        start = max(end - chunk_overlap, start + 1)

    return chunks


def upsert_file_chunks(cur, microsoft_file_id: str, extracted_text: str):
    cur.execute(
        "DELETE FROM office365.file_chunks WHERE microsoft_file_id = %s",
        (microsoft_file_id,),
    )

    chunks = chunk_text(extracted_text)
    inserted_chunks = 0

    for idx, chunk in enumerate(chunks):
        cur.execute(
            """
            INSERT INTO office365.file_chunks
            (microsoft_file_id, chunk_index, chunk_text, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            """,
            (microsoft_file_id, idx, chunk),
        )
        inserted_chunks += 1

    return inserted_chunks


def normalize_query_terms(query: str):
    words = re.findall(r"[a-zA-Z0-9_]+", query.lower())
    terms = [w for w in words if w not in STOP_WORDS and len(w) >= 3]
    seen = []
    for term in terms:
        if term not in seen:
            seen.append(term)
    return seen


def retrieve_chunks(query: str, limit: int = 5):
    terms = normalize_query_terms(query)

    if not terms:
        return []

    where_clauses = []
    params = []

    for term in terms:
        where_clauses.append("fc.chunk_text ILIKE %s")
        params.append(f"%{term}%")

    sql = f"""
        SELECT
            fc.microsoft_file_id,
            f.name,
            f.web_url,
            fc.chunk_index,
            fc.chunk_text,
            (
                {" + ".join(["CASE WHEN fc.chunk_text ILIKE %s THEN 1 ELSE 0 END" for _ in terms])}
            ) AS score
        FROM office365.file_chunks fc
        JOIN office365.files f
          ON f.microsoft_file_id = fc.microsoft_file_id
        WHERE {" OR ".join(where_clauses)}
        ORDER BY score DESC, f.updated_at DESC, fc.chunk_index ASC
        LIMIT %s
    """

    score_params = [f"%{term}%" for term in terms]
    final_params = score_params + params + [limit]

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, final_params)
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(
            {
                "microsoft_file_id": row[0],
                "name": row[1],
                "web_url": row[2],
                "chunk_index": row[3],
                "chunk_text": row[4],
                "score": row[5],
            }
        )

    return results


@app.get("/health")
def health():
    return {"ok": True, "service": "office365-brain-api"}


@app.get("/graph/test")
def graph_test():
    token = get_graph_token()
    return {"ok": True, "token_received": bool(token)}


@app.get("/graph/users")
def list_users():
    return {"value": fetch_graph_users()}


@app.get("/graph/sites")
def list_sites():
    return {"value": fetch_sharepoint_sites()}


@app.get("/graph/site-drives/{site_id}")
def list_site_drives(site_id: str):
    return {"value": fetch_site_drives(site_id)}


@app.get("/graph/site-drive-root/{drive_id}")
def get_site_drive_root(drive_id: str):
    return fetch_drive_root(drive_id)


@app.get("/graph/site-drive-items/{drive_id}")
def list_site_drive_items(drive_id: str):
    return {"value": fetch_drive_root_children(drive_id)}


@app.get("/graph/site-drive-item/{drive_id}/{item_id}")
def get_site_drive_item(drive_id: str, item_id: str):
    return fetch_drive_item(drive_id, item_id)


@app.get("/graph/site-drive-item-children/{drive_id}/{item_id}")
def list_site_drive_item_children(drive_id: str, item_id: str):
    return {"value": fetch_drive_item_children(drive_id, item_id)}


@app.post("/retrieve")
def retrieve(req: RetrieveRequest):
    results = retrieve_chunks(req.query, req.limit)
    return {
        "ok": True,
        "query": req.query,
        "terms": normalize_query_terms(req.query),
        "count": len(results),
        "results": results,
    }


@app.post("/sync/users")
def sync_users():
    users = fetch_graph_users()
    tenant_id = os.getenv("MICROSOFT_TENANT_ID")

    inserted = 0
    updated = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for user in users:
                microsoft_user_id = user.get("id")
                email = user.get("mail") or user.get("userPrincipalName")
                display_name = user.get("displayName")

                cur.execute(
                    """
                    INSERT INTO office365.users
                    (tenant_id, microsoft_user_id, email, display_name, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (microsoft_user_id)
                    DO UPDATE SET
                      email = EXCLUDED.email,
                      display_name = EXCLUDED.display_name,
                      updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted;
                    """,
                    (tenant_id, microsoft_user_id, email, display_name),
                )

                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    return {
        "ok": True,
        "fetched": len(users),
        "inserted": inserted,
        "updated": updated,
    }


@app.post("/sync/mail/{user_id}")
def sync_mail(user_id: str):
    messages = fetch_graph_messages(user_id)

    inserted = 0
    updated = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for msg in messages:
                microsoft_message_id = msg.get("id")
                subject = msg.get("subject")
                sent_at = msg.get("sentDateTime")
                user_email = user_id

                sender = None
                if msg.get("from") and msg["from"].get("emailAddress"):
                    sender = msg["from"]["emailAddress"].get("address")

                recipients = []
                for r in msg.get("toRecipients", []):
                    if r.get("emailAddress"):
                        recipients.append(r["emailAddress"].get("address"))

                body_text = None
                if msg.get("body"):
                    body_text = msg["body"].get("content")

                cur.execute(
                    """
                    INSERT INTO office365.emails
                    (microsoft_message_id, user_email, subject, sender, recipients, sent_at, body_text, raw_metadata, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (microsoft_message_id)
                    DO UPDATE SET
                      user_email = EXCLUDED.user_email,
                      subject = EXCLUDED.subject,
                      sender = EXCLUDED.sender,
                      recipients = EXCLUDED.recipients,
                      sent_at = EXCLUDED.sent_at,
                      body_text = EXCLUDED.body_text,
                      raw_metadata = EXCLUDED.raw_metadata,
                      updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted;
                    """,
                    (
                        microsoft_message_id,
                        user_email,
                        subject,
                        sender,
                        json.dumps(recipients),
                        sent_at,
                        body_text,
                        json.dumps(msg),
                    ),
                )

                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    return {
        "ok": True,
        "fetched": len(messages),
        "inserted": inserted,
        "updated": updated,
    }


@app.post("/sync/calendar/{user_id}")
def sync_calendar(user_id: str):
    events = fetch_graph_events(user_id)

    inserted = 0
    updated = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for event in events:
                microsoft_event_id = event.get("id")
                subject = event.get("subject")

                start_obj = event.get("start") or {}
                end_obj = event.get("end") or {}

                starts_at = start_obj.get("dateTime")
                ends_at = end_obj.get("dateTime")

                organizer = event.get("organizer") or {}
                organizer_email = (organizer.get("emailAddress") or {}).get("address")

                attendees = []
                for a in event.get("attendees", []):
                    email_addr = (a.get("emailAddress") or {}).get("address")
                    if email_addr:
                        attendees.append(email_addr)

                location = (event.get("location") or {}).get("displayName")

                body_text = None
                if event.get("body"):
                    body_text = event["body"].get("content")

                cur.execute(
                    """
                    INSERT INTO office365.calendar_events
                    (microsoft_event_id, organizer_email, subject, starts_at, ends_at, attendees, location, body_text, raw_metadata, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (microsoft_event_id)
                    DO UPDATE SET
                      organizer_email = EXCLUDED.organizer_email,
                      subject = EXCLUDED.subject,
                      starts_at = EXCLUDED.starts_at,
                      ends_at = EXCLUDED.ends_at,
                      attendees = EXCLUDED.attendees,
                      location = EXCLUDED.location,
                      body_text = EXCLUDED.body_text,
                      raw_metadata = EXCLUDED.raw_metadata,
                      updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted;
                    """,
                    (
                        microsoft_event_id,
                        organizer_email,
                        subject,
                        starts_at,
                        ends_at,
                        json.dumps(attendees),
                        location,
                        body_text,
                        json.dumps(event),
                    ),
                )

                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    return {
        "ok": True,
        "fetched": len(events),
        "inserted": inserted,
        "updated": updated,
    }


@app.post("/sync/contacts/{user_id}")
def sync_contacts(user_id: str):
    contacts = fetch_graph_contacts(user_id)

    inserted = 0
    updated = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for contact in contacts:
                microsoft_contact_id = contact.get("id")
                display_name = contact.get("displayName")
                company_name = contact.get("companyName")
                owner_email = user_id

                email_addresses = []
                for e in contact.get("emailAddresses", []):
                    addr = e.get("address")
                    if addr:
                        email_addresses.append(addr)

                phones = []
                for p in contact.get("businessPhones", []):
                    if p:
                        phones.append(p)

                if contact.get("mobilePhone"):
                    phones.append(contact.get("mobilePhone"))

                cur.execute(
                    """
                    INSERT INTO office365.contacts
                    (microsoft_contact_id, owner_email, display_name, email_addresses, phones, company_name, raw_metadata, created_at, updated_at)
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, NOW(), NOW())
                    ON CONFLICT (microsoft_contact_id)
                    DO UPDATE SET
                      owner_email = EXCLUDED.owner_email,
                      display_name = EXCLUDED.display_name,
                      email_addresses = EXCLUDED.email_addresses,
                      phones = EXCLUDED.phones,
                      company_name = EXCLUDED.company_name,
                      raw_metadata = EXCLUDED.raw_metadata,
                      updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted;
                    """,
                    (
                        microsoft_contact_id,
                        owner_email,
                        display_name,
                        json.dumps(email_addresses),
                        json.dumps(phones),
                        company_name,
                        json.dumps(contact),
                    ),
                )

                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    return {
        "ok": True,
        "fetched": len(contacts),
        "inserted": inserted,
        "updated": updated,
    }


@app.post("/sync/sharepoint/{site_id}/{drive_id}")
def sync_sharepoint_drive(site_id: str, drive_id: str):
    items = collect_all_drive_items(drive_id)

    inserted = 0
    updated = 0
    extracted = 0
    chunked = 0
    total_chunks = 0

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for item in items:
                microsoft_file_id = item.get("id")
                name = item.get("name")
                web_url = item.get("webUrl")
                last_modified_at = item.get("lastModifiedDateTime")
                raw_metadata = item

                folder_facet = item.get("folder")
                file_facet = item.get("file")

                if folder_facet:
                    mime_type = "folder"
                elif file_facet:
                    mime_type = file_facet.get("mimeType") if isinstance(file_facet, dict) else None
                else:
                    mime_type = None

                extracted_text = extract_text_for_item(item)
                if extracted_text:
                    extracted += 1

                cur.execute(
                    """
                    INSERT INTO office365.files
                    (microsoft_file_id, owner_microsoft_user_id, source_type, name, web_url, mime_type, last_modified_at, raw_metadata, extracted_text, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW())
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
                      updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted;
                    """,
                    (
                        microsoft_file_id,
                        site_id,
                        "sharepoint",
                        name,
                        web_url,
                        mime_type,
                        last_modified_at,
                        json.dumps(raw_metadata),
                        extracted_text,
                    ),
                )

                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

                if extracted_text:
                    inserted_chunks = upsert_file_chunks(cur, microsoft_file_id, extracted_text)
                    if inserted_chunks > 0:
                        chunked += 1
                        total_chunks += inserted_chunks

        conn.commit()

    return {
        "ok": True,
        "site_id": site_id,
        "drive_id": drive_id,
        "fetched": len(items),
        "inserted": inserted,
        "updated": updated,
        "extracted": extracted,
        "chunked_files": chunked,
        "total_chunks": total_chunks,
    }


@app.post("/chat")
def chat(req: ChatRequest):
    results = retrieve_chunks(req.message, 5)

    if results:
        return {
            "mode": "retrieval-first",
            "message": req.message,
            "terms": normalize_query_terms(req.message),
            "matches": len(results),
            "results": results,
        }

    return {
        "mode": "stub",
        "message": req.message,
        "terms": normalize_query_terms(req.message),
        "note": "No retrieval hits yet. Next step is embeddings or AI fallback."
    }
