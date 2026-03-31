CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS office365;

CREATE TABLE IF NOT EXISTS office365.users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  microsoft_user_id TEXT NOT NULL UNIQUE,
  email TEXT,
  display_name TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  microsoft_file_id TEXT NOT NULL UNIQUE,
  owner_microsoft_user_id TEXT,
  source_type TEXT NOT NULL,
  name TEXT NOT NULL,
  web_url TEXT,
  mime_type TEXT,
  last_modified_at TIMESTAMPTZ,
  raw_metadata JSONB,
  extracted_text TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.file_chunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  file_id UUID NOT NULL REFERENCES office365.files(id) ON DELETE CASCADE,
  chunk_index INT NOT NULL,
  content TEXT NOT NULL,
  token_count INT,
  embedding vector(1536),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.emails (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  microsoft_message_id TEXT NOT NULL UNIQUE,
  user_email TEXT,
  subject TEXT,
  sender TEXT,
  recipients JSONB,
  sent_at TIMESTAMPTZ,
  body_text TEXT,
  raw_metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.calendar_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  microsoft_event_id TEXT NOT NULL UNIQUE,
  organizer_email TEXT,
  subject TEXT,
  starts_at TIMESTAMPTZ,
  ends_at TIMESTAMPTZ,
  attendees JSONB,
  location TEXT,
  body_text TEXT,
  raw_metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  microsoft_contact_id TEXT NOT NULL UNIQUE,
  owner_email TEXT,
  display_name TEXT,
  email_addresses JSONB,
  phones JSONB,
  company_name TEXT,
  raw_metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office365.sync_state (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_type TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  last_delta_token TEXT,
  last_full_sync_at TIMESTAMPTZ,
  last_incremental_sync_at TIMESTAMPTZ,
  status TEXT,
  UNIQUE(source_type, scope_key)
);
