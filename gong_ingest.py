"""
Gong Transcript Ingestion for MongoDB Atlas Vector Search
==========================================================

Fetches Gong call transcripts, chunks by monologue segment,
embeds with OpenAI text-embedding-3-small, and stores in MongoDB Atlas.

Usage:
    python gong_ingest.py --days 90
"""

import argparse
import base64
import os
import sys
from datetime import datetime, timedelta

import requests
from openai import OpenAI
from pymongo import MongoClient

# ============================================================================
# CONFIGURATION
# ============================================================================

GONG_BASE_URL = os.environ.get("GONG_BASE_URL", "https://us-22394.api.gong.io")
GONG_API_KEY = os.environ.get("GONG_API_KEY")
GONG_API_SECRET = os.environ.get("GONG_API_SECRET")
MONGODB_URI = os.environ.get("MONGODB_URI")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536

# ============================================================================
# CLIENTS
# ============================================================================


def gong_request(method, endpoint, json_data=None, params=None):
    """Make a request to Gong API with Basic Auth."""
    auth_string = f"{GONG_API_KEY}:{GONG_API_SECRET}"
    base64_auth = base64.b64encode(auth_string.encode("ascii")).decode("ascii")

    headers = {
        "Authorization": f"Basic {base64_auth}",
        "Content-Type": "application/json",
    }

    url = f"{GONG_BASE_URL}{endpoint}"

    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=json_data)
        else:
            return None

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Gong API Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Gong API Exception: {e}")
        return None


def get_mongo_collection():
    """Get the MongoDB collection for transcript chunks."""
    client = MongoClient(MONGODB_URI)
    db = client["gong"]
    return client, db["gong_transcripts"]


def get_openai_client():
    """Get the OpenAI client."""
    return OpenAI(api_key=OPENAI_API_KEY)


# ============================================================================
# INGESTION LOGIC
# ============================================================================


def fetch_calls(from_date, to_date):
    """Fetch all calls from Gong for the given date range."""
    params = {
        "fromDateTime": f"{from_date}T00:00:00Z",
        "toDateTime": f"{to_date}T23:59:59Z",
    }

    result = gong_request("GET", "/v2/calls", params=params)
    if not result or "calls" not in result:
        return []
    return result["calls"]


def fetch_transcript(call_id):
    """Fetch the full transcript for a single call."""
    payload = {
        "filter": {
            "fromDateTime": (datetime.now() - timedelta(days=365)).strftime(
                "%Y-%m-%dT00:00:00Z"
            ),
            "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
            "callIds": [call_id],
        }
    }

    result = gong_request("POST", "/v2/calls/transcript", json_data=payload)
    if not result or not result.get("callTranscripts"):
        return []

    call_transcript_data = result["callTranscripts"][0]
    return call_transcript_data.get("transcript", [])


def embed_texts(openai_client, texts):
    """Embed a batch of texts using OpenAI."""
    response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]


def ingest_calls(days_back=90):
    """Main ingestion function. Returns count of calls ingested."""
    # Validate env vars
    missing = []
    if not GONG_API_KEY:
        missing.append("GONG_API_KEY")
    if not GONG_API_SECRET:
        missing.append("GONG_API_SECRET")
    if not MONGODB_URI:
        missing.append("MONGODB_URI")
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    mongo_client, collection = get_mongo_collection()
    openai_client = get_openai_client()

    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"Fetching Gong calls from {from_date} to {to_date}...")
    calls = fetch_calls(from_date, to_date)
    print(f"Found {len(calls)} calls")

    # Get already-ingested call IDs
    existing_call_ids = set(collection.distinct("call_id"))
    print(f"Already ingested: {len(existing_call_ids)} calls")

    ingested_count = 0

    for i, call in enumerate(calls):
        call_id = call.get("id")
        if call_id in existing_call_ids:
            continue

        call_title = call.get("title", "Untitled Call")
        call_date = call.get("started")
        call_url = call.get("url", "")
        participants = [
            p.get("emailAddress", "") for p in call.get("parties", []) if p.get("emailAddress")
        ]

        print(f"  [{i + 1}/{len(calls)}] Processing: {call_title} ({call_id})")

        # Fetch transcript
        segments = fetch_transcript(call_id)
        if not segments:
            print(f"    No transcript found, skipping")
            continue

        # Build chunk texts and metadata
        chunks = []
        chunk_texts = []

        for segment in segments:
            sentences = segment.get("sentences", [])
            full_text = " ".join(s.get("text", "") for s in sentences)
            if not full_text.strip():
                continue

            start_time = sentences[0].get("start") if sentences else None
            end_time = sentences[-1].get("end") if sentences else None

            chunks.append(
                {
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": datetime.fromisoformat(call_date.replace("Z", "+00:00"))
                    if call_date
                    else None,
                    "call_url": call_url,
                    "participants": participants,
                    "speaker_id": segment.get("speakerId"),
                    "topic": segment.get("topic", ""),
                    "text": full_text,
                    "start_time": start_time,
                    "end_time": end_time,
                    "ingested_at": datetime.utcnow(),
                }
            )
            chunk_texts.append(full_text)

        if not chunk_texts:
            print(f"    No text segments, skipping")
            continue

        # Embed in batches of 100
        all_embeddings = []
        for batch_start in range(0, len(chunk_texts), 100):
            batch = chunk_texts[batch_start : batch_start + 100]
            embeddings = embed_texts(openai_client, batch)
            all_embeddings.extend(embeddings)

        # Attach embeddings to chunks
        for chunk, embedding in zip(chunks, all_embeddings):
            chunk["embedding"] = embedding

        # Insert into MongoDB
        collection.insert_many(chunks)
        ingested_count += 1
        print(f"    Inserted {len(chunks)} segments")

    mongo_client.close()
    print(f"\nDone! Ingested {ingested_count} new calls.")
    return ingested_count


def ingest_single_call(call_id):
    """Ingest a single call by ID. Used by the webhook handler."""
    missing = []
    if not GONG_API_KEY:
        missing.append("GONG_API_KEY")
    if not GONG_API_SECRET:
        missing.append("GONG_API_SECRET")
    if not MONGODB_URI:
        missing.append("MONGODB_URI")
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    mongo_client, collection = get_mongo_collection()
    openai_client = get_openai_client()

    # Check if already ingested
    if collection.find_one({"call_id": call_id}):
        mongo_client.close()
        print(f"Call {call_id} already ingested, skipping")
        return False

    # Fetch call metadata
    params = {
        "fromDateTime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z"),
        "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
    }
    calls_result = gong_request("GET", "/v2/calls", params=params)

    call_data = None
    if calls_result and "calls" in calls_result:
        for call in calls_result["calls"]:
            if call.get("id") == call_id:
                call_data = call
                break

    if not call_data:
        mongo_client.close()
        print(f"Call {call_id} not found in Gong")
        return False

    call_title = call_data.get("title", "Untitled Call")
    call_date = call_data.get("started")
    call_url = call_data.get("url", "")
    participants = [
        p.get("emailAddress", "") for p in call_data.get("parties", []) if p.get("emailAddress")
    ]

    # Fetch transcript
    segments = fetch_transcript(call_id)
    if not segments:
        mongo_client.close()
        print(f"No transcript found for call {call_id}")
        return False

    # Build chunks
    chunks = []
    chunk_texts = []

    for segment in segments:
        sentences = segment.get("sentences", [])
        full_text = " ".join(s.get("text", "") for s in sentences)
        if not full_text.strip():
            continue

        start_time = sentences[0].get("start") if sentences else None
        end_time = sentences[-1].get("end") if sentences else None

        chunks.append(
            {
                "call_id": call_id,
                "call_title": call_title,
                "call_date": datetime.fromisoformat(call_date.replace("Z", "+00:00"))
                if call_date
                else None,
                "call_url": call_url,
                "participants": participants,
                "speaker_id": segment.get("speakerId"),
                "topic": segment.get("topic", ""),
                "text": full_text,
                "start_time": start_time,
                "end_time": end_time,
                "ingested_at": datetime.utcnow(),
            }
        )
        chunk_texts.append(full_text)

    if not chunk_texts:
        mongo_client.close()
        print(f"No text segments for call {call_id}")
        return False

    # Embed
    all_embeddings = []
    for batch_start in range(0, len(chunk_texts), 100):
        batch = chunk_texts[batch_start : batch_start + 100]
        embeddings = embed_texts(openai_client, batch)
        all_embeddings.extend(embeddings)

    for chunk, embedding in zip(chunks, all_embeddings):
        chunk["embedding"] = embedding

    collection.insert_many(chunks)
    mongo_client.close()
    print(f"Ingested call {call_id}: {len(chunks)} segments")
    return True


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Gong transcripts into MongoDB Atlas")
    parser.add_argument(
        "--days", type=int, default=90, help="Number of days back to fetch (default: 90)"
    )
    args = parser.parse_args()

    try:
        ingest_calls(days_back=args.days)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
