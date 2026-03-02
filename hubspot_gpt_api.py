"""
HubSpot + Gong Query API for Custom GPT
========================================

A unified API that allows a Custom GPT to query:
- Gong call recordings (transcripts, stats, trackers, participants)
- HubSpot CRM (contacts, deals, with Gong intelligence)

For production:
    gunicorn hubspot_gpt_api:app --bind 0.0.0.0:$PORT
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os
from datetime import datetime, timedelta
import base64

from openai import OpenAI
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

# ============================================================================
# CONFIGURATION
# ============================================================================

# HubSpot credentials
HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Gong credentials
GONG_BASE_URL = os.environ.get("GONG_BASE_URL", "https://us-22394.api.gong.io")
GONG_API_KEY = os.environ.get("GONG_API_KEY")
GONG_API_SECRET = os.environ.get("GONG_API_SECRET")

# MongoDB Atlas
MONGODB_URI = os.environ.get("MONGODB_URI")

# OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Validate required environment variables
if not HUBSPOT_ACCESS_TOKEN:
    raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable is required")
if not GONG_API_KEY or not GONG_API_SECRET:
    raise ValueError("GONG_API_KEY and GONG_API_SECRET environment variables are required")

# ============================================================================
# GONG CLIENT
# ============================================================================

def gong_request(method, endpoint, json_data=None, params=None):
    """Make a request to Gong API with Basic Auth"""
    # Gong uses Basic Auth with API key as username and secret as password
    auth_string = f"{GONG_API_KEY}:{GONG_API_SECRET}"
    auth_bytes = auth_string.encode('ascii')
    base64_bytes = base64.b64encode(auth_bytes)
    base64_auth = base64_bytes.decode('ascii')
    
    headers = {
        "Authorization": f"Basic {base64_auth}",
        "Content-Type": "application/json"
    }
    
    url = f"{GONG_BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=json_data)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Gong API Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Gong API Exception: {str(e)}")
        return None


# ============================================================================
# HUBSPOT CLIENT
# ============================================================================

def hubspot_request(method, endpoint, json_data=None):
    """Make a request to HubSpot API"""
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"{HUBSPOT_BASE_URL}{endpoint}"
    
    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers, json=json_data)
    
    return response.json() if response.status_code == 200 else None


def search_contacts(query: str, search_by: str = "email") -> list:
    """Search HubSpot contacts by email or company"""
    properties = [
        "email", "firstname", "lastname", "company", "jobtitle",
        "sector", "industry",
        "latest_call_date", "latest_call_duration", "latest_call_gong_url",
        "latest_call_summary", "gong_total_calls", "gong_buying_signals",
        "gong_competitor_mentioned", "gong_topics_discussed", "gong_next_steps",
        "pain_points_mentioned", "gong_last_sentiment",
        "outbound_platform", "latest_outbound_campaign", "latest_outbound_date",
        "has_responded", "reply_sentiment", "latest_response_text",
        "has_visited_website"
    ]
    
    if search_by == "email":
        filters = [{"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": query}]
    elif search_by == "company":
        filters = [{"propertyName": "company", "operator": "CONTAINS_TOKEN", "value": query}]
    else:
        filters = [{"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": query}]
    
    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": properties,
        "limit": 10
    }
    
    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)
    
    if result and "results" in result:
        return result["results"]
    return []


def format_contact_for_gpt(contact: dict) -> dict:
    """Format a HubSpot contact for GPT-friendly response"""
    props = contact.get("properties", {})
    
    return {
        "contact": {
            "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or "Unknown",
            "email": props.get("email", ""),
            "company": props.get("company", "Unknown"),
            "title": props.get("jobtitle", ""),
            "sector": props.get("sector") or props.get("industry", "Unknown")
        },
        "gong_calls": {
            "total_calls": props.get("gong_total_calls", "0"),
            "last_call_date": props.get("latest_call_date", "No calls recorded"),
            "last_call_duration_minutes": props.get("latest_call_duration", ""),
            "recording_url": props.get("latest_call_gong_url", ""),
            "call_summary": props.get("latest_call_summary", "No summary available"),
            "buying_signals": props.get("gong_buying_signals", "None detected"),
            "competitors_mentioned": props.get("gong_competitor_mentioned", "None mentioned"),
            "pain_points": props.get("pain_points_mentioned", "None identified"),
            "topics_discussed": props.get("gong_topics_discussed", ""),
            "next_steps": props.get("gong_next_steps", ""),
            "call_sentiment": props.get("gong_last_sentiment", "")
        },
        "outbound_activity": {
            "platform": props.get("outbound_platform", "None"),
            "campaign": props.get("latest_outbound_campaign", ""),
            "last_outreach_date": props.get("latest_outbound_date", ""),
            "has_replied": props.get("has_responded", "false"),
            "reply_sentiment": props.get("reply_sentiment", ""),
            "reply_text": props.get("latest_response_text", "")
        },
        "engagement": {
            "visited_website": props.get("has_visited_website", "false")
        }
    }


# ============================================================================
# GONG API ENDPOINTS
# ============================================================================

@app.route("/gong/calls/search", methods=["POST"])
def search_gong_calls():
    """
    Search Gong calls by date range
    
    Request body:
    {
        "from_date": "2024-01-01",  // Optional, defaults to 7 days ago
        "to_date": "2024-01-31",    // Optional, defaults to today
        "limit": 20  // Optional, defaults to 20
    }
    """
    data = request.json or {}
    
    # Default to last 7 days if no dates provided
    to_date = data.get("to_date") or datetime.now().strftime("%Y-%m-%d")
    from_date = data.get("from_date") or (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Use query parameters for GET request
    params = {
        "fromDateTime": f"{from_date}T00:00:00Z",
        "toDateTime": f"{to_date}T23:59:59Z"
    }
    
    result = gong_request("GET", "/v2/calls", params=params)
    
    if not result or "calls" not in result:
        return jsonify({"message": "No calls found", "results": []})
    
    # Format calls for GPT
    calls = []
    for call in result.get("calls", [])[:data.get("limit", 20)]:
        calls.append({
            "call_id": call.get("id"),
            "title": call.get("title", "Untitled Call"),
            "started": call.get("started"),
            "duration_seconds": call.get("duration"),
            "url": call.get("url"),
            "participants": [p.get("emailAddress") for p in call.get("parties", [])],
            "direction": call.get("direction"),
            "system": call.get("system")
        })
    
    return jsonify({
        "message": f"Found {len(calls)} calls from {from_date} to {to_date}",
        "results": calls
    })


@app.route("/gong/calls/<call_id>/transcript", methods=["GET"])
def get_call_transcript(call_id: str):
    """Get the full transcript of a Gong call"""
    # Gong requires date range + call IDs
    payload = {
        "filter": {
            "fromDateTime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z"),
            "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
            "callIds": [call_id]
        }
    }
    
    result = gong_request("POST", "/v2/calls/transcript", json_data=payload)
    
    if not result or not result.get("callTranscripts"):
        return jsonify({"error": "Call transcript not found"}), 404
    
    # Get the first (and should be only) transcript
    call_transcript_data = result.get("callTranscripts", [])[0]
    transcript_segments = call_transcript_data.get("transcript", [])
    
    formatted_transcript = []

    for segment in transcript_segments:
        # Each segment is a monologue with a sentences array containing the actual text
        sentences = segment.get("sentences", [])
        full_text = " ".join(s.get("text", "") for s in sentences)
        start_time = sentences[0].get("start") if sentences else None
        end_time = sentences[-1].get("end") if sentences else None

        formatted_transcript.append({
            "speaker_id": segment.get("speakerId"),
            "topic": segment.get("topic", ""),
            "text": full_text,
            "start_time": start_time,
            "end_time": end_time
        })
    
    return jsonify({
        "call_id": call_id,
        "transcript": formatted_transcript
    })
    
@app.route("/gong/calls/<call_id>/stats", methods=["GET"])
def get_call_stats(call_id: str):
    """Get call statistics including talk ratio and trackers"""
    # First get basic call info
    params = {
        "fromDateTime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z"),
        "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z")
    }
    
    result = gong_request("GET", "/v2/calls", params=params)
    
    if not result or "calls" not in result:
        return jsonify({"error": "Call not found"}), 404
    
    # Find the specific call
    call_data = None
    for call in result.get("calls", []):
        if call.get("id") == call_id:
            call_data = call
            break
    
    if not call_data:
        return jsonify({"error": "Call not found"}), 404
    
    return jsonify({
        "call_id": call_id,
        "title": call_data.get("title"),
        "url": call_data.get("url"),
        "duration_seconds": call_data.get("duration"),
        "started": call_data.get("started"),
        "direction": call_data.get("direction"),
        "participants": [p.get("emailAddress") for p in call_data.get("parties", [])]
    })


@app.route("/gong/contacts/<email>/calls", methods=["GET"])
def get_contact_calls(email: str):
    """Get all Gong calls for a specific contact email"""
    # Search calls in the last 90 days
    params = {
        "fromDateTime": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z"),
        "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z")
    }
    
    result = gong_request("GET", "/v2/calls", params=params)
    
    if not result or "calls" not in result:
        return jsonify({"message": f"No calls found for {email}", "results": []})
    
    # Filter calls where this email is a participant
    calls = []
    for call in result.get("calls", []):
        participants = [p.get("emailAddress") for p in call.get("parties", [])]
        if email.lower() in [p.lower() for p in participants if p]:
            calls.append({
                "call_id": call.get("id"),
                "title": call.get("title"),
                "date": call.get("started"),
                "duration_seconds": call.get("duration"),
                "url": call.get("url")
            })
    
    return jsonify({
        "email": email,
        "total_calls": len(calls),
        "calls": calls
    })


# ============================================================================
# HUBSPOT API ENDPOINTS
# ============================================================================

@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "HubSpot + Gong GPT Query API",
        "version": "2.0.0",
        "integrations": {
            "hubspot": "connected",
            "gong": "connected"
        }
    })


@app.route("/hubspot/search", methods=["POST"])
def search_hubspot():
    """Search HubSpot contacts"""
    data = request.json or {}
    query = data.get("query", "").strip()
    
    if not query:
        return jsonify({"error": "Please provide a search query"}), 400
    
    search_by = data.get("search_by")
    if not search_by:
        search_by = "email" if "@" in query else "company"
    
    contacts = search_contacts(query, search_by)
    
    if not contacts:
        return jsonify({
            "message": f"No contacts found matching '{query}'",
            "results": []
        })
    
    results = [format_contact_for_gpt(c) for c in contacts]
    
    return jsonify({
        "message": f"Found {len(results)} contact(s)",
        "results": results
    })


@app.route("/hubspot/contact/<email>", methods=["GET"])
def get_hubspot_contact(email: str):
    """Get a specific HubSpot contact by email"""
    contacts = search_contacts(email, "email")
    
    if not contacts:
        return jsonify({"error": f"Contact not found: {email}"}), 404
    
    return jsonify(format_contact_for_gpt(contacts[0]))


# ============================================================================
# GONG VECTOR SEARCH ENDPOINTS
# ============================================================================


def get_mongo_collection():
    """Get the MongoDB collection for transcript chunks."""
    client = MongoClient(MONGODB_URI)
    db = client["gong"]
    return client, db["gong_transcripts"]


def embed_query(text):
    """Embed a query string using OpenAI."""
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.embeddings.create(model="text-embedding-3-small", input=text)
    return response.data[0].embedding


@app.route("/gong/search", methods=["POST"])
def gong_vector_search():
    """
    Semantic search across Gong transcripts using MongoDB Atlas vector search.

    Request body:
    {
        "query": "Derek story",
        "from_date": "2025-12-01",   // Optional
        "to_date": "2026-03-01",     // Optional
        "limit": 20                  // Optional, defaults to 20
    }
    """
    data = request.json or {}
    query = data.get("query", "").strip()

    if not query:
        return jsonify({"error": "Please provide a search query"}), 400

    if not MONGODB_URI or not OPENAI_API_KEY:
        return jsonify({"error": "MONGODB_URI and OPENAI_API_KEY must be configured"}), 500

    limit = data.get("limit", 20)

    # Embed the query
    query_embedding = embed_query(query)

    # Build vector search pipeline
    vector_search_stage = {
        "$vectorSearch": {
            "index": "vector_index",
            "path": "embedding",
            "queryVector": query_embedding,
            "numCandidates": limit * 10,
            "limit": limit,
        }
    }

    # Add date filter if provided
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    if from_date or to_date:
        date_filter = {}
        if from_date:
            date_filter["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_filter["$lte"] = datetime.fromisoformat(to_date + "T23:59:59")
        vector_search_stage["$vectorSearch"]["filter"] = {"call_date": date_filter}

    pipeline = [
        vector_search_stage,
        {
            "$project": {
                "call_id": 1,
                "call_title": 1,
                "call_date": 1,
                "call_url": 1,
                "participants": 1,
                "speaker_id": 1,
                "topic": 1,
                "text": 1,
                "start_time": 1,
                "end_time": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]

    mongo_client, collection = get_mongo_collection()

    try:
        results = list(collection.aggregate(pipeline))
    except Exception as e:
        mongo_client.close()
        return jsonify({"error": f"Vector search failed: {str(e)}"}), 500

    # Format results
    matching_calls = []
    seen_call_ids = set()
    all_participants = set()

    for r in results:
        r["_id"] = str(r["_id"])
        if r.get("call_date"):
            r["call_date"] = r["call_date"].isoformat()
        matching_calls.append(r)
        seen_call_ids.add(r.get("call_id"))
        for p in r.get("participants", []):
            all_participants.add(p)

    # Cross-reference with HubSpot deals for correlation
    deal_correlation = None
    if all_participants:
        deal_correlation = compute_deal_correlation(
            all_participants, from_date, to_date
        )

    mongo_client.close()

    response = {
        "query": query,
        "matching_chunks": len(matching_calls),
        "unique_calls": len(seen_call_ids),
        "results": matching_calls,
    }
    if deal_correlation:
        response["deal_correlation"] = deal_correlation

    return jsonify(response)


def compute_deal_correlation(participant_emails, from_date, to_date):
    """
    For the given participant emails, find associated HubSpot deals
    and compute win-rate correlation.
    """
    closed_won = 0
    closed_lost = 0
    deal_ids_seen = set()

    for email in participant_emails:
        # Find contact by email
        contacts = search_contacts(email, "email")
        if not contacts:
            continue

        contact_id = contacts[0].get("id")
        if not contact_id:
            continue

        # Get associated deals
        assoc_result = hubspot_request(
            "GET", f"/crm/v4/objects/contacts/{contact_id}/associations/deals"
        )
        if not assoc_result or "results" not in assoc_result:
            continue

        for assoc in assoc_result["results"]:
            deal_id = assoc.get("toObjectId")
            if not deal_id or deal_id in deal_ids_seen:
                continue
            deal_ids_seen.add(deal_id)

            # Get deal properties
            deal_result = hubspot_request(
                "GET",
                f"/crm/v3/objects/deals/{deal_id}?properties=dealstage,dealname,amount,closedate",
            )
            if not deal_result:
                continue

            stage = deal_result.get("properties", {}).get("dealstage", "")
            if stage == "closedwon":
                closed_won += 1
            elif stage == "closedlost":
                closed_lost += 1

    total_with_outcome = closed_won + closed_lost
    if total_with_outcome == 0:
        return None

    # Get baseline: total deals in the same period
    baseline = get_baseline_deal_stats(from_date, to_date)

    correlation = {
        "calls_with_match": len(deal_ids_seen),
        "closed_won": closed_won,
        "closed_lost": closed_lost,
        "match_win_rate": f"{round(closed_won / total_with_outcome * 100)}%",
    }

    if baseline:
        correlation["total_deals_in_period"] = baseline["total"]
        correlation["baseline_win_rate"] = baseline["win_rate"]

    return correlation


def get_baseline_deal_stats(from_date, to_date):
    """Get overall deal win rate for a date range as baseline comparison."""
    filters = []
    if from_date:
        filters.append(
            {
                "propertyName": "closedate",
                "operator": "GTE",
                "value": datetime.fromisoformat(from_date).strftime("%s000"),
            }
        )
    if to_date:
        filters.append(
            {
                "propertyName": "closedate",
                "operator": "LTE",
                "value": datetime.fromisoformat(to_date + "T23:59:59").strftime("%s000"),
            }
        )

    # Only count closed deals
    filters.append(
        {
            "propertyName": "dealstage",
            "operator": "IN",
            "values": ["closedwon", "closedlost"],
        }
    )

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": ["dealstage"],
        "limit": 100,
    }

    result = hubspot_request("POST", "/crm/v3/objects/deals/search", payload)
    if not result or "results" not in result:
        return None

    deals = result["results"]
    total = len(deals)
    won = sum(
        1 for d in deals if d.get("properties", {}).get("dealstage") == "closedwon"
    )

    if total == 0:
        return None

    return {
        "total": total,
        "won": won,
        "lost": total - won,
        "win_rate": f"{round(won / total * 100)}%",
    }


@app.route("/gong/ingest", methods=["POST"])
def trigger_gong_ingest():
    """
    Trigger Gong transcript ingestion into MongoDB Atlas.

    Request body:
    {
        "days_back": 90  // Optional, defaults to 90
    }
    """
    from gong_ingest import ingest_calls

    data = request.json or {}
    days_back = data.get("days_back", 90)

    try:
        count = ingest_calls(days_back=days_back)
        return jsonify({"message": f"Ingested {count} new calls", "calls_ingested": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/gong/webhook", methods=["POST"])
def gong_webhook():
    """
    Webhook endpoint for Gong to notify when a call is analyzed.

    Gong sends a POST with the call ID when transcription is complete.
    This triggers automatic ingestion of the new call into MongoDB.

    Register this URL in Gong: Settings > API > Webhooks
    Event type: CALL_ANALYZED
    """
    from gong_ingest import ingest_single_call

    data = request.json or {}

    # Handle Gong webhook verification (ping)
    if data.get("type") == "WEBHOOK_VALIDATION":
        return jsonify({"status": "ok"})

    call_id = data.get("callId") or data.get("data", {}).get("callId")
    if not call_id:
        return jsonify({"error": "No callId in webhook payload"}), 400

    try:
        ingested = ingest_single_call(call_id)
        if ingested:
            return jsonify({"message": f"Call {call_id} ingested successfully"})
        else:
            return jsonify({"message": f"Call {call_id} skipped (already ingested or no transcript)"})
    except Exception as e:
        print(f"Webhook ingestion error for call {call_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# UNIFIED ENDPOINTS (HubSpot + Gong)
# ============================================================================

@app.route("/contact/<email>/full", methods=["GET"])
def get_full_contact_profile(email: str):
    """
    Get complete contact profile: HubSpot CRM data + all Gong calls
    """
    # Get HubSpot data
    hubspot_contacts = search_contacts(email, "email")
    hubspot_data = format_contact_for_gpt(hubspot_contacts[0]) if hubspot_contacts else {}
    
    # Get Gong calls
    params = {
        "fromDateTime": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z"),
        "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z")
    }
    
    gong_result = gong_request("GET", "/v2/calls", params=params)
    gong_calls = []
    
    if gong_result and "calls" in gong_result:
        for call in gong_result.get("calls", []):
            participants = [p.get("emailAddress") for p in call.get("parties", [])]
            if email.lower() in [p.lower() for p in participants if p]:
                gong_calls.append({
                    "call_id": call.get("id"),
                    "title": call.get("title"),
                    "date": call.get("started"),
                    "duration_seconds": call.get("duration"),
                    "url": call.get("url")
                })
    
    return jsonify({
        "email": email,
        "hubspot_data": hubspot_data,
        "gong_calls": {
            "total_calls": len(gong_calls),
            "recent_calls": gong_calls
        }
    })


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║       HubSpot + Gong GPT Query API                           ║
╠══════════════════════════════════════════════════════════════╣
║  Gong Endpoints:                                             ║
║    POST /gong/calls/search           - Search calls          ║
║    GET  /gong/calls/<id>/transcript  - Get transcript        ║
║    GET  /gong/calls/<id>/stats       - Get stats             ║
║    GET  /gong/contacts/<email>/calls - Contact's calls       ║
║    POST /gong/search                 - Vector search         ║
║    POST /gong/ingest                 - Trigger ingestion     ║
║    POST /gong/webhook                - Gong webhook          ║
║                                                               ║
║  HubSpot Endpoints:                                          ║
║    POST /hubspot/search              - Search contacts       ║
║    GET  /hubspot/contact/<email>     - Get contact           ║
║                                                               ║
║  Unified:                                                    ║
║    GET  /contact/<email>/full        - Full profile          ║
╚══════════════════════════════════════════════════════════════╝
    
Running on http://localhost:{port}
    """)
    
    app.run(host="0.0.0.0", port=port, debug=debug)