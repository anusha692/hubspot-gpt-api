"""
Gong Query API for Custom GPT
==============================

A unified API that allows a Custom GPT to query:
- Gong call recordings (transcripts, stats, trackers, participants)
- Semantic search across Gong transcripts via MongoDB Atlas vector search

For production:
    gunicorn hubspot_gpt_api:app --bind 0.0.0.0:$PORT
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os
import json as json_module
from datetime import datetime, timedelta
import base64

from openai import OpenAI
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Gong credentials
GONG_BASE_URL = os.environ.get("GONG_BASE_URL", "https://us-22394.api.gong.io")
GONG_API_KEY = os.environ.get("GONG_API_KEY")
GONG_API_SECRET = os.environ.get("GONG_API_SECRET")

# MongoDB Atlas
MONGODB_URI = os.environ.get("MONGODB_URI")

# OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Validate required environment variables
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
# GONG API ENDPOINTS
# ============================================================================

@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Gong GPT Query API",
        "version": "3.0.0",
        "integrations": {
            "gong": "connected"
        }
    })


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
    # Fetch more candidates if date filtering will be applied post-search
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    fetch_limit = limit * 3 if (from_date or to_date) else limit

    vector_search_stage = {
        "$vectorSearch": {
            "index": "vector_index_1",
            "path": "embedding",
            "queryVector": query_embedding,
            "numCandidates": fetch_limit * 10,
            "limit": fetch_limit,
        }
    }

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

    # Apply date filter after vector search (not inside $vectorSearch)
    if from_date or to_date:
        date_match = {}
        if from_date:
            date_match["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_match["$lte"] = datetime.fromisoformat(to_date + "T23:59:59")
        pipeline.append({"$match": {"call_date": date_match}})

    pipeline.append({"$limit": limit})

    mongo_client, collection = get_mongo_collection()

    try:
        results = list(collection.aggregate(pipeline))
    except Exception as e:
        mongo_client.close()
        return jsonify({"error": f"Vector search failed: {str(e)}"}), 500

    # Format results
    matching_calls = []
    seen_call_ids = set()

    for r in results:
        r["_id"] = str(r["_id"])
        if r.get("call_date"):
            r["call_date"] = r["call_date"].isoformat()
        matching_calls.append(r)
        seen_call_ids.add(r.get("call_id"))

    mongo_client.close()

    return jsonify({
        "query": query,
        "matching_chunks": len(matching_calls),
        "unique_calls": len(seen_call_ids),
        "results": matching_calls,
    })


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
# MCP SERVER (for Claude.ai integration)
# ============================================================================

MCP_TOOLS = {
    "search_gong_calls": {
        "description": "Search Gong calls by date range. Returns call IDs, titles, dates, and recording URLs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "from_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Defaults to 7 days ago."},
                "to_date": {"type": "string", "description": "End date (YYYY-MM-DD). Defaults to today."},
                "limit": {"type": "integer", "description": "Max results. Defaults to 20."},
            },
        },
    },
    "get_call_transcript": {
        "description": "Get the full transcript of a Gong call, broken down by speaker.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "call_id": {"type": "string", "description": "The Gong call ID"},
            },
            "required": ["call_id"],
        },
    },
    "search_transcripts": {
        "description": "Semantic search across all Gong transcripts from the last 3 months. Use this to find calls mentioning specific topics, stories, or patterns.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language search query (e.g. 'Derek story', 'pricing objection')"},
                "from_date": {"type": "string", "description": "Optional start date filter (YYYY-MM-DD)"},
                "to_date": {"type": "string", "description": "Optional end date filter (YYYY-MM-DD)"},
                "limit": {"type": "integer", "description": "Max results. Defaults to 20."},
            },
            "required": ["query"],
        },
    },
}


def mcp_call_tool(name, arguments):
    """Execute an MCP tool by calling the corresponding Flask endpoint internally."""
    with app.test_client() as client:
        if name == "search_gong_calls":
            resp = client.post("/gong/calls/search", json=arguments)
        elif name == "get_call_transcript":
            resp = client.get(f"/gong/calls/{arguments['call_id']}/transcript")
        elif name == "search_transcripts":
            resp = client.post("/gong/search", json=arguments)
        else:
            return {"error": f"Unknown tool: {name}"}
        return resp.get_json()


# ============================================================================
# MCP STREAMABLE HTTP ENDPOINT
# ============================================================================


@app.route("/mcp", methods=["POST"])
def handle_mcp():
    """
    MCP Streamable HTTP endpoint for Claude.ai integration.
    Handles JSON-RPC requests: initialize, tools/list, tools/call.
    """
    body = request.json
    method = body.get("method")
    req_id = body.get("id")
    params = body.get("params", {})

    if method == "initialize":
        return jsonify({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "sales-intelligence", "version": "1.0.0"},
            },
        })

    if method == "notifications/initialized":
        return jsonify({"jsonrpc": "2.0", "id": req_id, "result": {}})

    if method == "tools/list":
        tool_list = [
            {"name": name, "description": t["description"], "inputSchema": t["inputSchema"]}
            for name, t in MCP_TOOLS.items()
        ]
        return jsonify({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": tool_list},
        })

    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if tool_name not in MCP_TOOLS:
            return jsonify({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
            })

        try:
            result = mcp_call_tool(tool_name, arguments)
            return jsonify({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json_module.dumps(result, default=str)}],
                },
            })
        except Exception as e:
            return jsonify({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json_module.dumps({"error": str(e)})}],
                    "isError": True,
                },
            })

    return jsonify({
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"},
    })


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║       Gong GPT Query API                                     ║
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
║  MCP:                                                        ║
║    POST /mcp                         - MCP endpoint          ║
╚══════════════════════════════════════════════════════════════╝

Running on http://localhost:{port}
    """)

    app.run(host="0.0.0.0", port=port, debug=debug)
