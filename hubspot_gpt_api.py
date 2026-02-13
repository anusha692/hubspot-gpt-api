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
    Search Gong calls by date range, participants, or topics
    
    Request body:
    {
        "from_date": "2024-01-01",  // Optional, defaults to 7 days ago
        "to_date": "2024-01-31",    // Optional, defaults to today
        "participant_email": "john@example.com",  // Optional
        "limit": 20  // Optional, defaults to 20
    }
    """
    data = request.json or {}
    
    # Default to last 7 days if no dates provided
    to_date = data.get("to_date") or datetime.now().strftime("%Y-%m-%d")
    from_date = data.get("from_date") or (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    payload = {
        "filter": {
            "fromDateTime": f"{from_date}T00:00:00Z",
            "toDateTime": f"{to_date}T23:59:59Z"
        },
        "contentSelector": {
            "exposedFields": {
                "content": True,
                "structure": True,
                "parties": True,
                "interaction": True
            }
        }
    }
    
    # Add participant filter if provided
    if data.get("participant_email"):
        payload["filter"]["callParticipants"] = [data["participant_email"]]
    
    result = gong_request("POST", "/v2/calls", payload)
    
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
            "participants": [
                {
                    "name": p.get("name"),
                    "email": p.get("emailAddress"),
                    "user_id": p.get("userId")
                }
                for p in call.get("parties", [])
            ],
            "direction": call.get("direction"),  # Inbound/Outbound
            "system": call.get("system"),  # Zoom, Teams, etc.
        })
    
    return jsonify({
        "message": f"Found {len(calls)} calls from {from_date} to {to_date}",
        "results": calls
    })


@app.route("/gong/calls/<call_id>/transcript", methods=["GET"])
def get_call_transcript(call_id: str):
    """Get the full transcript of a Gong call"""
    result = gong_request("GET", f"/v2/calls/{call_id}/transcript")
    
    if not result:
        return jsonify({"error": "Call transcript not found"}), 404
    
    # Format transcript
    transcript = result.get("callTranscript", [])
    formatted_transcript = []
    
    for segment in transcript:
        formatted_transcript.append({
            "speaker": segment.get("speakerName", "Unknown"),
            "speaker_id": segment.get("speakerId"),
            "text": segment.get("text", ""),
            "start_time": segment.get("start"),
            "duration": segment.get("duration")
        })
    
    return jsonify({
        "call_id": call_id,
        "transcript": formatted_transcript
    })


@app.route("/gong/calls/<call_id>/stats", methods=["GET"])
def get_call_stats(call_id: str):
    """Get call statistics including talk ratio and trackers"""
    result = gong_request("GET", f"/v2/calls/{call_id}")
    
    if not result:
        return jsonify({"error": "Call not found"}), 404
    
    call_data = result.get("calls", [{}])[0]
    
    # Extract tracker mentions
    trackers = call_data.get("trackers", [])
    competitors = []
    pain_points = []
    objections = []
    buying_signals = []
    
    for tracker in trackers:
        tracker_name = tracker.get("name", "")
        tracker_type = tracker.get("type", "").lower()
        
        if "competitor" in tracker_type:
            competitors.append(tracker_name)
        elif "pain" in tracker_type:
            pain_points.append(tracker_name)
        elif "objection" in tracker_type:
            objections.append(tracker_name)
        elif "buying" in tracker_type or "signal" in tracker_type:
            buying_signals.append(tracker_name)
    
    # Get talk ratio
    interaction_stats = call_data.get("interaction", {})
    
    return jsonify({
        "call_id": call_id,
        "title": call_data.get("title"),
        "url": call_data.get("url"),
        "duration_seconds": call_data.get("duration"),
        "talk_ratio": {
            "rep_talk_percentage": interaction_stats.get("speakerTalkRatio", {}).get("rep", 0),
            "prospect_talk_percentage": interaction_stats.get("speakerTalkRatio", {}).get("prospect", 0)
        },
        "trackers": {
            "competitors": competitors,
            "pain_points": pain_points,
            "objections": objections,
            "buying_signals": buying_signals
        },
        "outcome": call_data.get("outcome"),
        "sentiment": call_data.get("sentiment")
    })


@app.route("/gong/contacts/<email>/calls", methods=["GET"])
def get_contact_calls(email: str):
    """Get all Gong calls for a specific contact email"""
    # Search calls where this email is a participant
    payload = {
        "filter": {
            "fromDateTime": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z"),
            "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
            "callParticipants": [email]
        }
    }
    
    result = gong_request("POST", "/v2/calls", payload)
    
    if not result or "calls" not in result:
        return jsonify({"message": f"No calls found for {email}", "results": []})
    
    calls = []
    for call in result.get("calls", []):
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
    gong_payload = {
        "filter": {
            "fromDateTime": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z"),
            "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
            "callParticipants": [email]
        }
    }
    
    gong_result = gong_request("POST", "/v2/calls", gong_payload)
    gong_calls = []
    
    if gong_result and "calls" in gong_result:
        for call in gong_result.get("calls", []):
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
║    GET  /gong/calls/<id>/stats       - Get stats & trackers  ║
║    GET  /gong/contacts/<email>/calls - Contact's calls       ║
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
