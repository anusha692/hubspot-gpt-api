"""
HubSpot Query API for Custom GPT
=================================

A simple API that allows a Custom GPT to query HubSpot for:
- Gong call data (transcripts, summaries, competitors, buying signals)
- Outbound activity (campaigns, replies, sentiment)
- Contact information

Deployment Options:
- Render.com (free tier)
- Railway.app ($5/mo)
- Vercel (free tier)
- AWS Lambda + API Gateway

Usage:
    pip install flask flask-cors requests gunicorn
    python hubspot_gpt_api.py

For production:
    gunicorn hubspot_gpt_api:app --bind 0.0.0.0:$PORT
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
CORS(app)  # Allow GPT to call this API

# ============================================================================
# CONFIGURATION
# ============================================================================

# Use environment variable in production, fallback to hardcoded for testing
HUBSPOT_ACCESS_TOKEN = os.getenv(
    "HUBSPOT_ACCESS_TOKEN", 
    "pat-na1-3cbd-0139-41ec-a37f-c4bda54e56ff"
)
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Optional: Add a simple API key for your GPT
GPT_API_KEY = os.getenv("GPT_API_KEY", "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")


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
    
    # Properties to fetch
    properties = [
        "email", "firstname", "lastname", "company", "jobtitle",
        "sector", "industry",
        # Gong fields
        "latest_call_date", "latest_call_duration", "latest_call_gong_url",
        "latest_call_summary", "gong_total_calls", "gong_buying_signals",
        "gong_competitor_mentioned", "gong_topics_discussed", "gong_next_steps",
        "pain_points_mentioned", "gong_last_sentiment",
        # Outbound fields
        "outbound_platform", "latest_outbound_campaign", "latest_outbound_date",
        "has_responded", "reply_sentiment", "latest_response_text",
        # Website
        "has_visited_website"
    ]
    
    # Build filter based on search type
    if search_by == "email":
        filters = [{
            "propertyName": "email",
            "operator": "CONTAINS_TOKEN",
            "value": query
        }]
    elif search_by == "company":
        filters = [{
            "propertyName": "company",
            "operator": "CONTAINS_TOKEN", 
            "value": query
        }]
    else:
        # Search both
        filters = [{
            "propertyName": "email",
            "operator": "CONTAINS_TOKEN",
            "value": query
        }]
    
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
# API ENDPOINTS
# ============================================================================

@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "HubSpot GPT Query API",
        "version": "1.0.0"
    })


@app.route("/search", methods=["POST"])
def search():
    """
    Search for contacts and return Gong/outbound data
    
    Request body:
    {
        "query": "john@acme.com" or "Acme Corp",
        "search_by": "email" or "company" (optional, default: auto-detect)
    }
    """
    data = request.json or {}
    query = data.get("query", "").strip()
    
    if not query:
        return jsonify({"error": "Please provide a search query"}), 400
    
    # Auto-detect search type
    search_by = data.get("search_by")
    if not search_by:
        search_by = "email" if "@" in query else "company"
    
    # Search HubSpot
    contacts = search_contacts(query, search_by)
    
    if not contacts:
        return jsonify({
            "message": f"No contacts found matching '{query}'",
            "results": []
        })
    
    # Format results
    results = [format_contact_for_gpt(c) for c in contacts]
    
    return jsonify({
        "message": f"Found {len(results)} contact(s)",
        "results": results
    })


@app.route("/contact/<email>", methods=["GET"])
def get_contact(email: str):
    """
    Get a specific contact by email
    """
    contacts = search_contacts(email, "email")
    
    if not contacts:
        return jsonify({"error": f"Contact not found: {email}"}), 404
    
    return jsonify(format_contact_for_gpt(contacts[0]))


@app.route("/calls/recent", methods=["GET"])
def recent_calls():
    """
    Get contacts with recent Gong calls
    """
    # Search for contacts with Gong data
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "gong_total_calls",
                "operator": "GT",
                "value": "0"
            }]
        }],
        "properties": [
            "email", "firstname", "lastname", "company",
            "latest_call_date", "latest_call_summary", 
            "gong_buying_signals", "gong_competitor_mentioned"
        ],
        "sorts": [{
            "propertyName": "latest_call_date",
            "direction": "DESCENDING"
        }],
        "limit": 20
    }
    
    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)
    
    if not result or "results" not in result:
        return jsonify({"message": "No recent calls found", "results": []})
    
    # Format for GPT
    calls = []
    for contact in result["results"]:
        props = contact.get("properties", {})
        calls.append({
            "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
            "email": props.get("email"),
            "company": props.get("company"),
            "last_call_date": props.get("latest_call_date"),
            "summary": props.get("latest_call_summary", "")[:200] + "..." if props.get("latest_call_summary") else "",
            "buying_signals": props.get("gong_buying_signals", "None"),
            "competitors": props.get("gong_competitor_mentioned", "None")
        })
    
    return jsonify({
        "message": f"Found {len(calls)} recent calls",
        "results": calls
    })


@app.route("/competitors", methods=["GET"])
def competitor_mentions():
    """
    Get contacts where competitors were mentioned
    """
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "gong_competitor_mentioned",
                "operator": "HAS_PROPERTY"
            }]
        }],
        "properties": [
            "email", "firstname", "lastname", "company",
            "latest_call_date", "gong_competitor_mentioned", "latest_call_summary"
        ],
        "limit": 20
    }
    
    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)
    
    if not result or "results" not in result:
        return jsonify({"message": "No competitor mentions found", "results": []})
    
    mentions = []
    for contact in result["results"]:
        props = contact.get("properties", {})
        if props.get("gong_competitor_mentioned"):
            mentions.append({
                "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                "email": props.get("email"),
                "company": props.get("company"),
                "competitors_mentioned": props.get("gong_competitor_mentioned"),
                "call_date": props.get("latest_call_date"),
                "context": props.get("latest_call_summary", "")[:200] if props.get("latest_call_summary") else ""
            })
    
    return jsonify({
        "message": f"Found {len(mentions)} competitor mentions",
        "results": mentions
    })


@app.route("/buying-signals", methods=["GET"])
def buying_signals():
    """
    Get contacts with buying signals detected
    """
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "gong_buying_signals",
                "operator": "HAS_PROPERTY"
            }]
        }],
        "properties": [
            "email", "firstname", "lastname", "company",
            "latest_call_date", "gong_buying_signals", "latest_call_summary"
        ],
        "limit": 20
    }
    
    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)
    
    if not result or "results" not in result:
        return jsonify({"message": "No buying signals found", "results": []})
    
    signals = []
    for contact in result["results"]:
        props = contact.get("properties", {})
        if props.get("gong_buying_signals"):
            signals.append({
                "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                "email": props.get("email"),
                "company": props.get("company"),
                "buying_signals": props.get("gong_buying_signals"),
                "call_date": props.get("latest_call_date")
            })
    
    return jsonify({
        "message": f"Found {len(signals)} contacts with buying signals",
        "results": signals
    })


@app.route("/replies", methods=["GET"])
def positive_replies():
    """
    Get contacts who replied positively to outreach
    """
    sentiment = request.args.get("sentiment", "Positive")
    
    payload = {
        "filterGroups": [{
            "filters": [
                {
                    "propertyName": "has_responded",
                    "operator": "EQ",
                    "value": "true"
                },
                {
                    "propertyName": "reply_sentiment",
                    "operator": "EQ",
                    "value": sentiment
                }
            ]
        }],
        "properties": [
            "email", "firstname", "lastname", "company", "sector",
            "outbound_platform", "latest_outbound_campaign",
            "reply_sentiment", "latest_response_text"
        ],
        "limit": 20
    }
    
    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)
    
    if not result or "results" not in result:
        return jsonify({"message": f"No {sentiment} replies found", "results": []})
    
    replies = []
    for contact in result["results"]:
        props = contact.get("properties", {})
        replies.append({
            "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
            "email": props.get("email"),
            "company": props.get("company"),
            "sector": props.get("sector"),
            "platform": props.get("outbound_platform"),
            "campaign": props.get("latest_outbound_campaign"),
            "sentiment": props.get("reply_sentiment"),
            "reply_preview": props.get("latest_response_text", "")[:100] if props.get("latest_response_text") else ""
        })
    
    return jsonify({
        "message": f"Found {len(replies)} {sentiment} replies",
        "results": replies
    })


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           HubSpot GPT Query API                              ║
╠══════════════════════════════════════════════════════════════╣
║  Endpoints:                                                  ║
║    GET  /                  - Health check                    ║
║    POST /search            - Search contacts                 ║
║    GET  /contact/<email>   - Get specific contact            ║
║    GET  /calls/recent      - Recent Gong calls               ║
║    GET  /competitors       - Competitor mentions             ║
║    GET  /buying-signals    - Buying signals detected         ║
║    GET  /replies           - Positive/negative replies       ║
╚══════════════════════════════════════════════════════════════╝
    
Running on http://localhost:{port}
    """)
    
    app.run(host="0.0.0.0", port=port, debug=debug)
