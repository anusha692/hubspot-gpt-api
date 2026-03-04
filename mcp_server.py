"""
Sales Intelligence MCP Server
==============================

MCP server for Claude.ai that provides access to Gong call recordings,
transcript semantic search, and HubSpot CRM data.

Uses the official MCP Python SDK with streamable-http transport.
"""

import os
import base64
import requests as http_requests
from datetime import datetime, timedelta

from openai import OpenAI
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# ============================================================================
# CONFIGURATION
# ============================================================================

HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

GONG_BASE_URL = os.environ.get("GONG_BASE_URL", "https://us-22394.api.gong.io")
GONG_API_KEY = os.environ.get("GONG_API_KEY")
GONG_API_SECRET = os.environ.get("GONG_API_SECRET")

MONGODB_URI = os.environ.get("MONGODB_URI")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ============================================================================
# API CLIENTS
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
            response = http_requests.get(url, headers=headers, params=params)
        elif method == "POST":
            response = http_requests.post(url, headers=headers, json=json_data)

        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception:
        return None


def hubspot_request(method, endpoint, json_data=None):
    """Make a request to HubSpot API."""
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    url = f"{HUBSPOT_BASE_URL}{endpoint}"

    if method == "GET":
        response = http_requests.get(url, headers=headers)
    elif method == "POST":
        response = http_requests.post(url, headers=headers, json=json_data)

    return response.json() if response.status_code == 200 else None


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


# ============================================================================
# MCP SERVER
# ============================================================================

mcp = FastMCP(
    "Sales Intelligence",
    stateless_http=True,
)


@mcp.tool()
def search_gong_calls(
    from_date: str = "",
    to_date: str = "",
    limit: int = 20,
) -> dict:
    """Search Gong calls by date range. Returns call IDs, titles, participants, and recording URLs.
    Defaults to last 7 days if no dates provided."""
    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")
    if not from_date:
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    params = {
        "fromDateTime": f"{from_date}T00:00:00Z",
        "toDateTime": f"{to_date}T23:59:59Z",
    }

    result = gong_request("GET", "/v2/calls", params=params)

    if not result or "calls" not in result:
        return {"message": "No calls found", "results": []}

    calls = []
    for call in result.get("calls", [])[:limit]:
        calls.append({
            "call_id": call.get("id"),
            "title": call.get("title", "Untitled Call"),
            "started": call.get("started"),
            "duration_seconds": call.get("duration"),
            "url": call.get("url"),
            "participants": [p.get("emailAddress") for p in call.get("parties", [])],
            "direction": call.get("direction"),
        })

    return {
        "message": f"Found {len(calls)} calls from {from_date} to {to_date}",
        "results": calls,
    }


@mcp.tool()
def get_call_transcript(call_id: str) -> dict:
    """Get the full transcript of a Gong call, broken down by speaker segments."""
    payload = {
        "filter": {
            "fromDateTime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z"),
            "toDateTime": datetime.now().strftime("%Y-%m-%dT23:59:59Z"),
            "callIds": [call_id],
        }
    }

    result = gong_request("POST", "/v2/calls/transcript", json_data=payload)

    if not result or not result.get("callTranscripts"):
        return {"error": "Call transcript not found"}

    call_transcript_data = result.get("callTranscripts", [])[0]
    transcript_segments = call_transcript_data.get("transcript", [])

    formatted_transcript = []
    for segment in transcript_segments:
        sentences = segment.get("sentences", [])
        full_text = " ".join(s.get("text", "") for s in sentences)
        start_time = sentences[0].get("start") if sentences else None
        end_time = sentences[-1].get("end") if sentences else None

        formatted_transcript.append({
            "speaker_id": segment.get("speakerId"),
            "topic": segment.get("topic", ""),
            "text": full_text,
            "start_time": start_time,
            "end_time": end_time,
        })

    return {"call_id": call_id, "transcript": formatted_transcript}


@mcp.tool()
def search_transcripts(
    query: str,
    from_date: str = "",
    to_date: str = "",
    limit: int = 20,
) -> dict:
    """Semantic search across all Gong transcripts from the last 3 months.
    Use this to find calls mentioning specific topics, stories, or patterns.
    Call titles follow the format 'Company (Contact) <> Clipbook (Rep)' —
    use company names to cross-reference with HubSpot deals."""
    if not query.strip():
        return {"error": "Please provide a search query"}

    if not MONGODB_URI or not OPENAI_API_KEY:
        return {"error": "MONGODB_URI and OPENAI_API_KEY must be configured"}

    query_embedding = embed_query(query)

    fetch_limit = limit * 3 if (from_date or to_date) else limit

    pipeline = [
        {
            "$vectorSearch": {
                "index": "vector_index_1",
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": fetch_limit * 10,
                "limit": fetch_limit,
            }
        },
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
        return {"error": f"Vector search failed: {str(e)}"}

    matching_calls = []
    seen_call_ids = set()

    for r in results:
        r["_id"] = str(r["_id"])
        if r.get("call_date"):
            r["call_date"] = r["call_date"].isoformat()
        matching_calls.append(r)
        seen_call_ids.add(r.get("call_id"))

    mongo_client.close()

    return {
        "query": query,
        "matching_chunks": len(matching_calls),
        "unique_calls": len(seen_call_ids),
        "results": matching_calls,
    }


@mcp.tool()
def search_hubspot_contacts(
    query: str,
    search_by: str = "",
) -> dict:
    """Search HubSpot contacts by email or company name."""
    if not query.strip():
        return {"error": "Please provide a search query"}

    if not search_by:
        search_by = "email" if "@" in query else "company"

    properties = [
        "email", "firstname", "lastname", "company", "jobtitle",
        "sector", "industry",
        "latest_call_date", "latest_call_duration", "latest_call_gong_url",
        "latest_call_summary", "gong_total_calls", "gong_buying_signals",
        "gong_competitor_mentioned", "gong_topics_discussed", "gong_next_steps",
        "pain_points_mentioned", "gong_last_sentiment",
    ]

    filters = [{"propertyName": search_by, "operator": "CONTAINS_TOKEN", "value": query}]

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": properties,
        "limit": 10,
    }

    result = hubspot_request("POST", "/crm/v3/objects/contacts/search", payload)

    if not result or "results" not in result:
        return {"message": f"No contacts found matching '{query}'", "results": []}

    contacts = []
    for contact in result["results"]:
        props = contact.get("properties", {})
        contacts.append({
            "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or "Unknown",
            "email": props.get("email", ""),
            "company": props.get("company", "Unknown"),
            "title": props.get("jobtitle", ""),
            "total_calls": props.get("gong_total_calls", "0"),
            "last_call_date": props.get("latest_call_date", ""),
            "call_summary": props.get("latest_call_summary", ""),
            "buying_signals": props.get("gong_buying_signals", ""),
            "competitors_mentioned": props.get("gong_competitor_mentioned", ""),
            "pain_points": props.get("pain_points_mentioned", ""),
        })

    return {"message": f"Found {len(contacts)} contact(s)", "results": contacts}


@mcp.tool()
def get_deal_pipelines() -> dict:
    """List all HubSpot deal pipelines and their stages.
    ALWAYS call this first to discover stage IDs before searching deals by stage."""
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    response = http_requests.get(f"{HUBSPOT_BASE_URL}/crm/v3/pipelines/deals", headers=headers)

    if response.status_code != 200:
        return {"error": "Could not fetch pipelines"}

    result = response.json()
    if "results" not in result:
        return {"error": "Unexpected response format"}

    pipelines = []
    for pipeline in result["results"]:
        stages = []
        for stage in pipeline.get("stages", []):
            stages.append({
                "stage_id": stage.get("id"),
                "label": stage.get("label"),
                "display_order": stage.get("displayOrder"),
            })
        pipelines.append({
            "pipeline_id": pipeline.get("id"),
            "label": pipeline.get("label"),
            "stages": stages,
        })

    return {"pipelines": pipelines}


@mcp.tool()
def search_deals(
    stage: str = "",
    from_date: str = "",
    to_date: str = "",
    company: str = "",
    limit: int = 100,
) -> dict:
    """Search HubSpot deals by stage ID, date range, or company name.
    Use get_deal_pipelines first to discover the correct stage IDs."""
    filters = []

    if stage:
        filters.append({
            "propertyName": "dealstage",
            "operator": "EQ",
            "value": stage,
        })

    if from_date:
        ts = int(datetime.fromisoformat(from_date).timestamp() * 1000)
        filters.append({
            "propertyName": "closedate",
            "operator": "GTE",
            "value": str(ts),
        })

    if to_date:
        ts = int(datetime.fromisoformat(to_date + "T23:59:59").timestamp() * 1000)
        filters.append({
            "propertyName": "closedate",
            "operator": "LTE",
            "value": str(ts),
        })

    if company:
        filters.append({
            "propertyName": "dealname",
            "operator": "CONTAINS_TOKEN",
            "value": company,
        })

    payload = {
        "filterGroups": [{"filters": filters}] if filters else [],
        "properties": [
            "dealname", "dealstage", "amount", "closedate",
            "pipeline", "hs_lastmodifieddate",
        ],
        "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}],
        "limit": limit,
    }

    result = hubspot_request("POST", "/crm/v3/objects/deals/search", payload)

    if not result or "results" not in result:
        return {"message": "No deals found", "results": [], "total": 0}

    deals = []
    for deal in result["results"]:
        props = deal.get("properties", {})
        deals.append({
            "deal_id": deal.get("id"),
            "name": props.get("dealname", ""),
            "stage": props.get("dealstage", ""),
            "amount": props.get("amount", ""),
            "close_date": props.get("closedate", ""),
            "pipeline": props.get("pipeline", ""),
        })

    return {
        "message": f"Found {len(deals)} deals",
        "total": result.get("total", len(deals)),
        "results": deals,
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
