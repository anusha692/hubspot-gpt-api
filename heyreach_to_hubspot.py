"""
HeyReach → HubSpot Sync Script
===============================

Pulls outbound messages and inbound replies from HeyReach campaigns,
classifies replies with OpenAI (falls back to keyword matching),
and syncs contacts into HubSpot.

Setup:
  pip install requests python-dotenv openai

  Create a .env file with:
    HEYREACH_API_KEY=your_heyreach_api_key
    HUBSPOT_ACCESS_TOKEN=your_hubspot_access_token
    OPENAI_API_KEY=your_openai_api_key (optional — falls back to keywords)
    SLACK_TOFU_REPLIES_WEBHOOK_URL=your_slack_webhook (optional)

  Run: python heyreach_to_hubspot.py
"""

import argparse
import os
import json
import logging
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

HEYREACH_API_KEY = os.getenv("HEYREACH_API_KEY")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SLACK_TOFU_REPLIES_WEBHOOK_URL = os.getenv("SLACK_TOFU_REPLIES_WEBHOOK_URL")

HEYREACH_BASE_URL = "https://api.heyreach.io/api/public"
HUBSPOT_BASE_URL = "https://api.hubapi.com"

STATE_FILE = "heyreach_hubspot_last_run.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# Try to init OpenAI client
openai_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        log.info("OpenAI client initialized — will use GPT for classification")
    except ImportError:
        log.warning("openai package not installed — falling back to keyword classification")


# ── State helpers ─────────────────────────────────────────────────────────────

def load_last_run() -> str | None:
    """Return the ISO timestamp of the last successful run, or None for first run."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            data = json.load(f)
            return data.get("last_run")
    return None


def save_last_run(ts: datetime):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_run": ts.isoformat()}, f)


# ── HeyReach helpers ─────────────────────────────────────────────────────────

def heyreach_headers() -> dict:
    return {"X-API-KEY": HEYREACH_API_KEY, "Content-Type": "application/json"}


def get_all_campaigns() -> list[dict]:
    """Fetch all HeyReach campaigns with pagination."""
    campaigns = []
    offset = 0
    limit = 100

    while True:
        resp = requests.post(
            f"{HEYREACH_BASE_URL}/campaign/GetAll",
            headers=heyreach_headers(),
            json={"offset": offset, "limit": limit},
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", data.get("campaigns", []))
        if not items:
            if isinstance(data, list):
                campaigns.extend(data)
            break

        campaigns.extend(items)

        total = data.get("totalCount", data.get("total", 0))
        offset += limit
        if offset >= total or len(items) < limit:
            break

        time.sleep(0.2)

    log.info(f"Found {len(campaigns)} HeyReach campaigns")
    return campaigns


def get_conversations_for_campaign(campaign_id: int) -> list[dict]:
    """Fetch all conversations for a campaign with pagination."""
    conversations = []
    offset = 0
    limit = 100

    while True:
        resp = requests.post(
            f"{HEYREACH_BASE_URL}/inbox/GetConversationsV2",
            headers=heyreach_headers(),
            json={"campaignId": campaign_id, "offset": offset, "limit": limit},
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", data.get("conversations", []))
        if not items:
            if isinstance(data, list):
                conversations.extend(data)
            break

        conversations.extend(items)

        total = data.get("totalCount", data.get("total", 0))
        offset += limit
        if offset >= total or len(items) < limit:
            break

        time.sleep(0.2)

    return conversations


# ── OpenAI classification ─────────────────────────────────────────────────────

def openai_classify_sentiment(messages: list[dict]) -> dict | None:
    """Use OpenAI to classify reply sentiment. Returns None on failure."""
    if not openai_client:
        return None

    conversation_text = ""
    for msg in messages:
        direction = "OUTBOUND" if msg.get("sender") == "ME" else "INBOUND"
        text = msg.get("body", "")
        conversation_text += f"[{direction}]: {text}\n"

    prompt = f"""Analyze this LinkedIn conversation and classify the lead's response.

Conversation:
{conversation_text}

Respond with a JSON object (no markdown, no code fences) with these fields:
- "reply_sentiment": one of "Enthusiastic", "Positive", "Neutral", "Negative", "Postponed", "Not Yet Responded"
- "taken_off_list": "yes" or "no" — is the lead asking to be removed, opting out, unsubscribing, saying "not interested", "remove me", "stop contacting", etc.?
- "is_postponed": "true" or "false" — is the lead saying "not right now", "reach out later", "busy right now", "maybe next quarter", etc.?
- "sentiment_notes": brief 1-2 sentence explanation of your classification"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        result = json.loads(response.choices[0].message.content.strip())
        return {
            "reply_sentiment": result.get("reply_sentiment", "Neutral"),
            "taken_off_list": result.get("taken_off_list", "no"),
            "is_postponed": result.get("is_postponed", "false"),
            "sentiment_notes": result.get("sentiment_notes", ""),
        }
    except Exception as e:
        log.warning(f"OpenAI sentiment failed, using keywords: {e}")
        return None


def openai_classify_sector(campaign_name: str) -> str | None:
    """Use OpenAI to classify campaign name into a sector. Returns None on failure."""
    if not openai_client:
        return None

    prompt = f"""Given this campaign name, classify it into the most specific sector.

Campaign name: "{campaign_name}"

Pick the single best-fit sector from this list, or identify a more specific sector from the campaign name. NEVER return "Other".

Sectors:
- Webinar — webinar-related campaigns
- Webinar Outreach — LinkedIn connections for webinar outreach
- PR/Comms — PR/comms firms
- Conference Outreach — conference-related
- Political — political campaigns/orgs
- Healthcare — healthcare sector
- Tech — technology sector
- Finance — financial services

Respond with ONLY the sector name, nothing else."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.warning(f"OpenAI sector failed, using keywords: {e}")
        return None


# ── Keyword-based fallback classification ─────────────────────────────────────

OPT_OUT_PHRASES = [
    "not interested", "remove me", "unsubscribe", "stop contacting",
    "take me off", "opt out", "don't contact", "do not contact",
    "leave me alone", "no thanks", "no thank you", "please stop",
    "not for me", "pass on this", "remove my name", "don't message",
    "do not message", "stop messaging", "never contact",
]

POSTPONE_PHRASES = [
    "not right now", "reach out later", "maybe later", "not a good time",
    "busy right now", "next quarter", "next month", "next year",
    "circle back", "check back", "get back to me", "follow up later",
    "touch base later", "not the right time", "revisit", "down the road",
    "in a few months", "in a few weeks", "after the holidays", "end of year",
    "beginning of next", "try again", "reconnect later",
]

POSITIVE_PHRASES = [
    "sounds great", "love to", "would love", "interested", "let's chat",
    "let's connect", "happy to", "sure", "yes", "absolutely",
    "sounds good", "let's do it", "looking forward", "book a time",
    "schedule a call", "set up a meeting", "tell me more", "send me info",
    "i'd like to learn", "sign me up", "count me in",
]

ENTHUSIASTIC_PHRASES = [
    "amazing", "fantastic", "perfect timing", "exactly what",
    "been looking for", "love this", "this is great", "awesome",
    "wonderful", "excellent", "thrilled",
]

NEGATIVE_PHRASES = [
    "not relevant", "wrong person", "don't need", "do not need",
    "already have", "not a fit", "no need", "we're good",
    "we already use", "not looking", "doesn't apply", "spam",
]

SECTOR_PATTERNS = [
    ("webinar outreach", "Webinar Outreach"),
    ("webinar", "Webinar"),
    ("conference", "Conference Outreach"),
    ("summit", "Conference Outreach"),
    ("event", "Conference Outreach"),
    ("pr firm", "PR/Comms"),
    ("pr ", "PR/Comms"),
    ("comms", "PR/Comms"),
    ("communications", "PR/Comms"),
    ("public relations", "PR/Comms"),
    ("political", "Political"),
    ("govt", "Political"),
    ("government", "Political"),
    ("public sector", "Political"),
    ("healthcare", "Healthcare"),
    ("health", "Healthcare"),
    ("hospital", "Healthcare"),
    ("medical", "Healthcare"),
    ("pharma", "Healthcare"),
    ("biotech", "Healthcare"),
    ("finance", "Finance"),
    ("financial", "Finance"),
    ("banking", "Finance"),
    ("insurance", "Finance"),
    ("fintech", "Finance"),
    ("accounting", "Finance"),
    ("cpa", "Finance"),
    ("saas", "Tech"),
    ("software", "Tech"),
    ("tech", "Tech"),
    ("eng ", "Tech"),
    ("engineering", "Tech"),
    ("ai ", "Tech"),
    ("cyber", "Tech"),
    ("startup", "Tech"),
]


def keyword_classify_sentiment(messages: list[dict]) -> dict:
    """Classify reply sentiment using keyword matching."""
    inbound_text = ""
    for msg in messages:
        if msg.get("sender") != "ME":
            inbound_text += " " + (msg.get("body", "") or "")
    inbound_lower = inbound_text.lower().strip()

    if not inbound_lower:
        return {
            "reply_sentiment": "Not Yet Responded",
            "taken_off_list": "no",
            "is_postponed": "false",
            "sentiment_notes": "No inbound messages found",
        }

    taken_off = "no"
    for phrase in OPT_OUT_PHRASES:
        if phrase in inbound_lower:
            taken_off = "yes"
            break

    is_postponed = "false"
    for phrase in POSTPONE_PHRASES:
        if phrase in inbound_lower:
            is_postponed = "true"
            break

    sentiment = "Neutral"
    notes = "Reply detected, classified by keyword matching."

    if taken_off == "yes":
        sentiment = "Negative"
        notes = "Lead appears to be opting out or not interested."
    elif is_postponed == "true":
        sentiment = "Postponed"
        notes = "Lead is interested but wants to revisit later."
    else:
        for phrase in ENTHUSIASTIC_PHRASES:
            if phrase in inbound_lower:
                sentiment = "Enthusiastic"
                notes = "Lead shows strong positive interest."
                break
        if sentiment == "Neutral":
            for phrase in POSITIVE_PHRASES:
                if phrase in inbound_lower:
                    sentiment = "Positive"
                    notes = "Lead shows interest in continuing the conversation."
                    break
        if sentiment == "Neutral":
            for phrase in NEGATIVE_PHRASES:
                if phrase in inbound_lower:
                    sentiment = "Negative"
                    notes = "Lead indicates this is not relevant to them."
                    break
        if sentiment == "Neutral":
            notes = "Reply received but could not determine clear sentiment from keywords."

    return {
        "reply_sentiment": sentiment,
        "taken_off_list": taken_off,
        "is_postponed": is_postponed,
        "sentiment_notes": notes,
    }


def keyword_classify_sector(campaign_name: str) -> str:
    """Classify campaign name into a sector using keyword matching."""
    name_lower = campaign_name.lower()
    for keyword, sector_name in SECTOR_PATTERNS:
        if keyword in name_lower:
            return sector_name
    return "Tech"


# ── Combined classifiers (OpenAI first, keyword fallback) ─────────────────────

def classify_reply_sentiment(messages: list[dict]) -> dict:
    """Classify sentiment — tries OpenAI, falls back to keywords."""
    result = openai_classify_sentiment(messages)
    if result:
        return result
    return keyword_classify_sentiment(messages)


def classify_sector(campaign_name: str, cache: dict) -> str:
    """Classify sector — tries OpenAI, falls back to keywords. Caches results."""
    if campaign_name in cache:
        return cache[campaign_name]

    sector = openai_classify_sector(campaign_name)
    if not sector:
        sector = keyword_classify_sector(campaign_name)

    # Never allow "Other" as a sector
    if sector.lower() == "other":
        sector = keyword_classify_sector(campaign_name)
        if sector.lower() == "other":
            sector = "Outreach"

    cache[campaign_name] = sector
    return sector


# ── Slack notifications ───────────────────────────────────────────────────────

def parse_followup_date(reply_text: str) -> str:
    """Parse a postponed reply to determine when to follow up. Default: 2 weeks."""
    text = reply_text.lower()
    now = datetime.now(timezone.utc)

    patterns = [
        (r"next quarter", timedelta(days=90)),
        (r"next year", timedelta(days=365)),
        (r"next month", timedelta(days=30)),
        (r"in (\d+)\s*months?", None),
        (r"in (\d+)\s*weeks?", None),
        (r"(\d+)\s*months?", None),
        (r"(\d+)\s*weeks?", None),
        (r"end of year", None),
        (r"after the holidays", timedelta(days=45)),
        (r"beginning of next", timedelta(days=30)),
        (r"q[1-4]", None),
    ]

    for pattern, delta in patterns:
        match = re.search(pattern, text)
        if match:
            if delta:
                return (now + delta).strftime("%Y-%m-%d")
            # Parse numeric values
            if "month" in pattern:
                months = int(match.group(1))
                return (now + timedelta(days=months * 30)).strftime("%Y-%m-%d")
            if "week" in pattern:
                weeks = int(match.group(1))
                return (now + timedelta(weeks=weeks)).strftime("%Y-%m-%d")
            if "end of year" in pattern:
                return f"{now.year}-12-31"
            if pattern.startswith("q"):
                q = text[match.start() + 1]
                quarter_month = {"1": 1, "2": 4, "3": 7, "4": 10}.get(q, 1)
                year = now.year if int(q) > (now.month - 1) // 3 + 1 else now.year + 1
                return f"{year}-{quarter_month:02d}-01"

    # Default: 2 weeks
    return (now + timedelta(weeks=2)).strftime("%Y-%m-%d")


def send_postponed_slack_notification(lead: dict):
    """Send a Slack notification to #tofu-replies for a postponed lead."""
    if not SLACK_TOFU_REPLIES_WEBHOOK_URL:
        log.warning("SLACK_TOFU_REPLIES_WEBHOOK_URL not set — skipping Slack notification")
        return

    followup_date = parse_followup_date(lead.get("latest_response_text", ""))
    name = f"{lead.get('firstname', '')} {lead.get('lastname', '')}".strip()
    company = lead.get("company", "")
    campaign = lead.get("latest_outbound_campaign", "")
    reply = lead.get("latest_response_text", "")
    linkedin = lead.get("linkedin", "")

    # Truncate reply if too long
    if len(reply) > 300:
        reply = reply[:300] + "..."

    message = {
        "text": (
            f"*Postponed Reply — Follow Up on {followup_date}*\n"
            f"*Lead:* {name}"
            + (f" at {company}" if company else "")
            + f"\n*Campaign:* {campaign}\n"
            f"*LinkedIn:* {linkedin}\n"
            f"*Reply:* _{reply}_\n"
            f"*Suggested follow-up:* {followup_date}"
        )
    }

    try:
        resp = requests.post(SLACK_TOFU_REPLIES_WEBHOOK_URL, json=message)
        resp.raise_for_status()
        log.info(f"  Slack notification sent for {name} (follow up: {followup_date})")
    except Exception as e:
        log.error(f"  Slack notification failed for {name}: {e}")


# ── Lead extraction ───────────────────────────────────────────────────────────

def to_midnight_ms(ts) -> str:
    """Convert a millisecond timestamp to midnight UTC (required by HubSpot date fields)."""
    try:
        ms = int(ts)
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return str(int(midnight.timestamp() * 1000))
    except (ValueError, TypeError, OSError):
        return ""


def extract_lead_data(conversation: dict, campaign_name: str, sector_cache: dict) -> dict | None:
    """Extract and enrich lead data from a HeyReach conversation."""
    lead = conversation.get("correspondentProfile", {})
    if not lead:
        return None

    profile_url = lead.get("profileUrl", "")
    if not profile_url:
        return None

    first_name = lead.get("firstName", "")
    last_name = lead.get("lastName", "")
    email = lead.get("emailAddress", lead.get("customEmailAddress", lead.get("enrichedEmailAddress", ""))) or ""
    company = lead.get("companyName", "") or ""
    job_title = lead.get("position", lead.get("headline", "")) or ""

    messages = conversation.get("messages", [])
    if not messages:
        return None

    # Separate outbound and inbound messages
    outbound_messages = []
    inbound_messages = []
    for msg in messages:
        if msg.get("sender") == "ME":
            outbound_messages.append(msg)
        else:
            inbound_messages.append(msg)

    has_responded = len(inbound_messages) > 0

    # Get latest outbound date
    latest_outbound_date = None
    for msg in outbound_messages:
        ts = msg.get("createdAt", "")
        if ts and (latest_outbound_date is None or str(ts) > str(latest_outbound_date)):
            latest_outbound_date = ts

    # Get latest inbound reply
    latest_response_text = ""
    latest_response_date = None
    for msg in inbound_messages:
        ts = msg.get("createdAt", "")
        if ts and (latest_response_date is None or str(ts) > str(latest_response_date)):
            latest_response_date = ts
            latest_response_text = msg.get("body", "")

    # Classify sentiment if lead has replied
    sentiment_data = {
        "reply_sentiment": "Not Yet Responded",
        "taken_off_list": "no",
        "is_postponed": "false",
        "sentiment_notes": "",
    }
    if has_responded:
        sentiment_data = classify_reply_sentiment(messages)

    # Classify sector from campaign name
    sector = classify_sector(campaign_name, sector_cache)

    # Compute follow-up date for postponed leads
    followup_date = ""
    if sentiment_data["is_postponed"] == "true":
        followup_date = parse_followup_date(latest_response_text)

    return {
        "firstname": first_name,
        "lastname": last_name,
        "email": email,
        "company": company,
        "jobtitle": job_title,
        "linkedin": profile_url,
        "hs_linkedin_url": profile_url,
        "outbound_platform": "Heyreach",
        "latest_outbound_campaign": campaign_name,
        "latest_outbound_date": to_midnight_ms(latest_outbound_date) if latest_outbound_date else "",
        "has_responded": "true" if has_responded else "false",
        "reply_sentiment": sentiment_data["reply_sentiment"],
        "latest_response_text": latest_response_text,
        "latest_response_date": to_midnight_ms(latest_response_date) if latest_response_date else "",
        "latest_response_platform": "Heyreach" if has_responded else "",
        "taken_off_list": sentiment_data["taken_off_list"],
        "is_postponed": sentiment_data["is_postponed"],
        "followup_date": followup_date,
        "sector": sector,
        "sentiment_notes": sentiment_data["sentiment_notes"],
        "response_count": str(len(inbound_messages)),
    }


# ── HubSpot helpers ───────────────────────────────────────────────────────────

def hubspot_headers() -> dict:
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def batch_upsert_contacts(leads: list[dict]):
    """
    Upsert contacts into HubSpot using the batch upsert API.
    Leads with email are upserted keyed on email.
    Leads without email are created individually.
    """
    if not leads:
        return {"created": 0, "updated": 0, "errors": 0}

    results = {"created": 0, "updated": 0, "errors": 0}

    # Split leads: those with email can use batch upsert, others need individual create
    leads_with_email = [l for l in leads if l.get("email")]
    leads_without_email = [l for l in leads if not l.get("email")]

    # Batch upsert leads that have email (keyed on email)
    for i in range(0, len(leads_with_email), 100):
        batch = leads_with_email[i : i + 100]
        inputs = []
        for lead in batch:
            properties = {k: v for k, v in lead.items() if v}
            inputs.append({
                "idProperty": "email",
                "id": lead["email"],
                "properties": properties,
            })

        payload = {"inputs": inputs}

        try:
            resp = requests.post(
                f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/batch/upsert",
                headers=hubspot_headers(),
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

            for result in data.get("results", []):
                if result.get("new", False):
                    results["created"] += 1
                else:
                    results["updated"] += 1

        except requests.exceptions.HTTPError as e:
            log.error(f"HubSpot batch upsert failed: {e}")
            try:
                log.error(f"Response: {resp.text[:500]}")
            except Exception:
                log.error("Could not read response body")
            results["errors"] += len(batch)
        except Exception as e:
            log.error(f"HubSpot batch upsert failed: {e}")
            results["errors"] += len(batch)

        time.sleep(0.5)

    # Create leads without email individually
    for lead in leads_without_email:
        properties = {k: v for k, v in lead.items() if v}
        try:
            resp = requests.post(
                f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts",
                headers=hubspot_headers(),
                json={"properties": properties},
            )
            if resp.status_code == 409:
                results["updated"] += 1
            else:
                resp.raise_for_status()
                results["created"] += 1
        except requests.exceptions.HTTPError as e:
            log.error(f"HubSpot create failed for {lead.get('linkedin', 'unknown')}: {resp.text[:300]}")
            results["errors"] += 1
        except Exception as e:
            log.error(f"HubSpot create failed: {e}")
            results["errors"] += 1

        time.sleep(0.1)

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HeyReach → HubSpot sync")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max number of campaigns to process (0 = all)")
    parser.add_argument("--max-leads", type=int, default=0,
                        help="Max number of leads to process per campaign (0 = all)")
    args = parser.parse_args()

    log.info("=== HeyReach → HubSpot sync starting ===")

    # Validate env vars
    if not HEYREACH_API_KEY:
        log.error("HEYREACH_API_KEY not set")
        return
    if not HUBSPOT_ACCESS_TOKEN:
        log.error("HUBSPOT_ACCESS_TOKEN not set")
        return

    # 1. Load last run state
    last_run = load_last_run()
    now = datetime.now(timezone.utc)
    if last_run:
        log.info(f"Incremental sync — pulling conversations since {last_run}")
    else:
        log.info("First run — pulling all conversations")

    # 2. Get all campaigns
    try:
        campaigns = get_all_campaigns()
    except Exception as e:
        log.error(f"Failed to fetch campaigns: {e}")
        return

    if not campaigns:
        log.info("No campaigns found. Nothing to do.")
        save_last_run(now)
        return

    if args.limit > 0:
        log.info(f"Limiting to first {args.limit} campaigns (of {len(campaigns)})")
        campaigns = campaigns[:args.limit]

    # 3. Process each campaign
    all_leads = []
    postponed_leads = []
    sector_cache = {}
    total_conversations = 0
    errors = []

    for campaign in campaigns:
        campaign_id = campaign.get("id") or campaign.get("campaignId")
        campaign_name = campaign.get("name", f"Campaign-{campaign_id}")
        log.info(f"Processing campaign: {campaign_name} (id={campaign_id})")

        try:
            conversations = get_conversations_for_campaign(campaign_id)
        except Exception as e:
            log.error(f"  Failed to fetch conversations for '{campaign_name}': {e}")
            errors.append(f"Campaign '{campaign_name}': {e}")
            continue

        log.info(f"  Found {len(conversations)} conversations")
        total_conversations += len(conversations)

        campaign_lead_count = 0
        for conv in conversations:
            # Filter by timestamp if incremental run
            if last_run:
                latest_ts = conv.get("lastMessageAt", "")
                if not latest_ts:
                    conv_messages = conv.get("messages", [])
                    for msg in conv_messages:
                        ts = msg.get("createdAt", "")
                        if ts and (not latest_ts or str(ts) > str(latest_ts)):
                            latest_ts = ts

                if latest_ts and str(latest_ts) < last_run:
                    continue

            if args.max_leads > 0 and campaign_lead_count >= args.max_leads:
                break

            try:
                lead_data = extract_lead_data(conv, campaign_name, sector_cache)
                if lead_data:
                    all_leads.append(lead_data)
                    campaign_lead_count += 1
                    log.info(f"  Extracted lead: {lead_data['firstname']} {lead_data['lastname']} ({lead_data['linkedin']})")

                    # Track postponed leads for Slack
                    if lead_data["is_postponed"] == "true":
                        postponed_leads.append(lead_data)

            except Exception as e:
                log.error(f"  Failed to process conversation: {e}")
                errors.append(f"Lead extraction error: {e}")

        time.sleep(0.2)

    # Deduplicate leads by email — keep the entry with the most data
    seen = {}
    no_email = []
    for lead in all_leads:
        key = lead.get("email", "").strip().lower()
        if not key:
            no_email.append(lead)
            continue

        if key not in seen:
            seen[key] = lead
        else:
            existing = seen[key]
            # Prefer the entry that has a reply
            if lead["has_responded"] == "true" and existing["has_responded"] != "true":
                seen[key] = lead
            # If both have replies, keep the one with the later response
            elif (lead["has_responded"] == "true" and existing["has_responded"] == "true"
                  and lead.get("latest_response_date", "") > existing.get("latest_response_date", "")):
                seen[key] = lead

    all_leads = list(seen.values()) + no_email
    log.info(f"Total unique leads to sync: {len(all_leads)} ({len(seen)} with email, {len(no_email)} without)")

    # 4. Upsert into HubSpot
    if all_leads:
        log.info("Upserting contacts into HubSpot...")
        results = batch_upsert_contacts(all_leads)
    else:
        results = {"created": 0, "updated": 0, "errors": 0}
        log.info("No leads to sync")

    # 5. Save state
    save_last_run(now)

    # 6. Summary
    log.info("=" * 60)
    log.info("SYNC SUMMARY")
    log.info("=" * 60)
    log.info(f"  Campaigns processed:   {len(campaigns)}")
    log.info(f"  Conversations found:   {total_conversations}")
    log.info(f"  Leads extracted:       {len(all_leads)}")
    log.info(f"  Contacts created:      {results['created']}")
    log.info(f"  Contacts updated:      {results['updated']}")
    log.info(f"  Postponed replies:     {len(postponed_leads)}")
    log.info(f"  Errors:                {results['errors'] + len(errors)}")
    if errors:
        log.info("  Error details:")
        for err in errors:
            log.info(f"    - {err}")
    log.info("=" * 60)
    log.info("=== Sync complete ===")


if __name__ == "__main__":
    main()
