"""
Instantly → HubSpot Sync Script
================================

Pulls outbound emails and inbound replies from Instantly campaigns,
classifies replies with OpenAI (falls back to keyword matching),
and syncs contacts into HubSpot.

Setup:
  pip install requests python-dotenv openai

  Add to .env:
    INSTANTLY_API_KEY=your_instantly_api_key
    HUBSPOT_ACCESS_TOKEN=your_hubspot_access_token
    OPENAI_API_KEY=your_openai_api_key (optional — falls back to keywords)
    SLACK_TOFU_REPLIES_WEBHOOK_URL=your_slack_webhook (optional)

  Run: python instantly_to_hubspot.py
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

INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SLACK_TOFU_REPLIES_WEBHOOK_URL = os.getenv("SLACK_TOFU_REPLIES_WEBHOOK_URL")

INSTANTLY_BASE_URL = "https://api.instantly.ai/api/v2"
HUBSPOT_BASE_URL = "https://api.hubapi.com"

STATE_FILE = "instantly_hubspot_last_run.json"

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
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            data = json.load(f)
            return data.get("last_run")
    return None


def save_last_run(ts: datetime):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_run": ts.isoformat()}, f)


# ── Instantly helpers ─────────────────────────────────────────────────────────

def instantly_headers() -> dict:
    return {
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
        "Content-Type": "application/json",
    }


def get_all_campaigns() -> list[dict]:
    """Fetch all Instantly campaigns with cursor pagination."""
    campaigns = []
    starting_after = None

    while True:
        params = {"limit": 100}
        if starting_after:
            params["starting_after"] = starting_after

        resp = requests.get(
            f"{INSTANTLY_BASE_URL}/campaigns",
            headers=instantly_headers(),
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        if not items:
            break

        campaigns.extend(items)

        # Cursor pagination
        next_cursor = None
        if isinstance(data, dict):
            next_cursor = data.get("next_starting_after")
        if not next_cursor and items:
            next_cursor = items[-1].get("id")

        if not next_cursor or len(items) < 100:
            break

        starting_after = next_cursor
        time.sleep(0.2)

    log.info(f"Found {len(campaigns)} Instantly campaigns")
    return campaigns


def get_leads_for_campaign(campaign_id: str, max_leads: int = 0) -> list[dict]:
    """Fetch leads for a campaign with cursor pagination."""
    leads = []
    starting_after = None

    while True:
        payload = {
            "campaign_id": campaign_id,
            "limit": 100,
        }
        if starting_after:
            payload["starting_after"] = starting_after

        resp = requests.post(
            f"{INSTANTLY_BASE_URL}/leads/list",
            headers=instantly_headers(),
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        if not items:
            break

        leads.extend(items)

        if max_leads > 0 and len(leads) >= max_leads:
            leads = leads[:max_leads]
            break

        # Cursor pagination
        next_cursor = None
        if isinstance(data, dict):
            next_cursor = data.get("next_starting_after")
        if not next_cursor and items:
            next_cursor = items[-1].get("id")

        if not next_cursor or len(items) < 100:
            break

        starting_after = next_cursor
        time.sleep(0.2)

    return leads


def get_emails_for_lead(lead_email: str, campaign_id: str) -> list[dict]:
    """Fetch all emails (sent + received) for a lead in a campaign."""
    emails = []

    for email_type in ["sent", "received"]:
        starting_after = None
        while True:
            params = {
                "campaign_id": campaign_id,
                "lead": lead_email,
                "email_type": email_type,
                "limit": 50,
            }
            if starting_after:
                params["starting_after"] = starting_after

            resp = requests.get(
                f"{INSTANTLY_BASE_URL}/emails",
                headers=instantly_headers(),
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

            items = data if isinstance(data, list) else data.get("items", data.get("data", []))
            if not items:
                break

            for item in items:
                item["_email_type"] = email_type
            emails.extend(items)

            next_cursor = None
            if isinstance(data, dict):
                next_cursor = data.get("next_starting_after")
            if not next_cursor or len(items) < 50:
                break

            starting_after = next_cursor
            time.sleep(0.1)

    return emails


# ── OpenAI classification ─────────────────────────────────────────────────────

def openai_classify_sentiment(messages: list[dict]) -> dict | None:
    if not openai_client:
        return None

    conversation_text = ""
    for msg in messages:
        direction = msg.get("_email_type", "sent").upper()
        if direction == "SENT":
            direction = "OUTBOUND"
        else:
            direction = "INBOUND"
        body = msg.get("body", {})
        text = body.get("text", "") if isinstance(body, dict) else str(body)
        conversation_text += f"[{direction}]: {text}\n"

    prompt = f"""Analyze this email conversation and classify the lead's response.

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
    inbound_text = ""
    for msg in messages:
        if msg.get("_email_type") == "received":
            body = msg.get("body", {})
            text = body.get("text", "") if isinstance(body, dict) else str(body)
            inbound_text += " " + text
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
    name_lower = campaign_name.lower()
    for keyword, sector_name in SECTOR_PATTERNS:
        if keyword in name_lower:
            return sector_name
    return "Outreach"


# ── Combined classifiers ─────────────────────────────────────────────────────

def classify_reply_sentiment(messages: list[dict]) -> dict:
    result = openai_classify_sentiment(messages)
    if result:
        return result
    return keyword_classify_sentiment(messages)


def classify_sector(campaign_name: str, cache: dict) -> str:
    if campaign_name in cache:
        return cache[campaign_name]

    sector = openai_classify_sector(campaign_name)
    if not sector:
        sector = keyword_classify_sector(campaign_name)

    if sector.lower() == "other":
        sector = keyword_classify_sector(campaign_name)
        if sector.lower() == "other":
            sector = "Outreach"

    cache[campaign_name] = sector
    return sector


# ── Slack notifications ───────────────────────────────────────────────────────

def parse_followup_date(reply_text: str) -> str:
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

    return (now + timedelta(weeks=2)).strftime("%Y-%m-%d")


def send_postponed_slack_notification(lead: dict):
    if not SLACK_TOFU_REPLIES_WEBHOOK_URL:
        log.warning("SLACK_TOFU_REPLIES_WEBHOOK_URL not set — skipping Slack notification")
        return

    followup_date = parse_followup_date(lead.get("latest_response_text", ""))
    name = f"{lead.get('firstname', '')} {lead.get('lastname', '')}".strip()
    company = lead.get("company", "")
    campaign = lead.get("latest_outbound_campaign", "")
    reply = lead.get("latest_response_text", "")
    email = lead.get("email", "")

    if len(reply) > 300:
        reply = reply[:300] + "..."

    message = {
        "text": (
            f"*Postponed Reply — Follow Up on {followup_date}*\n"
            f"*Lead:* {name}"
            + (f" at {company}" if company else "")
            + f"\n*Campaign:* {campaign}\n"
            f"*Email:* {email}\n"
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


# ── Date helper ──────────────────────────────────────────────────────────────

def to_midnight_ms(ts_str: str) -> str:
    """Convert an ISO timestamp string to midnight UTC ms (required by HubSpot)."""
    try:
        if not ts_str:
            return ""
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return str(int(midnight.timestamp() * 1000))
    except (ValueError, TypeError):
        return ""


# ── Lead extraction ───────────────────────────────────────────────────────────

def extract_lead_data(lead: dict, emails: list[dict], campaign_name: str, sector_cache: dict) -> dict | None:
    email_addr = lead.get("email", "")
    if not email_addr:
        return None

    first_name = lead.get("first_name", "")
    last_name = lead.get("last_name", "")
    company = lead.get("company_name", "")
    payload = lead.get("payload", {}) or {}
    job_title = payload.get("job_title", payload.get("title", payload.get("position", "")))

    # Separate outbound and inbound emails
    outbound = [e for e in emails if e.get("_email_type") == "sent"]
    inbound = [e for e in emails if e.get("_email_type") == "received"]

    has_responded = len(inbound) > 0

    # Get latest outbound date
    latest_outbound_date = ""
    for e in outbound:
        ts = e.get("timestamp_email", e.get("timestamp_created", ""))
        if ts and ts > latest_outbound_date:
            latest_outbound_date = ts

    # Get latest inbound reply
    latest_response_text = ""
    latest_response_date = ""
    for e in inbound:
        ts = e.get("timestamp_email", e.get("timestamp_created", ""))
        if ts and ts > latest_response_date:
            latest_response_date = ts
            body = e.get("body", {})
            latest_response_text = body.get("text", "") if isinstance(body, dict) else str(body)

    # Classify sentiment if lead has replied
    sentiment_data = {
        "reply_sentiment": "Not Yet Responded",
        "taken_off_list": "no",
        "is_postponed": "false",
        "sentiment_notes": "",
    }
    if has_responded:
        sentiment_data = classify_reply_sentiment(emails)

    # Classify sector
    sector = classify_sector(campaign_name, sector_cache)

    # Compute follow-up date for postponed leads
    followup_date = ""
    if sentiment_data["is_postponed"] == "true":
        followup_date = parse_followup_date(latest_response_text)

    return {
        "firstname": first_name,
        "lastname": last_name,
        "email": email_addr,
        "company": company,
        "jobtitle": job_title or "",
        "outbound_platform": "Instantly",
        "latest_outbound_campaign": campaign_name,
        "latest_outbound_date": to_midnight_ms(latest_outbound_date),
        "has_responded": "true" if has_responded else "false",
        "reply_sentiment": sentiment_data["reply_sentiment"],
        "latest_response_text": latest_response_text,
        "latest_response_date": to_midnight_ms(latest_response_date),
        "latest_response_platform": "Instantly" if has_responded else "",
        "taken_off_list": sentiment_data["taken_off_list"],
        "is_postponed": sentiment_data["is_postponed"],
        "followup_date": followup_date,
        "sector": sector,
        "sentiment_notes": sentiment_data["sentiment_notes"],
        "response_count": str(len(inbound)),
    }


# ── HubSpot helpers ───────────────────────────────────────────────────────────

def hubspot_headers() -> dict:
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def batch_upsert_contacts(leads: list[dict]):
    """Upsert contacts into HubSpot keyed on email."""
    if not leads:
        return {"created": 0, "updated": 0, "errors": 0}

    results = {"created": 0, "updated": 0, "errors": 0}

    for i in range(0, len(leads), 100):
        batch = leads[i : i + 100]
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

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Instantly → HubSpot sync")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max number of campaigns to process (0 = all)")
    parser.add_argument("--max-leads", type=int, default=0,
                        help="Max number of leads to process per campaign (0 = all)")
    args = parser.parse_args()

    log.info("=== Instantly → HubSpot sync starting ===")

    if not INSTANTLY_API_KEY:
        log.error("INSTANTLY_API_KEY not set")
        return
    if not HUBSPOT_ACCESS_TOKEN:
        log.error("HUBSPOT_ACCESS_TOKEN not set")
        return

    # 1. Load last run state
    last_run = load_last_run()
    now = datetime.now(timezone.utc)
    if last_run:
        log.info(f"Incremental sync — pulling leads updated since {last_run}")
    else:
        log.info("First run — pulling all leads")

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
    total_leads_fetched = 0
    errors = []

    for campaign in campaigns:
        campaign_id = campaign.get("id")
        campaign_name = campaign.get("name", f"Campaign-{campaign_id}")
        log.info(f"Processing campaign: {campaign_name} (id={campaign_id})")

        try:
            leads = get_leads_for_campaign(campaign_id, max_leads=args.max_leads)
        except Exception as e:
            log.error(f"  Failed to fetch leads for '{campaign_name}': {e}")
            errors.append(f"Campaign '{campaign_name}': {e}")
            continue

        log.info(f"  Found {len(leads)} leads")
        total_leads_fetched += len(leads)

        for lead in leads:
            lead_email = lead.get("email", "")
            if not lead_email:
                continue

            # Check if lead has replies — only fetch emails if reply_count > 0
            reply_count = lead.get("email_reply_count", 0) or 0

            emails = []
            if reply_count > 0:
                try:
                    emails = get_emails_for_lead(lead_email, campaign_id)
                except Exception as e:
                    log.warning(f"  Failed to fetch emails for {lead_email}: {e}")

            try:
                lead_data = extract_lead_data(lead, emails, campaign_name, sector_cache)
                if lead_data:
                    all_leads.append(lead_data)
                    log.info(f"  Extracted lead: {lead_data['firstname']} {lead_data['lastname']} ({lead_data['email']})")

                    if lead_data["is_postponed"] == "true":
                        postponed_leads.append(lead_data)

            except Exception as e:
                log.error(f"  Failed to process lead {lead_email}: {e}")
                errors.append(f"Lead {lead_email}: {e}")

        time.sleep(0.2)

    # Deduplicate by email
    seen = {}
    for lead in all_leads:
        key = lead.get("email", "").strip().lower()
        if not key:
            continue

        if key not in seen:
            seen[key] = lead
        else:
            existing = seen[key]
            if lead["has_responded"] == "true" and existing["has_responded"] != "true":
                seen[key] = lead
            elif (lead["has_responded"] == "true" and existing["has_responded"] == "true"
                  and lead.get("latest_response_date", "") > existing.get("latest_response_date", "")):
                seen[key] = lead

    all_leads = list(seen.values())
    log.info(f"Total unique leads to sync: {len(all_leads)}")

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
    log.info(f"  Leads fetched:         {total_leads_fetched}")
    log.info(f"  Unique leads to sync:  {len(all_leads)}")
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
