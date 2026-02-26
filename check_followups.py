"""
Postponed Reply Follow-Up Checker
==================================

Queries HubSpot for contacts where is_postponed=true AND followup_date <= today,
sends a Slack reminder for each, then sets is_postponed to "false" so we don't
re-notify.

Run daily (cron or manually):
  python check_followups.py

Requires .env:
  HUBSPOT_ACCESS_TOKEN=...
  SLACK_TOFU_REPLIES_WEBHOOK_URL=...
"""

import os
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
SLACK_TOFU_REPLIES_WEBHOOK_URL = os.getenv("SLACK_TOFU_REPLIES_WEBHOOK_URL")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


def hubspot_headers() -> dict:
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def search_postponed_contacts() -> list[dict]:
    """Find contacts where is_postponed=true AND followup_date <= today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # HubSpot date properties are stored as midnight UTC ms timestamps.
    # Convert today's date to ms for the LTE filter.
    today_dt = datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    today_ms = str(int(today_dt.timestamp() * 1000))

    contacts = []
    after = None

    while True:
        body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "is_postponed",
                            "operator": "EQ",
                            "value": "true",
                        },
                        {
                            "propertyName": "followup_date",
                            "operator": "LTE",
                            "value": today_ms,
                        },
                    ]
                }
            ],
            "properties": [
                "firstname",
                "lastname",
                "email",
                "company",
                "jobtitle",
                "linkedin",
                "latest_outbound_campaign",
                "latest_response_text",
                "followup_date",
                "outbound_platform",
            ],
            "limit": 100,
        }
        if after:
            body["after"] = after

        resp = requests.post(
            f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
            headers=hubspot_headers(),
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        for result in data.get("results", []):
            contacts.append(result)

        paging = data.get("paging", {})
        next_page = paging.get("next", {})
        after = next_page.get("after")
        if not after:
            break

    return contacts


def send_followup_slack(contact: dict):
    """Send a Slack reminder for a contact whose follow-up date has arrived."""
    if not SLACK_TOFU_REPLIES_WEBHOOK_URL:
        log.warning("SLACK_TOFU_REPLIES_WEBHOOK_URL not set — skipping")
        return

    props = contact.get("properties", {})
    name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or "Unknown"
    company = props.get("company", "") or ""
    campaign = props.get("latest_outbound_campaign", "") or ""
    reply = props.get("latest_response_text", "") or ""
    email = props.get("email", "") or ""
    linkedin = props.get("linkedin", "") or ""
    platform = props.get("outbound_platform", "") or ""
    followup_date = props.get("followup_date", "") or ""

    # Format followup_date from ms timestamp to readable date
    if followup_date:
        try:
            dt = datetime.fromtimestamp(int(followup_date) / 1000, tz=timezone.utc)
            followup_date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    if len(reply) > 300:
        reply = reply[:300] + "..."

    contact_id = contact.get("id", "")
    contact_line = f"*Email:* {email}" if email else f"*LinkedIn:* {linkedin}"

    message = {
        "text": (
            f":alarm_clock: *Time to Follow Up!*\n"
            f"*Lead:* {name}"
            + (f" at {company}" if company else "")
            + f"\n*Platform:* {platform}\n"
            f"*Campaign:* {campaign}\n"
            f"{contact_line}\n"
            f"*Original reply:* _{reply}_\n"
            f"*Follow-up date:* {followup_date}\n"
            f"*HubSpot:* https://app.hubspot.com/contacts/{contact_id}"
        )
    }

    try:
        resp = requests.post(SLACK_TOFU_REPLIES_WEBHOOK_URL, json=message)
        resp.raise_for_status()
        log.info(f"  Slack reminder sent for {name}")
    except Exception as e:
        log.error(f"  Slack reminder failed for {name}: {e}")


def clear_postponed_flag(contact_id: str):
    """Set is_postponed to 'false' so we don't re-notify."""
    try:
        resp = requests.patch(
            f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}",
            headers=hubspot_headers(),
            json={"properties": {"is_postponed": "false"}},
        )
        resp.raise_for_status()
    except Exception as e:
        log.error(f"  Failed to clear is_postponed for contact {contact_id}: {e}")


def main():
    log.info("=== Checking for postponed follow-ups ===")

    if not HUBSPOT_ACCESS_TOKEN:
        log.error("HUBSPOT_ACCESS_TOKEN not set")
        return

    contacts = search_postponed_contacts()
    log.info(f"Found {len(contacts)} contacts due for follow-up")

    if not contacts:
        log.info("Nothing to do.")
        return

    sent = 0
    for contact in contacts:
        props = contact.get("properties", {})
        name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
        log.info(f"  Processing: {name} (id={contact.get('id')})")

        send_followup_slack(contact)
        clear_postponed_flag(contact["id"])
        sent += 1

    log.info(f"=== Done — sent {sent} follow-up reminders ===")


if __name__ == "__main__":
    main()
