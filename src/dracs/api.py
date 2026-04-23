import os
from datetime import datetime
from typing import Optional, Tuple

import requests

from dracs.exceptions import APIError, ValidationError


def dell_api_warranty_date(svctag: Optional[str]) -> Tuple[int, str]:
    """
    Authenticates with Dell's OAuth2 API and fetches the latest warranty
    expiration date for a given service tag. Returns a tuple of (epoch, string).
    """
    if svctag is None:
        raise ValidationError("Service tag parameter is required")

    # Your credentials from TechDirect
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    if not CLIENT_ID or not CLIENT_SECRET:
        raise APIError(
            "Dell API credentials not found! "
            "Please set CLIENT_ID and CLIENT_SECRET in your .env file. "
            "Visit https://techdirect.dell.com to obtain API credentials"
        )

    # Verify current URL in TechDirect docs
    TOKEN_URL = (
        "https://apigtwb2c.us.dell.com/auth/oauth/v2/token"
    )

    # Fetch the token
    auth_response = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
    )

    token = auth_response.json().get("access_token")

    WARRANTY_API_URL = (
        "https://apigtwb2c.us.dell.com/PROD/sbil/eapi/v5/asset-entitlements"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {"servicetags": [svctag]}

    response = requests.get(WARRANTY_API_URL, headers=headers, params=payload)

    if response.status_code == 200:
        warranty_data = response.json()
    else:
        raise APIError(
            f"Dell API request failed: {response.status_code} - {response.text}"
        )

    for s in warranty_data:
        svctag = s["serviceTag"]
        entitlements = s["entitlements"]

    cur_eed = 0
    cur_eed_string = "January 1, 1970"
    for e in entitlements:
        eed = e["endDate"]
        eed_dt = datetime.fromisoformat(eed.replace("Z", "+00:00"))
        eed_dt_epoch = int(eed_dt.strftime("%s"))
        eed_dt_string = eed_dt.strftime("%B %e, %Y")
        if eed_dt_epoch > cur_eed:
            cur_eed = eed_dt_epoch
            cur_eed_string = eed_dt_string

    return (cur_eed, cur_eed_string)
