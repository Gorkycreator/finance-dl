"""
Example Usage from commandline:

$ python ./finance_dl/simplefin_access_token_setup.py <access token>
"""

import requests
import base64
import argparse
from urllib.parse import urlparse
import logging
import keyring
import getpass


logger = logging.getLogger('simplefin_dl_setup')


APP_NAME = "Finance-dl"
USERNAME = "SimpleFin-api"


def exchange_setup_token(setup_token):
    """
    Decodes the Setup Token and posts to the resulting Claim URL to get the Access URL.
    """

    logger.info("Starting SimpleFIN token exchange...")

    try: 
        claim_url = base64.b64decode(setup_token).decode('utf-8')
    except:
        raise ValueError(f"Token decoding failed for the following: {setup_token}")
    
    response = requests.post(
        claim_url
    )
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        msg = "The token has already been claimed or is invalid."
        inform_user_about_response_codes(e, logger, msg)
        raise
    
    access_url = response.text.strip()

    if not urlparse(access_url).netloc:
        raise ValueError(f"Invalid Access URL response: {access_url}")
    
    return access_url


def store_access_url(access_url):
    keyring.set_password(APP_NAME, USERNAME, access_url)


def get_access_url():
    return keyring.get_password(APP_NAME, USERNAME)


def auto_setup():
    stored_password = get_access_url()
    if not stored_password:
        logger.warning("--- First time setup of SimpleFin ---\n" \
            "Please obtain a setup token from https://beta-bridge.simplefin.org")
        token = getpass.getpass(prompt="Paste here and hit Enter (no text shown):")
        access_url = exchange_setup_token(token)
        store_access_url(access_url)
        logger.info("Success, access url has been stored!")
        stored_password = access_url
    return stored_password


def inform_user_about_response_codes(error, log, hint):
    if error.response.status_code == 403:
        log.error(f"Error 403: {hint}")
    elif error.response.status_code == 402:
        log.error("Error 402: Simplefin payment required.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Exchange a SimpleFIN Setup Token for a permanent Access URL."
    )

    parser.add_argument(
        'setup_token',
        type=str,
        help="The one-time-use Base64-encoded SimpleFIN setup token."
    )

    args = parser.parse_args()

    access_url = exchange_setup_token(args.setup_token)
    print(access_url)