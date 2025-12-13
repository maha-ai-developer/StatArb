# broker/kite_auth.py

import json
import os
from kiteconnect import KiteConnect, KiteTicker

CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")
SESSION_PATH = os.path.join(CONFIG_DIR, "kite_session.json")


# ---------- Config helpers ----------

def _load_raw_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def _save_raw_config(cfg):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def _get_kite_section(cfg):
    """
    Support both:
      {
        "api_key": "...",
        "api_secret": "...",
        "access_token": "..."
      }
    and:
      {
        "kite": {
          "api_key": "...",
          "api_secret": "...",
          "access_token": "..."
        }
      }
    Returns (section_dict, root_cfg, key_in_root).
    """
    if "kite" in cfg and isinstance(cfg["kite"], dict):
        return cfg["kite"], cfg, "kite"
    else:
        # assume flat layout
        return cfg, cfg, None


def load_config():
    """Return (api_key, api_secret, access_token or None)."""
    cfg = _load_raw_config()
    section, _root, _key = _get_kite_section(cfg)

    api_key = section.get("api_key")
    api_secret = section.get("api_secret")
    access_token = section.get("access_token")

    if not api_key or not api_secret:
        raise ValueError("api_key/api_secret missing in config/config.json")

    return api_key, api_secret, access_token


def save_access_token(access_token: str):
    """Save access_token into config.json AND kite_session.json."""
    cfg = _load_raw_config()
    section, root, key = _get_kite_section(cfg)

    section["access_token"] = access_token
    if key is not None:
        root[key] = section
    else:
        root = section

    _save_raw_config(root)

    # Also store in kite_session.json for debugging / external tools
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(SESSION_PATH, "w") as f:
        json.dump({"access_token": access_token}, f, indent=2)


# ---------- Core Kite clients ----------

def get_kite() -> KiteConnect:
    """
    Main entrypoint for REST API usage.

    Reads api_key/api_secret/access_token from config/config.json.
    """
    api_key, api_secret, access_token = load_config()
    if not access_token:
        raise RuntimeError(
            "No access_token found in config. "
            "Run the login + token flow first."
        )

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


# Some previous code versions used this name:
def get_kite_client() -> KiteConnect:
    return get_kite()


def get_kite_ticker() -> KiteTicker:
    """
    WebSocket ticker helper.

    Uses same api_key + access_token as get_kite().
    """
    api_key, _api_secret, access_token = load_config()
    if not access_token:
        raise RuntimeError(
            "No access_token found in config. "
            "Run the login + token flow first."
        )
    return KiteTicker(api_key, access_token)


# ---------- Login helpers ----------

def generate_login_url() -> str:
    """
    Generate Kite login URL using api_key from config.json.
    """
    api_key, _api_secret, _access_token = load_config()
    kite = KiteConnect(api_key=api_key)
    return kite.login_url()


# some older code called this instead
def login_url() -> str:
    return generate_login_url()


def exchange_request_token(request_token: str) -> str:
    """
    Exchange the request_token (from redirect URL) for access_token.
    Persist the access_token in config/config.json and kite_session.json.

    Returns the access_token.
    """
    api_key, api_secret, _old_token = load_config()
    kite = KiteConnect(api_key=api_key)

    print("[AUTH] Exchanging request_token for access_token…")
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]

    save_access_token(access_token)
    print("[AUTH] Saved new access_token.")
    print("[AUTH] Login successful!")

    return access_token


# Legacy helper – if some module calls login_user(), just show URL
def login_user():
    url = generate_login_url()
    print("[AUTH] Login URL:")
    print(url)
    print("\nAfter logging in, copy request_token from redirect URL and run:\n")
    print("  python cli.py token <request_token>")
