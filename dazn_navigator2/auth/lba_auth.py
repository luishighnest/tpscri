"""Modulo di autenticazione e gestione sessione per LBA TV (Endeavor Streaming / DCE)."""

import base64
import json
import time
from pathlib import Path
import urllib.request
import urllib.error

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SESSION_FILE = BASE_DIR / "lba_session.json"

API_BASE_URL = "https://dce-frontoffice.imggaming.com"
API_KEY = "857a1e5d-e35e-4fdf-805b-a87b6f8364bf"
REALM = "dce.lba"

HEADERS = {
    "x-api-key": API_KEY,
    "realm": REALM,
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
}


def _decode_jwt(token: str) -> dict:
    try:
        part = str(token).split(".")[1]
        part += "=" * ((4 - len(part) % 4) % 4)
        return json.loads(base64.urlsafe_b64decode(part.encode("ascii")))
    except Exception:
        return {}


def carica_sessione() -> dict:
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def salva_sessione(auth_token: str, refresh_token: str = "", extra_data: dict = None) -> bool:
    claims = _decode_jwt(auth_token)
    dati = {
        "authorisationToken": auth_token,
        "refreshToken": refresh_token,
        "exp": claims.get("exp", 0),
        "iat": claims.get("iat", 0),
        "aid": claims.get("aid", ""),
        "saved_at": int(time.time()),
        "extra": extra_data or {}
    }
    try:
        SESSION_FILE.write_text(json.dumps(dati, indent=2), encoding="utf-8")
        return True
    except Exception as e:
        print(f"[LBA Auth] Errore salvataggio sessione: {e}")
        return False


def cancella_sessione() -> bool:
    if SESSION_FILE.exists():
        try:
            SESSION_FILE.unlink()
            return True
        except Exception:
            return False
    return True


def stato_sessione() -> dict:
    sess = carica_sessione()
    if not sess or not sess.get("authorisationToken"):
        return {"presente": False, "valido": False, "resto": 0, "email": ""}
    
    exp = sess.get("exp", 0)
    now = int(time.time())
    resto = exp - now
    valido = resto > 30
    aid = sess.get("aid", "")
    
    return {
        "presente": True,
        "valido": valido,
        "resto": max(0, resto),
        "email": aid.split("|")[0] if "|" in aid else aid
    }


def accedi(email: str, password: str, timeout: int = 15) -> tuple[bool, str]:
    if not email or not password:
        return False, "Inserisci email e password."
    
    url = f"{API_BASE_URL}/api/v2/login"
    payload = json.dumps({"id": email.strip(), "secret": password}).encode("utf-8")
    
    req = urllib.request.Request(url, data=payload, headers=HEADERS, method="POST")
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            auth_token = data.get("authorisationToken")
            refresh_token = data.get("refreshToken", "")
            
            if not auth_token:
                return False, "Risposta di login priva di token di autorizzazione."
            
            salva_sessione(auth_token, refresh_token, data)
            return True, "Login effettuato con successo!"
            
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        try:
            err_json = json.loads(body)
            msgs = err_json.get("messages", [])
            msg_str = " ".join(msgs).lower()
            if "failedauthentication" in msg_str or e.code == 404:
                return False, "Credenziali non corrette (email o password errata)."
            if e.code == 403:
                return False, "Accesso rifiutato o blocco geografico/IP da parte di LBA."
            return False, f"Errore server ({e.code}): {', '.join(msgs) if msgs else body}"
        except Exception:
            return False, f"Errore HTTP {e.code} durante il login."
    except Exception as e:
        return False, f"Errore di connessione: {e}"
