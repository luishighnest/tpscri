"""Gestione token sessione LBA TV.

Rinnova il Bearer token SOLO quando sta per scadere (<= 120s) e in modo
thread-safe. Prima veniva rinnovato ad OGNI chiamata: con il worker
automatico e le richieste concorrenti questo causava una tempesta di
/api/v1/init, rotazione continua del token e callback di playback in 401.
"""

import base64
import json
import threading
import time
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SESSION_FILE = BASE_DIR / "lba_session.json"
API_BASE_URL = "https://dce-frontoffice.imggaming.com"
API_KEY = "857a1e5d-e35e-4fdf-805b-a87b6f8364bf"
REALM = "dce.lba"

# Margine di sicurezza: rinnova se mancano meno di 30 secondi.
# (Un margine troppo alto faceva ripartire il refresh a raffica negli ultimi
# minuti di vita del token: il server, finche' il token e' valido, restituisce
# lo stesso token e ogni thread ritentava.)
_REFRESH_MARGIN = 30
_REFRESH_COOLDOWN = 15  # secondi minimi tra due tentativi di refresh
_REFRESH_LOCK = threading.Lock()
_LAST_REFRESH = 0.0


def _jwt_exp(token: str) -> int:
    """Legge il campo `exp` da un JWT (senza verificare la firma)."""
    try:
        part = str(token).split(".")[1]
        part += "=" * ((4 - len(part) % 4) % 4)
        payload = json.loads(base64.urlsafe_b64decode(part.encode("ascii")))
        return int(payload.get("exp", 0) or 0)
    except Exception:
        return 0


def _load_session() -> dict:
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _token_valid(sess: dict) -> bool:
    """True se il token esiste e scade tra più di _REFRESH_MARGIN secondi."""
    tok = (sess or {}).get("authorisationToken", "")
    if not tok:
        return False
    # Alcuni salvataggi usano il formato "Mixed <jwt> <refresh>"
    candidate = tok.split()[1] if tok.startswith("Mixed ") else tok
    exp = _jwt_exp(candidate) or int((sess or {}).get("exp", 0) or 0)
    if not exp:
        # Nessuna scadenza leggibile: lo consideriamo valido per non martellare l'API.
        return True
    return int(time.time()) < (exp - _REFRESH_MARGIN)


def _do_refresh(sess: dict) -> str:
    auth_tok = sess.get("authorisationToken", "")
    ref_tok = sess.get("refreshToken", "")
    mixed = auth_tok if auth_tok.startswith("Mixed") else f"Mixed {auth_tok} {ref_tok}".strip()

    url = f"{API_BASE_URL}/api/v1/init/?readUserProfiles=true"
    headers = {
        "accept": "application/json, text/plain, */*",
        "app": "dice",
        "authorization": mixed,
        "content-type": "application/json",
        "origin": "https://lbatv.com",
        "realm": REALM,
        "referer": "https://lbatv.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36 Edg/156.0.0.0"
        ),
        "x-api-key": API_KEY,
        "x-app-var": "6.60.0.1eb8551",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    auth_info = data.get("authentication", {}) or {}
    new_auth = auth_info.get("authorisationToken")
    new_ref = auth_info.get("refreshToken")
    if new_auth:
        sess["authorisationToken"] = new_auth
        if new_ref:
            sess["refreshToken"] = new_ref
        sess["saved_at"] = int(time.time())
        SESSION_FILE.write_text(json.dumps(sess, indent=2), encoding="utf-8")
        print("[LBA Token Refresh] Token rinnovato via init.")
        return new_auth
    return auth_tok


def refresh_token_se_necessario() -> str:
    """Ritorna il Bearer token corrente, rinnovandolo solo se sta per scadere."""
    global _LAST_REFRESH

    sess = _load_session()
    if not sess:
        return ""

    tok = sess.get("authorisationToken", "")
    if _token_valid(sess):
        return tok

    with _REFRESH_LOCK:
        # Ricontrolla: un altro thread potrebbe aver già rinnovato mentre attendevamo il lock.
        sess = _load_session()
        tok = sess.get("authorisationToken", "")
        if _token_valid(sess):
            return tok

        # Token GIA' scaduto: forza il refresh ignorando il cooldown, altrimenti
        # le chiamate API fallirebbero con "Bearer token is not valid".
        candidate = tok.split()[1] if tok.startswith("Mixed ") else tok
        exp = _jwt_exp(candidate) or int(sess.get("exp", 0) or 0)
        if exp and exp <= time.time():
            _LAST_REFRESH = time.time()
            try:
                return _do_refresh(sess)
            except Exception as e:
                print(f"[LBA Token Refresh Err] {e}")
                return tok

        # Cooldown: evita raffiche di /api/v1/init quando il server restituisce
        # ancora lo stesso token (perché è sì vicino alla scadenza ma non scaduto).
        if (time.time() - _LAST_REFRESH) < _REFRESH_COOLDOWN:
            return tok

        _LAST_REFRESH = time.time()
        try:
            return _do_refresh(sess)
        except Exception as e:
            print(f"[LBA Token Refresh Err] {e}")
            return tok
