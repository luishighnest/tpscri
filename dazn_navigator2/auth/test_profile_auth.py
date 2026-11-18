"""Login DAZN diretto per il solo profilo TEST.

Gli altri profili (pz8, prova, mpd) continuano a usare il login via browser
di dazn_navigator2.auth.login: questo modulo non li tocca.

Perche' serve una via diversa: il login diretto con email e password e' l'unico
che funziona senza aprire una finestra, ma puo' funzionare solo se la richiesta
ha l'identita' TLS di un browser vero. Con httpx o aiohttp, ClientHello
CloudFront davanti agli endpoint DAZN risponde "403 The request could not be
satisfied" anche con credenziali corrette; per questo qui si usa curl_cffi con
impersonate, lo stesso client che il resto di script2 gia' usa per le API.

Il token ottenuto e' un normale JWT DAZN: viene scritto nel profilo TEST negli
stessi file che il resto del codice gia' sa leggere (auth_token.json e
dazn_session.json), quindi non serve modificare l'estrattore.
"""

import base64
import json
import secrets
import time
from pathlib import Path

SIGNIN_URL = "https://authentication-prod.ar.indazn.com/v5/SignIn"

PROFILE_ID = "TEST"
PROFILE_DIR = Path(__file__).resolve().parent.parent.parent / "saved_profiles" / f"profile_{PROFILE_ID}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)

# Stessi header della richiesta che da' il via libera a CloudFront: senza
# sec-ch-ua e sec-fetch la risposta e' un 403 anche con la password giusta.
HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://www.dazn.com",
    "referer": "https://www.dazn.com/",
    "user-agent": USER_AGENT,
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="142", "Chromium";v="142"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
}


def profilo_abilitato(profile_id):
    """True solo se il profilo attivo e' quello TEST."""
    return str(profile_id or "") == PROFILE_ID


def _estrai_token(dati):
    """Il JWT dentro la risposta di SignIn, che puo' arrivare annidato."""
    if not isinstance(dati, dict):
        return ""
    for chiave in ("Token", "token", "access_token", "AccessToken"):
        valore = dati.get(chiave)
        if isinstance(valore, str) and valore.startswith("eyJ"):
            return valore
    for chiave in ("AuthToken", "Data", "Session"):
        annidato = dati.get(chiave)
        if isinstance(annidato, dict):
            trovato = _estrai_token(annidato)
            if trovato:
                return trovato
    return ""


def _claims(token):
    try:
        parte = str(token).split(".")[1]
        parte += "=" * ((4 - len(parte) % 4) % 4)
        return json.loads(base64.urlsafe_b64decode(parte.encode("ascii")))
    except Exception:
        return {}


def _spiega(dati, codice, testo):
    """Un motivo leggibile, per non stampare un JSON grezzo all'utente."""
    codice_errore = ""
    messaggio = ""
    if isinstance(dati, dict):
        errore = dati.get("odata.error") if isinstance(dati.get("odata.error"), dict) else dati
        if isinstance(errore, dict):
            codice_errore = str(errore.get("code") or dati.get("ErrorCode") or "")
            grezzo = errore.get("message") or dati.get("Message") or ""
            if isinstance(grezzo, dict):
                grezzo = grezzo.get("value") or ""
            messaggio = str(grezzo or "")
    if "cloudfront" in testo.lower() or "could not be satisfied" in testo.lower():
        return "Bloccato da CloudFront: l'identita' di rete non e' riconosciuta come un browser."
    if "invalidpassword" in (codice_errore + messaggio).lower():
        return "Password sbagliata."
    if "usernotfound" in (codice_errore + messaggio).lower() or "notfound" in (codice_errore + messaggio).lower():
        return "Email non trovata."
    if "too many" in (codice_errore + messaggio).lower():
        return "Troppi tentativi da questo indirizzo: riprova fra poco."
    if messaggio:
        return messaggio
    return "Accesso non riuscito (HTTP %s)." % codice


def _impersonate():
    """Stesso profilo TLS che usa il resto di script2, cosi' i due concordano."""
    try:
        from dazn_navigator2.services.extractor import get_impersonate
        return get_impersonate()
    except Exception:
        return "chrome142"


def accedi(email, password, timeout=30):
    """Accede a DAZN con email e password. Ritorna un dizionario di esito.

    Non stampa e non salva la password in nessun punto.
    """
    esito = {"ok": False, "jwt": "", "device_id": "", "error": ""}
    if not email or not password:
        esito["error"] = "Email o password mancanti."
        return esito
    try:
        from curl_cffi import requests as cr
    except ImportError:
        esito["error"] = "Manca curl_cffi, necessario per il login diretto."
        return esito

    device_id = secrets.token_hex(5)
    corpo = {"Email": email, "Password": password, "Platform": "web", "DeviceId": device_id}
    try:
        risposta = cr.post(SIGNIN_URL, headers=HEADERS, json=corpo,
                           impersonate=_impersonate(), timeout=timeout)
    except Exception as e:
        esito["error"] = "Connessione a DAZN non riuscita: %s" % e
        return esito

    testo = ""
    try:
        testo = risposta.text or ""
    except Exception:
        testo = ""
    dati = {}
    try:
        dati = json.loads(testo) if testo else {}
    except Exception:
        dati = {}

    if risposta.status_code != 200:
        esito["error"] = _spiega(dati, risposta.status_code, testo)
        return esito

    token = _estrai_token(dati)
    if not token:
        esito["error"] = "Accesso non riuscito: DAZN non ha restituito un token."
        return esito

    esito.update(ok=True, jwt=token, device_id=device_id)
    return esito


def _scrivi(nome_file, contenuto):
    try:
        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        (PROFILE_DIR / nome_file).write_text(
            json.dumps(contenuto, ensure_ascii=False), encoding="utf-8")
        return True
    except OSError:
        return False


def salva_sessione(jwt, device_id=""):
    """Scrive il token dove il resto di script2 lo cerca gia'."""
    if not jwt or not str(jwt).startswith("eyJ"):
        return False
    contenuto = {"jwt": jwt, "device_id": device_id or ""}
    return _scrivi("auth_token.json", contenuto) and _scrivi("dazn_session.json", contenuto)


def stato_sessione():
    """Il token del profilo TEST con quello che resta di validita'."""
    info = {"presente": False, "valido": False, "resto": 0, "jwt": "", "paese": "", "device_id": ""}
    for nome_file in ("auth_token.json", "dazn_session.json"):
        percorso = PROFILE_DIR / nome_file
        if not percorso.exists():
            continue
        try:
            dati = json.loads(percorso.read_text(encoding="utf-8"))
        except Exception:
            continue
        token = str(dati.get("jwt") or "")
        if not token.startswith("eyJ"):
            continue
        info["presente"] = True
        info["jwt"] = token
        info["device_id"] = str(dati.get("device_id") or "")
        scadenza = _claims(token).get("exp") or 0
        info["resto"] = max(0, int(scadenza) - int(time.time()))
        info["valido"] = info["resto"] > 0
        info["paese"] = str(_claims(token).get("contentCountry") or _claims(token).get("country") or "")
        break
    return info


def cancella_sessione():
    rimossi = 0
    for nome_file in ("auth_token.json", "dazn_session.json"):
        try:
            percorso = PROFILE_DIR / nome_file
            if percorso.exists():
                percorso.unlink()
                rimossi += 1
        except OSError:
            continue
    return rimossi
