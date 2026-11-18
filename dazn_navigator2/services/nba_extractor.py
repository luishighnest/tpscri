"""Estrattore eventi e stream NBA League Pass (HLS + Widevine ClearKey).

Pattern identico a lba_extractor.py:
  - get_catalog()         -> catalogo live/upcoming/vod da API NBA
  - estrai_stream_e_chiave(media_id, is_vod) -> (mpd_url, kid:key)
  - estrai_eventi(tipo)   -> batch su tutto il catalogo
"""

import base64
import json
import re
import threading
import time
import uuid
from pathlib import Path
import urllib.request
import urllib.parse
import urllib.error

# curl_cffi impersona TLS Chrome → supera il bot-check di cdn.nba.com
try:
    from curl_cffi import requests as _cf
    _HAS_CURL_CFFI = True
except ImportError:
    _HAS_CURL_CFFI = False


def _cf_get(url: str, headers: dict = None, timeout: int = 12) -> tuple[int, bytes]:
    """GET con curl_cffi (Chrome TLS fingerprint). Fallback su urllib."""
    h = headers or {}
    if _HAS_CURL_CFFI:
        try:
            r = _cf.get(url, impersonate="chrome131", headers=h, timeout=timeout)
            return r.status_code, r.content
        except Exception as e:
            print(f"[curl_cffi err] {e}")
    # fallback urllib
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception:
        return 0, b""

BASE_DIR = Path(__file__).resolve().parent.parent.parent
WVD_PATH = BASE_DIR / "device.wvd"
SESSION_FILE = BASE_DIR / "nba_session.json"

# ── Costanti NBA ─────────────────────────────────────────────────────────────
NBA_API_BASE        = "https://content-api-prod.nba.com"
NBA_SCHEDULE_URL    = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_15.json"
NBA_LIVE_URL        = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_LICENSE_URL     = "https://ottapp-appgw-amp.nba.com/v1/client/get-widevine-license"
NBA_STREAM_API      = "https://ottapp-appgw-amp.nba.com/v1/channel"
NBA_SESSION_ID_SEED = "nba_script2"

APPLICATION_TOKEN   = "201822"
AZUKI_IMC           = "IMC7.5.0_AN_D3.0.0_S0"

# UUID fisso del dispositivo - deve combaciare con deviceprofile e token STS
DEVICE_UUID = "f97f36120b4d4627a66daca6f153fb16"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

def _make_deviceprofile_b64(device_uuid: str = DEVICE_UUID) -> str:
    """Genera il DEVICEPROFILE_B64 con il deviceUUID corretto."""
    import json as _json
    profile = {
        "model": "Desktop",
        "osVersion": "10",
        "vendorName": "Microsoft",
        "osName": "HTML5",
        "supportsHevcDecode": False,
        "wvLevel": "L3",
        "deviceUUID": device_uuid,
    }
    return base64.b64encode(_json.dumps(profile, separators=(",", ":")).encode()).decode()

DEVICEPROFILE_B64 = _make_deviceprofile_b64()

LOCATION_B64 = "eyJPb3N0YWxDb2RlIjoiODAxMzMiLCJDb3VudHJ5Q29kZSI6IklUIn0="

WIDEVINE_SYSTEM_ID = bytes.fromhex("edef8ba979d64acea3c827dcd51d21ed")

# ── Rate limiter ──────────────────────────────────────────────────────────────
_EXTRACT_LOCK = threading.Lock()
_RATE_LOCK    = threading.Lock()
_RATE_LAST    = 0.0
_THROTTLE_GAP = 2.0

# Cache chiavi (kid:key durano ore)
_KEY_CACHE: dict = {}   # media_id -> (key, pssh, timestamp)
_KEY_TTL  = 6 * 3600

# Cache catalogo
_CATALOG_CACHE      = None
_CATALOG_CACHE_TIME = 0.0
_CATALOG_TTL        = 60  # secondi


def _throttle(gap: float | None = None) -> None:
    global _RATE_LAST
    g = _THROTTLE_GAP if gap is None else gap
    with _RATE_LOCK:
        now  = time.time()
        wait = _RATE_LAST + g - now
        if wait > 0:
            time.sleep(wait)
        _RATE_LAST = time.time()


def _get_session() -> dict:
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _auth_token_valido(sess: dict) -> bool:
    """Controlla se l'AuthToken1 è ancora valido (almeno 5 min rimanenti)."""
    expires_on = sess.get("auth_expires_on", 0)
    if not expires_on:
        saved = sess.get("saved_at", 0)
        expires_on = saved + 7 * 86400
    return expires_on > int(time.time()) + 300


def _get_auth_token() -> str:
    """Ritorna un AuthToken1 valido, rinnovandolo automaticamente se scaduto."""
    sess = _get_session()
    if not sess:
        return ""

    # Rinnova se scaduto
    if not _auth_token_valido(sess):
        print("[NBA Extractor] AuthToken1 scaduto, rinnovo automatico...")
        try:
            from dazn_navigator2.auth.nba_auth import aggiorna_auth_token
            ok, msg = aggiorna_auth_token()
            print(f"[NBA Extractor] Rinnovo token: {msg}")
            if ok:
                sess = _get_session()  # rileggi sessione aggiornata
        except Exception as e:
            print(f"[NBA Extractor] Errore rinnovo token: {e}")

    return sess.get("authorizationToken", "")


def _new_session_id() -> str:
    return str(uuid.uuid4())


# ── Catalogo ──────────────────────────────────────────────────────────────────

def _fetch_scoreboard() -> list:
    """Partite di oggi con status live/finito (usa curl_cffi per bypassare bot-check CDN NBA)."""
    h = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
    }
    status, body = _cf_get(NBA_LIVE_URL, headers=h, timeout=12)
    if status == 200:
        try:
            data = json.loads(body.decode("utf-8"))
            return data.get("scoreboard", {}).get("games", [])
        except Exception as e:
            print(f"[NBA Scoreboard Parse Err] {e}")
    else:
        print(f"[NBA Scoreboard HTTP {status}]")
    return []


def _fetch_schedule() -> list:
    """Scarica il calendario completo NBA (CDN statica)."""
    h = {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com/",
    }
    status, body = _cf_get(NBA_SCHEDULE_URL, headers=h, timeout=20)
    if status == 200:
        try:
            data = json.loads(body.decode("utf-8"))
            return data.get("leagueSchedule", {}).get("gameDates", [])
        except Exception as e:
            print(f"[NBA Schedule Parse Err] {e}")
    else:
        print(f"[NBA Schedule HTTP {status}]")
    return []


def get_catalog() -> dict:
    """
    Restituisce catalogo NBA diviso in live / upcoming / vod.
    - live:     partite di oggi in corso
    - upcoming: partite di oggi non ancora iniziate
    - vod:      ultime 2 giornate di partite già finite
    Include sempre NBA TV Live come canale live speciale.
    """
    global _CATALOG_CACHE, _CATALOG_CACHE_TIME
    if _CATALOG_CACHE and (time.time() - _CATALOG_CACHE_TIME) < _CATALOG_TTL:
        return _CATALOG_CACHE

    res = {"live": [], "upcoming": [], "vod": [], "total": 0}
    nba_logo = "https://cdn.nba.com/logos/leagues/L/logo-nba.svg"

    # ── NBA TV Live (canale sempre presente se autenticato) ───────────────────
    res["live"].append({
        "id"    : "NBATVI",
        "title" : "NBA TV Live",
        "type"  : "LIVE",
        "bucket": "NBA TV",
        "image" : nba_logo,
        "start" : "",
        "is_vod": False,
        "media_id": "NBATVI",
    })

    # ── Scoreboard oggi ───────────────────────────────────────────────────────
    today_games = _fetch_scoreboard()
    today_ids   = set()

    # Ottieni la data di oggi in formato NBA (YYYYMMDD)
    today_str = time.strftime("%Y%m%d")

    for g in today_games:
        gid   = g.get("gameId", "")
        hteam = g.get("homeTeam", {}).get("teamName", "")
        ateam = g.get("awayTeam", {}).get("teamName", "")
        hcity = g.get("homeTeam", {}).get("teamCity", "")
        acity = g.get("awayTeam", {}).get("teamCity", "")
        title = f"{acity} {ateam} vs {hcity} {hteam}"
        status_num = g.get("gameStatusText", "")
        gstatus    = g.get("gameStatus", 1)  # 1=pre, 2=live, 3=final

        # media_id formato NBA LP: g<stagione><gid_senza_stagione><home_abbr><seq><away_abbr>
        # Usiamo gameId direttamente come media_id per l'API LP
        # Il formato reale: g0022600034nyk1000467ind (da conversazione precedente)
        # Lo costruiamo dal gameId NBA quando possibile
        media_id = _build_media_id(g)

        entry = {
            "id"      : gid,
            "title"   : title,
            "type"    : "LIVE" if gstatus == 2 else ("UPCOMING" if gstatus == 1 else "VOD"),
            "bucket"  : "NBA League Pass",
            "image"   : nba_logo,
            "start"   : g.get("gameTimeUTC", ""),
            "is_vod"  : gstatus == 3,
            "media_id": media_id,
            "status"  : status_num,
        }

        today_ids.add(gid)

        if gstatus == 2:
            res["live"].append(entry)
        elif gstatus == 1:
            res["upcoming"].append(entry)
        else:
            res["vod"].append(entry)

    # ── Ultime 2 giornate dal calendario (VOD recenti) ────────────────────────
    if len(res["vod"]) == 0:
        game_dates = _fetch_schedule()
        # Prendi le ultime date con partite
        recent = []
        for gd in reversed(game_dates):
            date_games = gd.get("games", [])
            if not date_games:
                continue
            # Includi solo date passate
            date_str = gd.get("gameDate", "")[:10].replace("-", "")
            if date_str and date_str < today_str:
                for g in date_games:
                    gid  = g.get("gameId", "")
                    if gid in today_ids:
                        continue
                    hteam = g.get("homeTeam", {}).get("teamName", "")
                    ateam = g.get("awayTeam", {}).get("teamName", "")
                    hcity = g.get("homeTeam", {}).get("teamCity", "")
                    acity = g.get("awayTeam", {}).get("teamCity", "")
                    title = f"{acity} {ateam} vs {hcity} {hteam} (VOD)"
                    media_id = _build_media_id(g)
                    res["vod"].append({
                        "id"      : gid,
                        "title"   : title,
                        "type"    : "VOD",
                        "bucket"  : "NBA League Pass VOD",
                        "image"   : nba_logo,
                        "start"   : g.get("gameDateTimeUTC", ""),
                        "is_vod"  : True,
                        "media_id": media_id,
                    })
                recent.append(gd.get("gameDate", ""))
                if len(recent) >= 2:
                    break

    res["total"] = len(res["live"]) + len(res["upcoming"]) + len(res["vod"])
    _CATALOG_CACHE      = res
    _CATALOG_CACHE_TIME = time.time()
    return res


def _build_media_id(game: dict) -> str:
    """
    Costruisce il mediaId NBA LP dal dizionario partita NBA API.
    Formato osservato: g0022600034nyk1000467ind
    = g + stagione(5) + seq(4) + homeAbbr(3) + seq2(7) + awayAbbr(3)

    Dato che l'API NBA non espone direttamente il mediaId LP,
    usiamo il gameId grezzo (10 cifre) come fallback: l'extractor
    lo usa per costruire l'URL LP corretto.

    Il mediaId vero viene estratto dal manifest M3U8 durante l'estrazione.
    """
    gid = game.get("gameId", "")
    # Il gameId NBA è tipo "0022600034" (stagione 00226, game 00034)
    # La serie LP usa: g + gameId + homeAbbr + seq + awayAbbr
    # Per ora passiamo il gameId: l'extractor proverà l'API LP con esso
    home_abbr = (game.get("homeTeam", {}).get("teamTricode") or "").lower()
    away_abbr = (game.get("awayTeam", {}).get("teamTricode") or "").lower()
    if gid and home_abbr and away_abbr:
        # Formato usato dal sito NBA: g + gameId + homeAbbr + idx + awayAbbr
        # Il numero nell'idx (es. 1000467) non è prevedibile, ma LP accetta anche solo gameId
        return f"g{gid}{home_abbr}"
    return gid or "NBATVI"


# ── Estrazione stream ─────────────────────────────────────────────────────────

def _resolve_vod_media_id(game_id: str, auth_token: str) -> tuple[str, str]:
    """
    Risolve il vero mediaId VOD per una partita NBA interrogando l'API NBA content.
    Ritorna (media_id_reale, errore).

    Il sito NBA usa questo pattern per trovare l'asset ID del replay:
    GET https://content-api-prod.nba.com/public/1/games/{gameId}
    La risposta contiene 'broadcastId' o 'mediaId' con il vero ID per Akamai LP.
    """
    # Prova l'API NBA Games Content
    game_id_clean = game_id.lstrip("g")  # rimuovi eventuale 'g' iniziale

    # API 1: content API
    for api_url in [
        f"https://content-api-prod.nba.com/public/1/games/{game_id_clean}",
        f"https://cdn.nba.com/static/json/liveData/boxscore/bs_{game_id_clean}.json",
    ]:
        h = {
            "Accept": "application/json",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
        }
        status, body = _cf_get(api_url, headers=h, timeout=10)
        if status == 200:
            try:
                data = json.loads(body.decode("utf-8"))
                # Cerca broadcastId o mediaId nei vari percorsi della risposta
                for path in [
                    ["video", "broadcastId"],
                    ["video", "mediaId"],
                    ["data", "video", "broadcastId"],
                    ["data", "broadcastId"],
                    ["broadcastId"],
                    ["mediaId"],
                ]:
                    val = data
                    for key in path:
                        val = val.get(key, {}) if isinstance(val, dict) else None
                        if not val:
                            break
                    if val and isinstance(val, str) and len(val) > 5:
                        print(f"[NBA VOD] Trovato mediaId da API: {val}")
                        return val, ""
            except Exception as e:
                print(f"[NBA VOD] Parse error {api_url}: {e}")

    # API 2: Schedule content - cerca l'asset nei dati di programmazione
    h2 = {
        "Accept": "application/json",
        "Referer": "https://www.nba.com/",
        "authorizationtoken": auth_token,
    }
    # Prova endpoint LP direttamente con gameId varianti
    for media_variant in [
        game_id_clean,                    # es. 0022600232
        f"g{game_id_clean}",              # es. g0022600232
    ]:
        for ep_type in ("vod", "live"):
            prefix = "vod-pz/p" if ep_type == "vod" else "live-pz/p"
            test_url = f"https://lpnba.akamaized.net/{prefix}/hls-wvpr/{media_variant}/index.m3u8?addUserInfo=1"
            req = urllib.request.Request(test_url, headers={
                "User-Agent": UA,
                "Referer": "https://www.nba.com/",
                "authorizationtoken": auth_token,
            })
            try:
                with urllib.request.urlopen(req, timeout=8) as resp:
                    if resp.status == 200:
                        print(f"[NBA VOD] URL diretto funziona: {media_variant} ({ep_type})")
                        return media_variant, ""
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    continue
                return "", f"LP HTTP {e.code} per {media_variant}"
            except Exception:
                continue

    return "", f"Impossibile risolvere mediaId per gameId={game_id_clean}"


def _get_hls_url(media_id: str, auth_token: str, session_id: str) -> tuple[str, str]:
    """
    Ottiene l'URL HLS (index.m3u8) dall'API NBA LP.
    Per i VOD, prova prima a risolvere il vero mediaId dall'API NBA content.
    Ritorna (url, errore).
    """
    # Parametri comuni
    params = {
        "addUserInfo"   : "1",
        "vpmute"        : "0",
        "State"         : "NA",
        "PostalCode"    : "80133",
        "Zip"           : "80133",
        "LatLongRadius" : "40.84:14.26",
        "ISO3166"       : "IT",
        "sessionId"     : session_id,
        "inMultiview"   : "false",
        "ads"           : "mr",
    }

    # Costruisci URL HLS Akamai LP
    # Formato live:  https://lpnba.akamaized.net/live-pz/q/hls-wvpr/{mediaId}/index.m3u8
    # Formato VOD:   https://lpnba.akamaized.net/vod-pz/p/hls-wvpr/{mediaId}/index.m3u8

    for endpoint_type in ("live", "vod"):
        prefix = "live-pz/p" if endpoint_type == "live" else "vod-pz/p"
        base = f"https://lpnba.akamaized.net/{prefix}/hls-wvpr/{media_id}/index.m3u8"
        qs   = urllib.parse.urlencode(params)
        url  = f"{base}?{qs}"

        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Referer"   : "https://www.nba.com/",
            "Origin"    : "https://www.nba.com",
            "authorizationtoken": auth_token,
        })
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                if resp.status == 200:
                    return url, ""
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue  # Prova l'altro endpoint
            return "", f"HLS HTTP {e.code}"
        except Exception as e:
            return "", f"HLS error: {e}"

    # Se l'ID sintetico non funziona, prova a risolvere il vero mediaId VOD
    print(f"[NBA] mediaId sintetico '{media_id}' non trovato su Akamai, risolvo VOD ID reale...")
    real_id, resolve_err = _resolve_vod_media_id(media_id, auth_token)
    if real_id and real_id != media_id:
        # Riprova con il mediaId reale
        for endpoint_type in ("vod", "live"):
            prefix = "vod-pz/p" if endpoint_type == "vod" else "live-pz/p"
            base = f"https://lpnba.akamaized.net/{prefix}/hls-wvpr/{real_id}/index.m3u8"
            qs   = urllib.parse.urlencode(params)
            url  = f"{base}?{qs}"
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Referer"   : "https://www.nba.com/",
                "Origin"    : "https://www.nba.com",
                "authorizationtoken": auth_token,
            })
            try:
                with urllib.request.urlopen(req, timeout=12) as resp:
                    if resp.status == 200:
                        return url, ""
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    continue
                return "", f"HLS (real ID) HTTP {e.code}"
            except Exception:
                continue

    return "", f"Nessun endpoint HLS disponibile per mediaId={media_id}" + (f" ({resolve_err})" if resolve_err else "")


def _fetch_manifest(hls_url: str, auth_token: str) -> tuple[str, str]:
    """Scarica il manifest M3U8 e ritorna (testo, errore)."""
    _throttle()
    h = {
        "User-Agent" : UA,
        "Referer"    : "https://www.nba.com/",
        "Origin"     : "https://www.nba.com",
        "authorizationtoken": auth_token,
    }
    status, body = _cf_get(hls_url, headers=h, timeout=15)
    if status == 200:
        return body.decode("utf-8", errors="replace"), ""
    return "", f"Manifest HTTP {status}"


def _extract_widevine_pssh(manifest: str) -> str:
    """Estrae il PSSH Widevine (base64) dal manifest HLS."""
    wv_dash = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
    wv_nodash = WIDEVINE_SYSTEM_ID.hex().lower()
    for line in manifest.splitlines():
        is_wv = "widevine" in line.lower() or wv_dash in line.lower() or wv_nodash in line.lower()
        if is_wv and ("EXT-X-KEY" in line or "EXT-X-SESSION-KEY" in line):
            m = re.search(r'URI="data:text/plain;base64,([^"]+)"', line)
            if m:
                return m.group(1)
            m = re.search(r'URI="([^"]+)"', line)
            if m:
                raw_uri = m.group(1)
                if raw_uri.startswith("data:text/plain;base64,"):
                    return raw_uri.split(",", 1)[1]
    return ""


def _get_widevine_license(
    pssh_b64   : str,
    media_id   : str,
    session_id : str,
    auth_token : str,
    is_dvr     : bool = False,
) -> tuple[str, str]:
    """
    Richiede la licenza Widevine e ritorna (kid:key, errore).
    Usa pywidevine per generare il challenge e parsare la risposta.
    """
    try:
        from pywidevine import PSSH, Cdm, Device
    except ImportError:
        return "", "pywidevine non installato"

    if not WVD_PATH.exists():
        return "", f"device.wvd non trovato: {WVD_PATH}"

    pssh_bytes = base64.b64decode(pssh_b64)
    pssh_obj   = PSSH(pssh_bytes)

    device  = Device.load(str(WVD_PATH))
    cdm     = Cdm.from_device(device)
    sess_id = cdm.open()

    challenge = cdm.get_license_challenge(sess_id, pssh_obj)

    lic_url = (
        f"{NBA_LICENSE_URL}"
        f"?ownerUid=azuki"
        f"&mediaId={urllib.parse.quote(media_id)}"
        f"&sessionId={urllib.parse.quote(session_id)}"
        f"&is_dvr={str(is_dvr).lower()}"
    )

    lic_headers = {
        "accept"            : "*/*",
        "accept-language"   : "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "applicationtoken"  : APPLICATION_TOKEN,
        "authorizationtoken": auth_token,
        "azukiimc"          : AZUKI_IMC,
        "content-type"      : "text/plain",
        "deviceprofile"     : DEVICEPROFILE_B64,
        "origin"            : "https://www.nba.com",
        "referer"           : "https://www.nba.com/",
        "user-agent"        : UA,
        "x-locationdetails" : LOCATION_B64,
    }

    _throttle()
    # Usa curl_cffi per la richiesta licenza (impersona Chrome, bypassa TLS fingerprint)
    if _HAS_CURL_CFFI:
        try:
            import curl_cffi.requests as _cf_req
            lic_resp = _cf_req.post(
                lic_url,
                content=bytes(challenge),
                headers=lic_headers,
                impersonate="chrome131",
                timeout=15,
            )
            if lic_resp.status_code == 200:
                lic_body = lic_resp.content
            else:
                cdm.close(sess_id)
                return "", f"License HTTP {lic_resp.status_code}: {lic_resp.text[:300]}"
        except Exception as e:
            cdm.close(sess_id)
            return "", f"License error (curl_cffi): {e}"
    else:
        req = urllib.request.Request(
            lic_url,
            data    = bytes(challenge),
            headers = lic_headers,
            method  = "POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                lic_body = resp.read()
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", errors="ignore")
            cdm.close(sess_id)
            return "", f"License HTTP {e.code}: {body}"
        except Exception as e:
            cdm.close(sess_id)
            return "", f"License error: {e}"

    cdm.parse_license(sess_id, lic_body)
    keys = [
        f"{k.kid.hex}:{k.key.hex()}"
        for k in cdm.get_keys(sess_id)
        if k.type == "CONTENT"
    ]
    cdm.close(sess_id)

    if not keys:
        return "", "Nessuna content key nella risposta licenza"

    return keys[0], ""


def estrai_stream_e_chiave(
    media_id : str,
    is_vod   : bool = False,
    mpd_only : bool = False,
) -> tuple[dict | None, str]:
    """
    Estrae URL HLS + chiave Widevine per un evento/canale NBA LP.

    Parametri:
      media_id  - es. 'NBATVI' (live), 'g0022600034nyk' (partita)
      is_vod    - True per partite finite / replay
      mpd_only  - True per ottenere solo l'URL senza chiave

    Ritorna:
      ({"mpd", "key", "ua", "referer"}, "") oppure (None, "errore")
    """
    auth_token = _get_auth_token()
    if not auth_token:
        return None, "Token NBA assente. Vai su /nba-auth per fare login."

    session_id = _new_session_id()

    # 1. Ottieni URL HLS
    # Per NBATVI usa direttamente l'URL noto
    if media_id == "NBATVI":
        hls_url = (
            f"https://lpnba.akamaized.net/live-pz/p/hls-wvpr/NBATVI/index.m3u8"
            f"?start=LIVE&end=END&addUserInfo=1&vpmute=0"
            f"&State=NA&PostalCode=80133&Zip=80133"
            f"&LatLongRadius=40.84%3A14.26&ISO3166=IT"
            f"&sessionId={session_id}&inMultiview=false&ads=mr"
        )
        hls_err = ""
    else:
        with _EXTRACT_LOCK:
            hls_url, hls_err = _get_hls_url(media_id, auth_token, session_id)

    if not hls_url:
        return None, hls_err or "URL HLS non trovato"

    if mpd_only:
        return {
            "mpd"    : hls_url,
            "key"    : "",
            "ua"     : UA,
            "referer": "https://www.nba.com/",
        }, ""

    # 2. Cache chiave
    cached = _KEY_CACHE.get(media_id)
    if cached and (time.time() - cached[2]) < _KEY_TTL:
        return {
            "mpd"    : hls_url,
            "key"    : cached[0],
            "pssh"   : cached[1],
            "ua"     : UA,
            "referer": "https://www.nba.com/",
        }, ""

    # 3. Scarica manifest
    manifest, err = _fetch_manifest(hls_url, auth_token)
    if not manifest:
        return None, err

    # 4. Estrai PSSH
    pssh_b64 = _extract_widevine_pssh(manifest)
    if not pssh_b64:
        # Nessun DRM: stream cleartext (raro ma possibile)
        print(f"[NBA] Nessun PSSH Widevine in manifest per {media_id}: forse ClearKey statica")
        # Prova a estrarre clearkey direttamente dal manifest HLS
        for line in manifest.splitlines():
            if "EXT-X-KEY" in line and "METHOD=AES-128" in line:
                m = re.search(r'URI="([^"]+)"', line)
                if m:
                    # Stream AES-128 non-DRM
                    return {
                        "mpd"    : hls_url,
                        "key"    : "",
                        "ua"     : UA,
                        "referer": "https://www.nba.com/",
                    }, ""
        return None, f"PSSH Widevine non trovato nel manifest per {media_id}"

    # 5. Richiedi licenza
    key, err = _get_widevine_license(
        pssh_b64, media_id, session_id, auth_token, is_dvr=False
    )
    if not key:
        return None, err

    _KEY_CACHE[media_id] = (key, pssh_b64, time.time())

    return {
        "mpd"    : hls_url,
        "key"    : key,
        "pssh"   : pssh_b64,
        "ua"     : UA,
        "referer": "https://www.nba.com/",
    }, ""


def estrai_eventi(tipo: str = "live") -> dict:
    """
    Estrae stream + chiavi DRM per gli eventi NBA nel catalogo.

    tipo: 'live', 'upcoming', 'vod', 'all'/'both'
    """
    catalog  = get_catalog()
    res      = {"ok": True, "eventi": [], "estratti": 0, "errori": []}
    nba_logo = "https://cdn.nba.com/logos/leagues/L/logo-nba.svg"

    tipo_norm = (tipo or "live").strip().lower()
    seletti   = {
        "live"    : ["live"],
        "upcoming": ["upcoming"],
        "vod"     : ["vod"],
        "all"     : ["live", "upcoming", "vod"],
        "both"    : ["live", "upcoming", "vod"],
        "tutto"   : ["live", "upcoming", "vod"],
    }.get(tipo_norm, ["live"])

    pool = []
    for s in seletti:
        pool.extend(catalog.get(s, []))

    # Limita (non estrarre centinaia di VOD per volta)
    limit = 10 if tipo_norm in ("all", "both", "tutto") else 5
    pool  = pool[:limit]

    for ev in pool:
        media_id = ev.get("media_id") or ev.get("id", "")
        is_vod   = ev.get("is_vod", False)
        title    = ev.get("title", f"NBA {media_id}")

        res_i, err = estrai_stream_e_chiave(media_id, is_vod=is_vod)
        if not res_i:
            res["errori"].append(f"{title}: {err}")
            continue

        res["eventi"].append({
            "name"    : title,
            "image"   : ev.get("image") or nba_logo,
            "start"   : ev.get("start") or "",
            "end"     : "",
            "mpd"     : res_i.get("mpd") or "",
            "key"     : res_i.get("key") or "",
            "ua"      : res_i.get("ua") or "",
            "referer" : res_i.get("referer") or "",
            "media_id": media_id,
            "is_vod"  : is_vod,
        })
        res["estratti"] += 1

    return res
