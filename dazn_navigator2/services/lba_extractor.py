"""Estrattore eventi e stream LBA TV tramite API Endeavor/DCE ufficiale."""

import base64
import json
import re
import threading
import time
from pathlib import Path
import urllib.request
import urllib.parse
import urllib.error

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SESSION_FILE = BASE_DIR / "lba_session.json"
WIDEVINE_SID = bytes.fromhex("edef8ba979d64acea3c827dcd51d21ed")

API_BASE = "https://dce-frontoffice.imggaming.com"
API_KEY = "857a1e5d-e35e-4fdf-805b-a87b6f8364bf"
REALM = "dce.lba"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36 Edg/156.0.0.0"
)

# ---------------------------------------------------------------------------
# Rate limiter globale
# ---------------------------------------------------------------------------
# Il backend DGE risponde 401 "Invalid timestamp" quando riceve troppe
# richieste ravvicinate (e' un rate limit mascherato). Serializziamo le
# chiamate stream+callback con un piccolo gap per non far scattare il limite.
_RATE_LOCK = threading.Lock()
_RATE_LAST = 0.0
_RATE_MIN_GAP = 2.0

# Serializza l'intera sequenza stream+callback: il backend DGE valida il
# timestamp in ordine crescente, quindi due estrazioni concorrenti possono
# invalidarsi a vicenda ("Invalid timestamp").
_EXTRACT_LOCK = threading.Lock()


def _throttle(gap: float | None = None) -> None:
    """Attende il gap minimo tra due chiamate di rete consecutive."""
    global _RATE_LAST
    g = _RATE_MIN_GAP if gap is None else gap
    with _RATE_LOCK:
        now = time.time()
        wait = _RATE_LAST + g - now
        if wait > 0:
            time.sleep(wait)
        _RATE_LAST = time.time()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _resolve_long_lived_mpd(mpd_url: str) -> str:
    """Segue il redirect Akamai hdnea (40s) -> hdntl (token ~24h).

    Akamai risponde 301 sul manifest con token breve e mette nell'header
    Location un URL `hdntl=...` valido ~24h: e' lo stesso link che usa il
    player web e che il plugin WidevineProxy2 mostra. Salvando l'URL finale
    il link resta valido per un giorno intero.
    """
    if not mpd_url:
        return mpd_url
    try:
        opener = urllib.request.build_opener(_NoRedirectHandler)
        req = urllib.request.Request(mpd_url, headers={"user-agent": UA})
        try:
            with opener.open(req, timeout=12) as r:
                final = r.geturl()
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 303, 307, 308):
                final = e.headers.get("Location") or ""
            else:
                return mpd_url
        if final and "hdntl=" in final:
            return final
    except Exception:
        pass
    return mpd_url


def _get_session() -> dict:
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_tokens() -> tuple[str, str]:
    """Ritorna (bearer_token, mixed_token), rinnovando se necessario."""
    try:
        from dazn_navigator2.auth.lba_token_refresher import refresh_token_se_necessario
        refresh_token_se_necessario()
    except Exception:
        pass

    sess = _get_session()
    auth_token = sess.get("authorisationToken", "")
    refresh_token = sess.get("refreshToken", "")
    
    if auth_token.startswith("Mixed "):
        parts = auth_token.split(" ")
        bearer = parts[1] if len(parts) > 1 else auth_token
        return bearer, auth_token
    elif " " in auth_token:
        bearer = auth_token.split(" ")[0]
        mixed = f"Mixed {auth_token}"
        return bearer, mixed
    else:
        bearer = auth_token
        mixed = f"Mixed {auth_token} {refresh_token}".strip() if refresh_token else f"Bearer {auth_token}"
        return bearer, mixed


def _fetch_section_init(section_name: str = "SERIE A") -> dict | None:
    bearer, mixed = _get_tokens()
    if not bearer:
        return None
    url = f"{API_BASE}/api/v1/init/?section={urllib.parse.quote(section_name)}&countEvents=LIVE&readLicences=true&rpp=20&bpp=10&bspp=15"
    headers = {
        "accept": "application/json, text/plain, */*",
        "app": "dice",
        "authorization": mixed,
        "content-type": "application/json",
        "origin": "https://lbatv.com",
        "realm": REALM,
        "referer": "https://lbatv.com/",
        "user-agent": UA,
        "x-api-key": API_KEY,
        "x-app-var": "6.60.0.1eb8551"
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[LBA Init Section {section_name} Err] {e}")
        return None



def _fetch_playlist_vods(playlist_id: int) -> list:
    """Scarica i singoli VOD appartenenti a una playlist/giornata tramite /api/v2/vod/playlist/{id}."""
    bearer, _ = _get_tokens()
    if not bearer:
        return []
    url = f"{API_BASE}/api/v2/vod/playlist/{playlist_id}"
    req = urllib.request.Request(url, headers={
        "accept": "application/json, text/plain, */*",
        "app": "dice",
        "authorization": f"Bearer {bearer}" if not bearer.startswith("Bearer ") else bearer,
        "origin": "https://lbatv.com",
        "realm": REALM,
        "referer": "https://lbatv.com/",
        "user-agent": UA,
        "x-api-key": API_KEY,
        "x-app-var": "6.60.0.1eb8551"
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("videos", {}).get("vods", [])
    except Exception as e:
        print(f"[LBA Playlist {playlist_id} Err] {e}")
        return []

_CATALOG_CACHE = None
_CATALOG_CACHE_TIME = 0

# Cache per velocizzare le estrazioni ripetute (il callback e la chiave DRM
# restano validi a lungo; l'MPD invece va rigenerato perche' ha token brevi).
_CALLBACK_CACHE: dict = {}   # id -> (callback_url, ts)
_KEY_CACHE: dict = {}        # id -> (key, pssh, ts)
_CALLBACK_TTL = 900          # 15 minuti
_KEY_TTL = 6 * 3600          # 6 ore

def get_catalog() -> dict:
    """Restituisce il catalogo diviso in live, upcoming, vod (per la UI di script2), con download parallelo e cache."""
    global _CATALOG_CACHE, _CATALOG_CACHE_TIME
    if _CATALOG_CACHE and (time.time() - _CATALOG_CACHE_TIME) < 120:
        return _CATALOG_CACHE

    res = {
        "live": [],
        "upcoming": [],
        "vod": [],
        "total": 0
    }
    
    sections = ["SERIE A", "HOME", "SCHEDULE"]
    visti = set()
    playlist_tasks = []  # list of (playlist_id, playlist_title)
    
    for sec in sections:
        data = _fetch_section_init(sec)
        if not data:
            continue
        content = data.get("content", {})
        buckets = content.get("buckets", [])
        for b in buckets:
            b_name = (b.get("name") or "").upper()
            b_type = (b.get("type") or "").upper()
            items = b.get("contentList", [])
            for it in items:
                it_id = it.get("id")
                if not it_id or it_id in visti:
                    continue
                visti.add(it_id)
                
                title = it.get("title") or it.get("name") or f"LBA Event {it_id}"
                it_type = (it.get("type") or "").upper()
                thumb = it.get("thumbnailUrl") or it.get("coverUrl") or it.get("posterUrl") or ""
                start_ts = it.get("startDate")
                start_iso = ""
                if start_ts:
                    try:
                        start_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start_ts / 1000 if start_ts > 1e11 else start_ts))
                    except Exception:
                        start_iso = str(start_ts)
                
                if it_type == "PLAYLIST" or b_type == "PLAYLISTS":
                    # Espandiamo solo le playlist rilevanti di partite/replay/highlights (evita centinaia di chiamate inutili)
                    if any(k in b_name or k in title.upper() for k in ["REPLAY", "HIGHLIGHTS", "CONDENSED", "FINALE", "SCUDETTO", "SERIE A"]):
                        playlist_tasks.append((it_id, title, thumb))
                    continue
                
                entry = {
                    "id": it_id,
                    "title": title,
                    "type": it_type,
                    "bucket": b.get("name"),
                    "image": thumb,
                    "start": start_iso,
                    "raw": it
                }
                
                if it_type == "LIVE" or "LIVE" in b_type or "IN ARRIVO" in b_name or b_type == "UPCOMING":
                    if "IN ARRIVO" in b_name or b_type == "UPCOMING":
                        res["upcoming"].append(entry)
                    else:
                        res["live"].append(entry)
                else:
                    res["vod"].append(entry)

    # Scarica le playlist in parallelo ultra-rapido (max 12 worker)
    import concurrent.futures
    def _fetch_tuple(task):
        p_id, p_title, p_thumb = task
        vods = _fetch_playlist_vods(p_id)
        return p_title, p_thumb, vods

    if playlist_tasks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
            results = ex.map(_fetch_tuple, playlist_tasks)
            for p_title, p_thumb, inner_vods in results:
                for vod in inner_vods:
                    v_id = vod.get("id")
                    if not v_id or v_id in visti:
                        continue
                    visti.add(v_id)
                    v_title = vod.get("title") or f"VOD {v_id}"
                    v_thumb = vod.get("thumbnailUrl") or vod.get("coverUrl") or p_thumb
                    res["vod"].append({
                        "id": v_id,
                        "title": v_title,
                        "type": "VOD",
                        "bucket": p_title,
                        "image": v_thumb,
                        "start": "",
                        "raw": vod
                    })
                    
    res["total"] = len(res["live"]) + len(res["upcoming"]) + len(res["vod"])
    _CATALOG_CACHE = res
    _CATALOG_CACHE_TIME = time.time()
    return res


def estrai_stream_e_chiave(event_or_vod_id: int, is_vod: bool = False, mpd_only: bool = False) -> tuple[dict | None, str]:
    """Chiama API playback di LBA TV per ottenere MPD Akamai e risolve Widevine ClearKey istantaneamente."""
    bearer, mixed = _get_tokens()
    if not bearer:
        return None, "Token di sessione LBA assente."
        
    headers = {
        "accept": "application/json, text/plain, */*",
        "app": "dice",
        "authorization": f"Bearer {bearer}",
        "content-type": "application/json",
        "origin": "https://lbatv.com",
        "realm": REALM,
        "referer": "https://lbatv.com/",
        "user-agent": UA,
        "x-api-key": API_KEY,
        "x-app-var": "6.60.0.1eb8551"
    }
    
    endpoint = f"/api/v3/stream/vod/{event_or_vod_id}" if is_vod else f"/api/v3/stream/event/{event_or_vod_id}"
    url = f"{API_BASE}{endpoint}"

    def _stream_call():
        """Chiama l'API stream e ritorna il playerUrlCallback."""
        _throttle()
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                d = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            alt_endpoint = f"/api/v3/stream/event/{event_or_vod_id}" if is_vod else f"/api/v3/stream/vod/{event_or_vod_id}"
            try:
                req_alt = urllib.request.Request(f"{API_BASE}{alt_endpoint}", headers=headers)
                with urllib.request.urlopen(req_alt, timeout=10) as resp_alt:
                    d = json.loads(resp_alt.read().decode("utf-8"))
            except Exception:
                return None, f"Errore API Stream ({e.code}): {body[:150]}"
        except Exception as e:
            return None, f"Errore richiesta stream: {e}"
        c = d.get("playerUrlCallback") or d.get("playerUriCallback")
        if not c:
            return None, "playerUrlCallback non trovato nella risposta."
        return c, ""

    def _callback_call(c):
        # L'URL di callback del player è MONOUSO: la prima chiamata risponde 200,
        # le successive 401 "Invalid timestamp". Va quindi chiamato una sola
        # volta; per ritentare serve un callback NUOVO.
        _throttle()
        cb_req = urllib.request.Request(c, headers={"user-agent": UA})
        with urllib.request.urlopen(cb_req, timeout=10) as cb_resp:
            return json.loads(cb_resp.read().decode("utf-8"))

    # Il callback è monouso e il backend valida il timestamp in ordine
    # crescente: serializza stream+callback per evitare invalidazioni incrociate.
    cb = None
    pb_data = None
    last_err = ""
    with _EXTRACT_LOCK:
        for _attempt in range(3):
            cb, stream_err = _stream_call()
            if not cb:
                last_err = stream_err
                time.sleep(0.5)
                continue
            try:
                pb_data = _callback_call(cb)
                break
            except Exception as e:
                last_err = f"Errore callback playback: {e}"
                time.sleep(0.5)
    if pb_data is None:
        return None, last_err or "Callback playback fallito"

    callback_url = cb
        
    # Estrai info dash e drm
    dash_items = pb_data.get("dash", [])
    if not dash_items or not isinstance(dash_items, list):
        return None, "Nessun flusso dash trovato nel playback."
        
    primary_dash = dash_items[0]
    mpd_url = primary_dash.get("url", "")
    drm_info = primary_dash.get("drm", {})
    drm_jwt = drm_info.get("jwtToken", "")
    license_url = drm_info.get("url", "https://shield-drm.onvesper.com/api/v2/license")
    
    if not mpd_url:
        return None, "Nessun URL MPD trovato."

    # Segui il 301 Akamai: trasforma il token breve hdnea (40s) nel token
    # lungo hdntl (~24h) usato dal player web.
    mpd_url = _resolve_long_lived_mpd(mpd_url)

    # Solo URL MPD (per il proxy del manifest): evita download MPD + licenza.
    if mpd_only:
        return {
            "mpd": mpd_url,
            "pssh": "",
            "key": "",
            "ua": UA,
            "referer": "https://lbatv.com/",
            "lba_callback": callback_url
        }, ""

    # Chiave già nota: evita download MPD + licenza (~10-15s risparmiati)
    cached_key = _KEY_CACHE.get(event_or_vod_id)
    if cached_key and (time.time() - cached_key[2]) < _KEY_TTL:
        return {
            "mpd": mpd_url,
            "pssh": cached_key[1],
            "key": cached_key[0],
            "ua": UA,
            "referer": "https://lbatv.com/",
            "lba_callback": callback_url
        }, ""

    # Scarica MPD per estrarre PSSH
    mpd_req = urllib.request.Request(mpd_url, headers={
        "User-Agent": UA,
        "Origin": "https://lbatv.com",
        "Referer": "https://lbatv.com/"
    })
    pssh = ""
    try:
        with urllib.request.urlopen(mpd_req, timeout=12) as mpd_resp:
            mpd_txt = mpd_resp.read().decode("utf-8", errors="ignore")
            pattern = re.compile(r'<cenc:pssh[^>]*>([A-Za-z0-9+/=]+)</cenc:pssh>', re.IGNORECASE)
            for m in pattern.findall(mpd_txt):
                try:
                    raw = base64.b64decode(m)
                    if WIDEVINE_SID in raw:
                        pssh = m
                        break
                except Exception:
                    continue
            if not pssh and pattern.findall(mpd_txt):
                pssh = pattern.findall(mpd_txt)[0]

            # Flussi LIVE Endeavor/Dice: PSSH Widevine presente nell'init segment .m4s
            if not pssh:
                init_match = re.search(r'initialization="([^"]+)"', mpd_txt)
                base_match = re.search(r'<BaseURL[^>]*>(.*?)</BaseURL>', mpd_txt)
                rep_match = re.search(r'<Representation[^>]+id="([^"]+)"', mpd_txt)
                if init_match and rep_match:
                    init_tpl = init_match.group(1)
                    rep_id = rep_match.group(1)
                    init_file = init_tpl.replace("$RepresentationID$", rep_id)
                    base_dir = mpd_url.split("?")[0].rsplit("/", 1)[0]
                    qs = mpd_url.split("?")[1] if "?" in mpd_url else ""
                    sub_path = base_match.group(1) if base_match else ""
                    init_url = f"{base_dir}/{sub_path}{init_file}"
                    if qs:
                        init_url += f"?{qs}"
                    try:
                        init_req = urllib.request.Request(init_url, headers={
                            "User-Agent": UA,
                            "Origin": "https://lbatv.com",
                            "Referer": "https://lbatv.com/"
                        })
                        with urllib.request.urlopen(init_req, timeout=12) as i_resp:
                            init_bytes = i_resp.read()
                            idx = 0
                            while True:
                                idx = init_bytes.find(b"pssh", idx)
                                if idx == -1:
                                    break
                                box_size = int.from_bytes(init_bytes[idx-4:idx], byteorder="big")
                                box_data = init_bytes[idx-4:idx-4+box_size]
                                if WIDEVINE_SID in box_data:
                                    pssh = base64.b64encode(box_data).decode("ascii")
                                    break
                                idx += 4
                    except Exception as e_init:
                        print(f"[Init Fetch Err] {e_init}")
    except Exception as e:
        print(f"[MPD Fetch Err] {e}")
        
    key = ""
    if pssh and drm_jwt:
        key = _decrypt_widevine(pssh, drm_jwt, license_url)
        if key:
            _KEY_CACHE[event_or_vod_id] = (key, pssh, time.time())
        
    return {
        "mpd": mpd_url,
        "pssh": pssh,
        "key": key,
        "ua": UA,
        "referer": "https://lbatv.com/",
        "lba_callback": callback_url
    }, ""


def _decrypt_widevine(pssh_b64: str, drm_jwt: str, lic_url: str) -> str:
    try:
        from pywidevine import PSSH, Cdm, Device
        wvd_path = BASE_DIR / "device.wvd"
        if not wvd_path.exists():
            return ""
        cdm = Cdm.from_device(Device.load(str(wvd_path)))
        sess = cdm.open()
        chal = cdm.get_license_challenge(sess, PSSH(base64.b64decode(pssh_b64)))
        
        lic_headers = {
            "accept": "*/*",
            "authorization": f"Bearer {drm_jwt}",
            "user-agent": UA,
            "x-drm-info": "eyJzeXN0ZW0iOiJjb20ud2lkZXZpbmUuYWxwaGEifQ==",
            "origin": "https://lbatv.com",
            "referer": "https://lbatv.com/"
        }
        req = urllib.request.Request(lic_url, data=bytes(chal), headers=lic_headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            lic_body = resp.read()
            
        cdm.parse_license(sess, lic_body)
        keys = [f"{k.kid.hex}:{k.key.hex()}" for k in cdm.get_keys(sess) if k.type == "CONTENT"]
        cdm.close(sess)
        return keys[0] if keys else ""
    except Exception as e:
        print(f"[LBA DRM Decrypt Error] {e}")
        return ""


def estrai_eventi(tipo: str = "both") -> dict:
    """Estrae stream + chiavi DRM per i contenuti LBA TV (live / upcoming / vod)."""
    catalog = get_catalog()
    res = {"ok": True, "eventi": [], "estratti": 0, "errori": []}

    tipo_norm = (tipo or "both").strip().lower()
    seletti = {
        "both": ["live", "upcoming", "vod"],
        "all": ["live", "upcoming", "vod"],
        "tutto": ["live", "upcoming", "vod"],
        "live": ["live"],
        "upcoming": ["upcoming"],
        "in_arrivo": ["upcoming"],
        "vod": ["vod"],
    }.get(tipo_norm, ["live", "upcoming", "vod"])

    limit = 15
    pool = []
    for s in seletti:
        items = catalog.get(s) or []
        pool.extend(items[:limit])

    for ev in pool:
        ev_type = (ev.get("type") or "").upper()
        is_vod = ev_type == "VOD"
        ev_id = ev.get("id")
        if not ev_id:
            continue
        title = ev.get("title") or f"LBA Event {ev_id}"
        res_i, err = estrai_stream_e_chiave(ev_id, is_vod=is_vod)
        if not res_i:
            res["errori"].append(f"{title}: {err}")
            continue
        res["eventi"].append({
            "name": title,
            "image": ev.get("image") or "",
            "start": ev.get("start") or "",
            "end": "",
            "mpd": res_i.get("mpd") or "",
            "key": res_i.get("key") or "",
            "ua": res_i.get("ua") or "",
            "referer": res_i.get("referer") or "",
            "lba_id": ev_id,
            "is_vod": is_vod
        })
        res["estratti"] += 1

    return res
