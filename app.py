import asyncio
import json
import os
import re
import sys
import shutil
import time
import zipfile
import subprocess
import urllib
import urllib.parse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))


def _upstash_token():
    """Token Upstash, letto da un file locale coperto da .gitignore.

    Era scritto dentro il codice, quindi chiunque leggesse il repository
    poteva scrivere e leggere tutto il database. La lettura sta ora in
    dazn_navigator2/services/secrets_store.py, condivisa con eventi_cmds.
    Se il file non c'e' si ottiene una stringa vuota: le chiamate Upstash
    falliranno, ma il resto del sito continuera' a funzionare.
    """
    from dazn_navigator2.services.secrets_store import upstash_token
    return upstash_token()

class _FailoverStream:
    """Stream che non lascia mai propagare un errore di I/O.

    Quando il server gira senza una console reale (avviato in background con
    Start-Process/nascosto) l'handle console ereditato diventa inutilizzabile:
    i write riescono finche' il buffer non si riempie, poi flush() solleva
    OSError [Errno 22] "Invalid argument". Nei print di avanzamento
    dell'estrattore quell'eccezione veniva propagata come fallimento
    dell'ESTRAZIONE (HTTP 500) invece di essere un problema di solo logging.

    Alla prima scrittura/flush fallita lo stream originals viene abbandonato
    e tutti i log successivi vanno su flask.log. La console resta primaria
    quando funziona, quindi il launcher .bat continua a mostrare l'output.
    """

    def __init__(self, stream, log_path):
        self._stream = stream
        self._log_path = log_path
        self._log = None

    def _get_log(self):
        if self._log is None:
            try:
                self._log = open(self._log_path, "a", encoding="utf-8", buffering=1)
            except Exception:
                self._log = False
        return self._log or None

    def _break_stream(self):
        stream, self._stream = self._stream, None
        try:
            stream.flush()
        except Exception:
            pass
        try:
            stream.close()
        except Exception:
            pass

    def write(self, data):
        if self._stream is not None:
            try:
                return self._stream.write(data)
            except Exception:
                self._break_stream()
        log = self._get_log()
        if log is not None:
            try:
                log.write(data)
            except Exception:
                pass
        return len(data) if isinstance(data, (str, bytes, bytearray)) else 0

    def flush(self):
        if self._stream is not None:
            try:
                return self._stream.flush()
            except Exception:
                self._break_stream()
        log = self._get_log()
        if log is not None:
            try:
                log.flush()
            except Exception:
                pass
        return None

    def isatty(self):
        if self._stream is None:
            return False
        try:
            return self._stream.isatty()
        except Exception:
            return False

    def fileno(self):
        if self._stream is None:
            raise OSError("stream non disponibile")
        return self._stream.fileno()

    def writable(self):
        return True

    def readable(self):
        return False

    def __getattr__(self, name):
        # Delega attributi rari (encoding, errors, newlines) al stream
        # originale; dopo il failover usa i default di un file testuale.
        if self._stream is not None:
            return getattr(self._stream, name)
        if name == "encoding":
            return "utf-8"
        if name in ("errors", "newlines"):
            return None
        raise AttributeError(name)


# Protezione per esecuzione nascosta/senza console (es. pythonw o script VBS)
if sys.stdout is None:
    try:
        sys.stdout = open(BASE_DIR / "flask.log", "a", encoding="utf-8", buffering=1)
    except Exception:
        import io
        sys.stdout = io.StringIO()
if sys.stderr is None:
    try:
        sys.stderr = open(BASE_DIR / "flask.log", "a", encoding="utf-8", buffering=1)
    except Exception:
        import io
        sys.stderr = io.StringIO()

sys.stdout = _FailoverStream(sys.stdout, BASE_DIR / "flask.log")
sys.stderr = _FailoverStream(sys.stderr, BASE_DIR / "flask.log")

# ── Neutralizza il renderer "legacy Windows" di rich ─────────────────────
# Quando il server gira SENZA una console reale (avviato in background con
# Start-Process/nascosto, handle console ereditato da una shell che poi
# chiude), rich rileva l'assenza di supporto VT e usa LegacyWindowsTerm per
# i print di avanzamento dell'estrattore. Il flush su quell'handle morto
# solleva OSError [Errno 22] e l'errore viene propagato come fallimento
# dell'ESTRAZIONE (500) invece di essere un innocuo problema di logging.
# Forzando legacy_windows=False rich scrive sul file/pipe senza toccare le
# API console: sicuro su Windows (batch e background) e no-op su Linux.
try:
    import rich.console as _rich_console
    _rich_console.detect_legacy_windows = lambda: False
except Exception:
    pass

import threading
from dazn_navigator2.cli.eventi_cmds import _load, _save, _fetch_from_upstash, add_event, add_events, pubblica
from dazn_navigator2.services.explorer import DaznExplorer
from dazn_navigator2.services.extractor import HeadlessExtractor
from dazn_navigator2.services.browser import BrowserManager
from flask import Flask, render_template, jsonify, request, Response, session, redirect, url_for

# Event loop persistente su thread dedicato per evitare 'Future attached to a different loop'
_ASYNC_LOOP = None
_ASYNC_THREAD = None
_LOOP_LOCK = threading.Lock()

# Cache dei VOD (solo sezione VOD): evita di rifare le ~70 chiamate DAZN a ogni click
_VOD_CACHE = None
_VOD_CACHE_TIME = 0
_VOD_CACHE_TTL = 300
_VOD_CACHE_LOCK = threading.Lock()

# Mette in pausa AutoExtract/estrazione durante le operazioni sul profilo (upload/delete).
# E' un timestamp con scadenza: si auto-azzera dopo il tempo indicato.
_PROFILE_BUSY_UNTIL = 0.0

# Lock globale di estrazione: impedisce collisioni contemporanee tra AutoExtract e click manuali
_EXTRACTION_LOCK = threading.Lock()

# Ultimo profilo usato dal sito: serve ai thread in background, dove la
_LAST_PID = None

# ── Cloudflare Workers Pool a rotazione (Round-Robin) ────────────────────
WORKERS_POOL = [
    "https://young-bush-b452.lssfkp01.workers.dev",
    "https://still-grass-35e0.kdjdj12121.workers.dev",
    "https://steep-heart-a0a6.pippo2992k.workers.dev",
    "https://still-morning-a752.ghjffvjuyggg.workers.dev",
    "https://polished-sky-a2c7.rmddk2364.workers.dev"
]
_WORKER_INDEX_FILE = BASE_DIR / "worker_index.txt"
_WORKER_ENABLED_FILE = BASE_DIR / "worker_enabled.txt"
_WORKER_ROTATION_LOCK = threading.Lock()

def is_worker_option_enabled():
    """Ritorna True se l'opzione di estrarre anche il link con Worker Cloudflare è attiva."""
    try:
        if _WORKER_ENABLED_FILE.exists():
            return _WORKER_ENABLED_FILE.read_text(encoding="utf-8").strip() == "1"
    except Exception:
        pass
    return False

def set_worker_option_enabled(enabled: bool):
    try:
        _WORKER_ENABLED_FILE.write_text("1" if enabled else "0", encoding="utf-8")
    except Exception:
        pass

def get_next_worker_prefix():
    """Restituisce il prossimo worker manifest a rotazione (da 1 a 5, poi ricomincia da 1).
    Salva l'indice su file così persiste anche se il server viene riavviato."""
    with _WORKER_ROTATION_LOCK:
        idx = 0
        if _WORKER_INDEX_FILE.exists():
            try:
                idx = int(_WORKER_INDEX_FILE.read_text(encoding="utf-8").strip())
            except Exception:
                idx = 0
        worker_url = WORKERS_POOL[idx % len(WORKERS_POOL)]
        next_idx = (idx + 1) % len(WORKERS_POOL)
        try:
            _WORKER_INDEX_FILE.write_text(str(next_idx), encoding="utf-8")
        except Exception:
            pass
        return f"{worker_url}/manifest?url="

# --- Blocco dei tentativi di login ----------------------------------------
# Senza un limite le password si possono provare all'infinito. Il conteggio
# e' per indirizzo, ricavato da CF-Connecting-IP (header che imposta
# cloudflared): dietro il tunnel, senza quell'header, Flask vedrebbe solo
# 127.0.0.1 e il blocco colpirebbe insieme tutti, quindi in quel caso si
# ricade sul contatore globale.
_LOGIN_TENTATIVI = {}   # ip -> [fallimenti, timestamp di sblocco]
_LOGIN_GLOBALI = []     # timestamp dei fallimenti recenti, a prescindere dall'ip
_LOGIN_LOCK = threading.Lock()
_LOGIN_MAX_TENTATIVI = 5        # dopo quanti errori consecutivi si blocca
_LOGIN_BLOCCO_SEC = 300         # 5 minuti di attesa
# Rete di sicurezza per attacchi distribuiti su tanti indirizzi. Va tenuta
# ALTA: e' un tetto globale, quindi quando scatta blocca anche l'utente
# legittimo. Con 40 fallimenti in 5 minuti bastava a chiunque bloccare
# l'utente fuori dal sito a ripetizione, ed era un problema peggiore di quello
# che si voleva prevenire. Il blocco vero e proprio e' quello per IP.
_LOGIN_MAX_GLOBALI = 150


def _login_client_id():
    """Identifica il visitatore per il blocco dei tentativi.

    Dietro il tunnel cloudflared l'ip reale arriva in CF-Connecting-IP; se
    l'header manca (per esempio accesso diretto in rete locale) si usa
    l'indirizzo del socket.
    """
    ip = (request.headers.get("CF-Connecting-IP") or "").strip()
    if not ip:
        ip = request.remote_addr or "sconosciuto"
    return ip


def _login_bloccato():
    """True se il visitatore deve aspettare. Restituisce i secondi rimanenti."""
    import time as _t
    ora = _t.time()
    cid = _login_client_id()
    with _LOGIN_LOCK:
        # il tetto globale si applica a tutti, cosi' un attaccante non puo'
        # eluderlo semplicemente ruotando indirizzo
        _LOGIN_GLOBALI[:] = [x for x in _LOGIN_GLOBALI if ora - x < _LOGIN_BLOCCO_SEC]
        if len(_LOGIN_GLOBALI) >= _LOGIN_MAX_GLOBALI:
            return _LOGIN_BLOCCO_SEC
        dati = _LOGIN_TENTATIVI.get(cid)
        if not dati:
            return 0
        fallimenti, sblocco = dati
        if sblocco > ora:
            return int(sblocco - ora)
        if sblocco:
            # il blocco e' scaduto: si riparte puliti
            _LOGIN_TENTATIVI[cid] = [0, 0.0]
    return 0


def _login_registra_fallimento():
    import time as _t
    ora = _t.time()
    cid = _login_client_id()
    with _LOGIN_LOCK:
        _LOGIN_GLOBALI.append(ora)
        dati = _LOGIN_TENTATIVI.get(cid) or [0, 0.0]
        dati[0] += 1
        if dati[0] >= _LOGIN_MAX_TENTATIVI:
            dati[1] = ora + _LOGIN_BLOCCO_SEC
        _LOGIN_TENTATIVI[cid] = dati
        return dati[0]


def _login_azzera():
    """Login riuscito: il contatore di quell'indirizzo riparte da zero."""
    with _LOGIN_LOCK:
        _LOGIN_TENTATIVI.pop(_login_client_id(), None)


def _start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

def run_async(coro, timeout=90):
    global _ASYNC_LOOP, _ASYNC_THREAD
    with _LOOP_LOCK:
        if _ASYNC_LOOP is None or not _ASYNC_LOOP.is_running():
            _ASYNC_LOOP = asyncio.new_event_loop()
            _ASYNC_THREAD = threading.Thread(target=_start_background_loop, args=(_ASYNC_LOOP,), daemon=True)
            _ASYNC_THREAD.start()
    # Avvolge la coroutine in wait_for per garantire il timeout lato asyncio
    async def _wrapped():
        return await asyncio.wait_for(coro, timeout=timeout)
    future = asyncio.run_coroutine_threadsafe(_wrapped(), _ASYNC_LOOP)
    try:
        return future.result(timeout=timeout + 5)
    except Exception:
        try:
            future.cancel()
        except Exception:
            pass
        raise

app = Flask(__name__)


def _carica_secret_key():
    """Chiave per firmare i cookie di sessione.

    Prima era una stringa fissa scritta in questo file, che essendo pubblico
    su GitHub permetteva a chiunque di fabbricare una sessione valida e
    saltare il login. Ora la chiave e' casuale e vive in un file locale
    coperto da .gitignore, quindi non e' piu' leggibile dal repository.
    L'ordine di preferenza e': variabile d'ambiente, poi file locale. Se
    non esiste, se ne genera una e la si salva, cosi' l'app si avvia sempre
    e le sessioni sopravvivono ai riavvii.
    """
    env = os.environ.get("SECRET_KEY")
    if env:
        return env
    file_chiave = BASE_DIR / ".secret_key"
    try:
        if file_chiave.exists():
            salvata = file_chiave.read_text(encoding="utf-8").strip()
            if salvata:
                return salvata
        import secrets as _secrets
        nuova = _secrets.token_hex(32)
        file_chiave.write_text(nuova, encoding="utf-8")
        return nuova
    except Exception:
        # se non si puo' scrivere il file, si usa una chiave di sessione:
        # le sessioni non sopravvivono al riavvio, ma il sito funziona
        return _secrets.token_hex(32) if "_secrets" in dir() else os.urandom(32).hex()


app.secret_key = _carica_secret_key()

PROFILES_CONFIG_FILE = BASE_DIR / "profiles_config.json"
UPLOAD_PROFILES_DIR = BASE_DIR / "saved_profiles"
UPLOAD_PROFILES_DIR.mkdir(parents=True, exist_ok=True)

# Profili con password dedicate
PROFILES = {
    "pz8": {"id": "pz8", "name": "Profilo PZ8"},
    "prova": {"id": "prova", "name": "Profilo Test"},
    "mpd": {"id": "mpd", "name": "Profilo MPD"}
}

def load_profiles_config():
    if PROFILES_CONFIG_FILE.exists():
        try:
            return json.loads(PROFILES_CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_profiles_config(data):
    PROFILES_CONFIG_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def sync_to_github(commit_msg: str):
    # Modalità 100% Locale: nessun push verso GitHub
    return True, "100% Locale (nessun sync remoto)"

def _image_url(img) -> str:
    if isinstance(img, dict):
        img_id = img.get("Id", "")
        if img_id:
            return f"https://image.discovery.indazn.com/eu/v3/eu/none/{img_id}/fill/none/top/none/100/1280/720/png/image"
        return ""
    return str(img) if img else ""


def _tile_image(tile) -> str:
    """URL immagine di una tile, provando Image, LogoImage e PromoImage."""
    raw = getattr(tile, "raw", {}) or {}
    for key in ("Image", "LogoImage", "PromoImage", "HeroImage", "BackgroundImage", "PortraitImage"):
        val = raw.get(key)
        url = _image_url(val)
        if url:
            return url
    return _image_url(getattr(tile, "image", ""))


def _to_local_time(value: str) -> str:
    """Converte un orario UTC di DAZN nell'ora italiana (Europe/Rome)."""
    if not value or not isinstance(value, str):
        return value if isinstance(value, str) else ""
    s_val = value.strip()
    if not s_val:
        return ""
    val_after_t = s_val[10:] if len(s_val) > 10 else s_val
    if "Z" not in s_val and "+" not in val_after_t and "-" not in val_after_t:
        return s_val
    try:
        from datetime import datetime, timezone, timedelta
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo("Europe/Rome")
        except Exception:
            dt_temp = datetime.fromisoformat(s_val.replace("Z", "+00:00"))
            month = dt_temp.month
            offset_hours = 2 if 3 <= month <= 10 else 1
            tz = timezone(timedelta(hours=offset_hours))
        dt = datetime.fromisoformat(s_val.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return str(value)


_IMAGE_SECTIONS = (
    "epg", "LinearChannels", "Live", "LiveAndNextNew", "Sport", "Catchup",
)


def _norm_title(v) -> str:
    """Normalizza un titolo per il confronto: minuscole, senza punteggiatura."""
    s = str(v or "").lower()
    for ch in ".,:;'\"()[]|!?":
        s = s.replace(ch, " ")
    return " ".join(s.split())


def _title_matches(a: str, b: str) -> bool:
    """Due titoli sono lo stesso evento se uno contiene l'altro come parole."""
    na, nb = _norm_title(a), _norm_title(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    wa, wb = na.split(), nb.split()
    if not wa or not wb:
        return False
    if len(wb) < len(wa):
        wa, wb = wb, wa
    # il titolo piu' corto deve comparire per intero dentro quello piu' lungo
    return " ".join(wb) in " ".join(wa) or " ".join(wa) in " ".join(wb)


async def _resolve_image_by_asset(asset_id: str, title: str = "") -> str:
    """Cerca la locandina del canale/evento partendo da asset_id o titolo.

    L'estrattore non restituisce il campo image, quindi quando il frontend non
    la manda la scheda restava senza locandina. Qui si recupera cercando la
    tile corrispondente: prima per asset_id esatto, poi per titolo.
    Per gli eventi sportivi il titolo nei rail differisce spesso da quello
    salvato ('Eupago Porto Open | Giorno 2' vs 'Porto Open Day 2'), quindi il
    confronto e' tolerantepar le parole, non letterale.
    """
    if not asset_id and not title:
        return ""

    def _norm_id(v):
        return str(v or "").strip().lower().replace("linear:", "")

    want_aid = _norm_id(asset_id)
    want_title = str(title or "").strip()

    explorer = None
    try:
        explorer = DaznExplorer()

        # 1) asset_id esatto: e' il caso piu' comune e non richiede confronti
        if want_aid:
            for section in _IMAGE_SECTIONS:
                try:
                    tiles = await explorer.get_tiles(section)
                except Exception:
                    continue
                for t in tiles or []:
                    raw = getattr(t, "raw", {}) or {}
                    ids = {t.id, t.asset_id, raw.get("Id"), raw.get("AssetId")}
                    if any(_norm_id(i) == want_aid for i in ids if i):
                        url = _tile_image(t)
                        if url:
                            return url

        # 2) titolo: confronto esatto sui rail
        if want_title:
            for section in _IMAGE_SECTIONS:
                try:
                    tiles = await explorer.get_tiles(section)
                except Exception:
                    continue
                for t in tiles or []:
                    if str(getattr(t, "title", "")).strip().lower() == want_title.lower():
                        url = _tile_image(t)
                        if url:
                            return url

        # 3) evento sportivo: il titolo non coincide mai alla lettera, quindi
        #    si usa la ricerca testuale e si sceglie il risultato piu' vicino.
        if want_title:
            try:
                res = await explorer.search(want_title)
            except Exception:
                res = []
            best, best_score = "", -1.0
            for t in res or []:
                t_title = str(getattr(t, "title", ""))
                if not _title_matches(t_title, want_title):
                    continue
                url = _tile_image(t)
                if not url:
                    continue
                na, nb = _norm_title(t_title), _norm_title(want_title)
                wa, wb = set(na.split()), set(nb.split())
                score = len(wa & wb) / max(len(wa | wb), 1)
                if score > best_score:
                    best, best_score = url, score
            if best:
                return best
        return ""
    except Exception:
        return ""
    finally:
        if explorer is not None:
            try:
                await explorer.close()
            except Exception:
                pass

def _build_mpd_auth(mpd_url: str, dazn_token: str, cdn_name: str = "dazn-token") -> str:
    """Inserisce il token nel formato corretto:
    - /@token/ nel path per i token JWT dei canali lineari (es. indazn.com)
    - query parameter ?dazn-token=... per i token Akamai (tend:...) degli eventi Live e VOD (es. daznedge.net)
    """
    if not dazn_token or not mpd_url:
        return mpd_url
    if "/@" in mpd_url or "dazn-token=" in mpd_url or "hdnea=" in mpd_url:
        return mpd_url

    if dazn_token.startswith("eyJ") and "daznedge.net" not in mpd_url:
        if "://" in mpd_url:
            proto, rest = mpd_url.split("://", 1)
            if "/" in rest:
                host, path = rest.split("/", 1)
                return f"{proto}://{host}/@{dazn_token}/{path}"
            return f"{proto}://{rest}/@{dazn_token}"
        return mpd_url
    else:
        param_name = cdn_name if cdn_name else "dazn-token"
        sep = "&" if "?" in mpd_url else "?"
        return f"{mpd_url}{sep}{param_name}={dazn_token}"

def normalize_event_title(title: str) -> str:
    """Uniforma categoricamente il titolo dello scontro nel formato standard: 'Squadra 1 - Squadra 2'."""
    if not title:
        return ""
    import re
    t = re.sub(r'\s*\((?:WARP|WORKER|ACE|TEST ACE)\)\s*', ' ', title, flags=re.IGNORECASE).strip()
    if t.strip().upper() == "DAZN":
        return "DAZN 1"
    if t.strip().upper() == "EUROSPORT":
        return "Eurosport 1"
    if re.search(r'\bZONA\s+SERIE\s+B\b', t, flags=re.IGNORECASE):
        return "ZONA SERIE B"
    if re.search(r'\bZONA\s+SERIE\s+A\b', t, flags=re.IGNORECASE):
        return "ZONA SERIE A"
    if re.search(r'\bZONA\s+GOL\b', t, flags=re.IGNORECASE):
        return "ZONA GOL"
    # Rimuove prefissi o suffissi tipo MD1, MD2, MD7, Giornata X
    t = re.sub(r'\s*\bMD\s*\d+\b\s*', ' ', t, flags=re.IGNORECASE)
    # Sostituisce 'v', 'vs', 'vs.', 'v.' con o senza spazi dopo il punto in ' - '
    t = re.sub(r'\s+(?:vs?\.?)\s+', ' - ', t, flags=re.IGNORECASE)
    t = re.sub(r'\s+(?:vs?)\.(?=[^\s])', ' - ', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*-\s*', ' - ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t

def _norm_for_dedup(name):
    """Normalizza un nome evento per deduplicazione.
    Rimuove: prefissi studio ('Fuoriclasse | ', 'Zona Gol | ' ecc.),
    accenti, suffissi tecnici (WARP/TEST/ACE) e spazi extra.
    """
    import unicodedata as _ud
    n = (name or "").strip().lower()
    # Rimuovi prefissi tipo "Fuoriclasse | " o "Studio | " prima del match
    n = re.sub(r'^[^|]+\|\s*', '', n).strip()
    # Rimuovi accenti per match robusto (Atletico vs Atlético)
    n = _ud.normalize('NFKD', n)
    n = ''.join(c for c in n if not _ud.combining(c))
    n = re.sub(r'\s*\((?:warp|worker|ace|test ace)\)\s*', ' ', n, flags=re.IGNORECASE).strip()
    n = re.sub(r'\s+test\s*$', '', n, flags=re.IGNORECASE).strip()
    return n
def _format_tile_item(t):
    raw = getattr(t, 'raw', {}) or {}
    sport = raw.get("Sport", {})
    if isinstance(sport, dict):
        sport = sport.get("Title", "")
    elif sport:
        sport = str(sport)
    else:
        sport = ""

    comp = raw.get("Competition", {})
    if isinstance(comp, dict):
        comp = comp.get("Title", "")
    elif comp:
        comp = str(comp)
    else:
        comp = ""

    # Se la competizione non è specificata ed è un canale lineare o live tv
    ttype = getattr(t, 'tile_type', '') or ''
    if not comp:
        if ttype.lower() == 'linear' or 'dazn' in (getattr(t, 'title', '') or '').lower() or 'eurosport' in (getattr(t, 'title', '') or '').lower():
            comp = "Live TV"
        elif sport:
            comp = sport

    raw_title = getattr(t, 'title', '') or ''
    title_str = normalize_event_title(raw_title)

    return {
        "id": t.id,
        "asset_id": getattr(t, 'asset_id', None) or t.id,
        "title": title_str,
        "sport": sport,
        "competition": comp or "Eventi",
        "image": _image_url(t.image),
        "tile_type": t.tile_type,
        "start": _to_local_time(raw.get("Start") or ""),
        "end": _to_local_time(raw.get("End") or ""),
    }

def _resolve_profile_dir(path_str):
    if not path_str:
        return None
    # Supporta sia separatori Windows che Linux
    clean_p = str(path_str).replace("\\", "/")
    p_obj = Path(clean_p)
    if not p_obj.is_absolute():
        p_obj = BASE_DIR / p_obj
    return p_obj

def _profile_dir_has_session(p):
    """True solo se nel profilo esiste un JWT DAZN valido salvato su disco."""
    if not p or not Path(p).exists():
        return False
    for fname in ("dazn_session.json", "auth_token.json"):
        try:
            f = Path(p) / fname
            if f.exists():
                j = json.loads(f.read_text(encoding="utf-8")).get("jwt", "")
                if j and j.startswith("eyJ"):
                    return True
        except Exception:
            continue
    return False

def get_active_chrome_profile(profile_id):
    # 1. Priorità assoluta: cartella chrome_profile presente direttamente sul Desktop
    desktop_profile = Path(r"C:\Users\alecl\Desktop\chrome_profile")
    if desktop_profile.exists():
        return str(desktop_profile)

    cfg = load_profiles_config()
    p = cfg.get(profile_id, {}).get("chrome_profile_path")
    if p:
        p_obj = _resolve_profile_dir(p)
        if p_obj and p_obj.exists():
            return str(p_obj)
    # Fallback predefinito alla cartella salvata per id
    fallback_dir = UPLOAD_PROFILES_DIR / f"profile_{profile_id}"
    if fallback_dir.exists():
        return str(fallback_dir)
    return str(BASE_DIR / "chrome_profile")

def _current_pid():
    return session.get("user_profile_id")

@app.route("/")
def home():
    if "user_profile_id" in session:
        return redirect("/script")
    return redirect("/login")

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        attesa = _login_bloccato()
        if attesa:
            return render_template(
                "login.html",
                error=("Troppi tentativi falliti. Attendi %d secondi e riprova." % attesa)
            ), 429
        password = request.form.get("password", "").strip()
        if password in PROFILES:
            prof = PROFILES[password]
            session["user_profile_id"] = prof["id"]
            session["user_profile_name"] = prof["name"]
            _login_azzera()
            return redirect("/script")
        falliti = _login_registra_fallimento()
        if falliti >= _LOGIN_MAX_TENTATIVI:
            return render_template(
                "login.html",
                error=("Password non valida. Troppi tentativi: attendi %d secondi." % _LOGIN_BLOCCO_SEC)
            ), 429
        return render_template(
            "login.html",
            error="Password non valida. Riprova. (%d tentativi falliti)" % falliti
        )
    
    if "user_profile_id" in session:
        return redirect("/script")
    return render_template("login.html")

@app.route("/lba-auth", methods=["GET", "POST"])
def lba_auth_page():
    if "user_profile_id" not in session:
        return redirect("/login")

    from dazn_navigator2.auth import lba_auth

    if request.method == "POST":
        azione = request.form.get("azione", "").strip()
        if azione == "logout":
            lba_auth.cancella_sessione()
            return redirect("/lba-auth")

        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        ok, msg = lba_auth.accedi(email, password)
        stato = lba_auth.stato_sessione()
        if ok:
            return render_template("lba_auth.html", stato=stato, ok_message=msg)
        return render_template("lba_auth.html", stato=stato, error=msg)

    stato = lba_auth.stato_sessione()
    return render_template("lba_auth.html", stato=stato)

@app.route("/api/lba/probe", methods=["GET"])
def api_lba_probe():
    from dazn_navigator2.auth import lba_auth
    import urllib.request, urllib.error
    sess = lba_auth.carica_sessione()
    token = sess.get("authorisationToken", "")
    
    headers = {
        "x-api-key": "857a1e5d-e35e-4fdf-805b-a87b6f8364bf",
        "realm": "dce.lba",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0"
    }
    
    candidates = [
        "/api/v2/navigation",
        "/api/v1/init",
        "/api/v2/home",
        "/api/v2/section",
        "/api/v2/sections",
        "/api/v2/rail",
        "/api/v2/rails",
        "/api/v2/bucket/home",
        "/api/v2/bucket/live",
        "/api/v2/bucket/vod",
        "/api/v2/event",
        "/api/v2/events",
        "/api/v2/vod",
        "/api/v2/vods",
        "/api/v3/event",
        "/api/v3/events",
        "/api/v3/vod",
        "/api/v3/vods",
        "/api/v2/feed",
        "/api/v2/content",
        "/api/v3/content",
        "/api/v3/external-table",
        "/api/v3/manifest/event",
        "/api/v3/manifest/video"
    ]
    
    results = {}
    for c in candidates:
        url = "https://dce-frontoffice.imggaming.com" + c
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = resp.read().decode('utf-8', errors='ignore')
                results[c] = {"status": resp.status, "body": data[:300]}
        except urllib.error.HTTPError as e:
            results[c] = {"status": e.code, "body": e.read().decode('utf-8', errors='ignore')[:150]}
        except Exception as ex:
            results[c] = {"status": "ERR", "body": str(ex)}
            
    return jsonify(results)


def get_active_tunnel_url(fallback_request=None) -> str:
    """Restituisce l'URL pubblico attivo del tunnel Cloudflare (da cf_bat.log) oppure fallback da request."""
    import re
    log_path = os.path.expandvars(r"%TEMP%\opencode\cf_bat.log")
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                matches = re.findall(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", content)
                if matches:
                    return matches[-1].rstrip("/")
        except Exception as e:
            print(f"[get_active_tunnel_url error] {e}")
            
    if fallback_request:
        host = fallback_request.headers.get("X-Forwarded-Host") or fallback_request.headers.get("Host") or fallback_request.host
        proto = fallback_request.headers.get("X-Forwarded-Proto") or ("https" if fallback_request.is_secure else "http")
        return f"{proto}://{host}".rstrip("/")
        
    return "http://127.0.0.1:5000"


@app.route("/lba")
def lba_page():
    if "user_profile_id" not in session:
        return redirect("/login")
    return render_template("lba_script.html", profile_name=session.get("user_profile_name", "Profilo"))

@app.route("/api/lba/catalog", methods=["GET"])
def api_lba_catalog():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import lba_extractor
    try:
        cat = lba_extractor.get_catalog()
        return jsonify({"ok": True, "catalog": cat})
    except Exception as e:
        return jsonify({"ok": False, "errore": str(e)}), 500

@app.route("/api/lba/extract-single", methods=["POST"])
def api_lba_extract_single():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import lba_extractor
    data = request.get_json(silent=True) or {}
    item_id = data.get("id")
    title = data.get("title", f"LBA Event {item_id}")
    image = data.get("image", "")
    start = data.get("start", "")
    is_vod = data.get("is_vod", False)
    
    if not item_id:
        return jsonify({"ok": False, "errore": "ID evento mancante"}), 400
        
    res, err = lba_extractor.estrai_stream_e_chiave(item_id, is_vod=is_vod)
    if not res:
        return jsonify({"ok": False, "errore": err}), 400
        
    mpd_url = res["mpd"]

    ev = {
        "name": title,
        "image": image,
        "start": start,
        "end": "",
        "mpd": mpd_url,
        "key": res["key"],
        "ua": res["ua"],
        "referer": res["referer"],
        "lba_id": item_id,
        "is_vod": is_vod
    }
    
    # Salva in dazn_event.json sotto 'LBA TV'
    ev_file = BASE_DIR / "dazn_event.json"
    cat_data = {}
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
        except Exception:
            cat_data = {}
    if "LBA TV" not in cat_data:
        cat_data["LBA TV"] = []
        
    cat_data["LBA TV"] = [e for e in cat_data["LBA TV"] if e.get("name") != title]
    cat_data["LBA TV"].append(ev)
    ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    
    try:
        from dazn_navigator2.cli.eventi_cmds import pubblica
        pubblica('Aggiornato LBA', cat_data, asincrono=False)
    except Exception as e:
        print(f"[LBA Sync Upstash] {e}")
        
    return jsonify({"ok": True, "event": ev})

_LBA_AUTO_FAIL_UNTIL: dict = {}  # id -> timestamp: backoff auto-extract (eventi non live)
_LBA_LAST_AUTO_RUN = 0.0  # ultima esecuzione auto-extract LBA (throttle globale)
_LBA_AUTO_EXTRACT_ENABLED = True  # auto-extract LBA riabilitato su richiesta

# ── NBA TV ──────────────────────────────────────────────────────────────────
@app.route("/nba-auth", methods=["GET", "POST"])
def nba_auth_page():
    if "user_profile_id" not in session:
        return redirect("/login")
    from dazn_navigator2.auth import nba_auth
    if request.method == "POST":
        azione = request.form.get("azione", "").strip()
        if azione == "logout":
            nba_auth.cancella_sessione()
            return redirect("/nba-auth")
        if azione == "refresh":
            ok, msg = nba_auth.aggiorna_auth_token()
            stato = nba_auth.stato_sessione()
            if ok:
                return render_template("nba_auth.html", stato=stato, ok_message=msg)
            return render_template("nba_auth.html", stato=stato, error=msg)
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        ok, msg = nba_auth.accedi(email, password)
        stato = nba_auth.stato_sessione()
        if ok:
            return render_template("nba_auth.html", stato=stato, ok_message=msg)
        return render_template("nba_auth.html", stato=stato, error=msg)
    stato = nba_auth.stato_sessione()
    return render_template("nba_auth.html", stato=stato)


@app.route("/api/nba/refresh-token", methods=["POST"])
def api_nba_refresh_token():
    """Rinnova il token STS NBA senza re-login (usa cookies di sessione salvati)."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.auth import nba_auth
    ok, msg = nba_auth.aggiorna_auth_token()
    return jsonify({"ok": ok, "messaggio": msg})


@app.route("/nba")
def nba_page():
    if "user_profile_id" not in session:
        return redirect("/login")
    from dazn_navigator2.auth import nba_auth
    stato = nba_auth.stato_sessione()
    return render_template("nba_script.html", profile_name=session.get("user_profile_name", "Profilo"))


@app.route("/api/nba/extract", methods=["POST"])
def api_nba_extract():
    """Estrai NBA TV Live (NBATVI) - retrocompatibilità."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import nba_extractor
    res, err = nba_extractor.estrai_stream_e_chiave("NBATVI", is_vod=False)
    if not res:
        return jsonify({"ok": False, "errore": err}), 400
    ev = {
        "name"   : "NBA TV Live",
        "image"  : "https://cdn.nba.com/logos/leagues/L/logo-nba.svg",
        "start"  : "",
        "end"    : "",
        "mpd"    : res.get("mpd", ""),
        "key"    : res["key"],
        "ua"     : res["ua"],
        "referer": res["referer"],
        "is_vod" : False,
    }
    ev_file  = BASE_DIR / "dazn_event.json"
    cat_data = {}
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
        except Exception:
            cat_data = {}
    if "NBA League Pass" not in cat_data:
        cat_data["NBA League Pass"] = []
    cat_data["NBA League Pass"] = [e for e in cat_data["NBA League Pass"] if e.get("name") != "NBA TV Live"]
    cat_data["NBA League Pass"].insert(0, ev)
    ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    try:
        from dazn_navigator2.cli.eventi_cmds import pubblica
        pubblica('Aggiornato NBA League Pass', cat_data, asincrono=False)
    except Exception as e:
        print(f"[NBA Sync Upstash] {e}")
    return jsonify({"ok": True, "event": ev})


@app.route("/api/nba/catalog", methods=["GET"])
def api_nba_catalog():
    """Catalogo partite NBA (live / upcoming / vod) senza estrarre stream."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import nba_extractor
    try:
        cat = nba_extractor.get_catalog()
        return jsonify({"ok": True, "catalog": cat})
    except Exception as e:
        return jsonify({"ok": False, "errore": str(e)}), 500


@app.route("/api/nba/extract-single", methods=["POST"])
def api_nba_extract_single():
    """Estrai stream + chiave per un singolo evento NBA LP."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import nba_extractor
    data     = request.get_json(silent=True) or {}
    media_id = data.get("media_id") or data.get("id", "")
    title    = data.get("title", f"NBA {media_id}")
    image    = data.get("image", "https://cdn.nba.com/logos/leagues/L/logo-nba.svg")
    start    = data.get("start", "")
    is_vod   = data.get("is_vod", False)

    if not media_id:
        return jsonify({"ok": False, "errore": "media_id mancante"}), 400

    res, err = nba_extractor.estrai_stream_e_chiave(media_id, is_vod=is_vod)
    if not res:
        return jsonify({"ok": False, "errore": err}), 400

    ev = {
        "name"    : title,
        "image"   : image,
        "start"   : start,
        "end"     : "",
        "mpd"     : res.get("mpd", ""),
        "key"     : res.get("key", ""),
        "ua"      : res.get("ua", ""),
        "referer" : res.get("referer", ""),
        "media_id": media_id,
        "is_vod"  : is_vod,
    }

    # Salva in dazn_event.json -> 'NBA League Pass'
    ev_file  = BASE_DIR / "dazn_event.json"
    cat_data = {}
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
        except Exception:
            cat_data = {}
    if "NBA League Pass" not in cat_data:
        cat_data["NBA League Pass"] = []
    cat_data["NBA League Pass"] = [e for e in cat_data["NBA League Pass"] if e.get("name") != title]
    cat_data["NBA League Pass"].append(ev)
    ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    try:
        from dazn_navigator2.cli.eventi_cmds import pubblica
        pubblica('Aggiornato NBA League Pass', cat_data, asincrono=False)
    except Exception as e:
        print(f"[NBA Single Sync] {e}")

    return jsonify({"ok": True, "event": ev})


@app.route("/api/nba/manual-push", methods=["POST"])
def api_nba_manual_push():
    """Riceve i curl bash copiati per MPD e Licenza, estrae PSSH e chiave Widevine, salva e pubblica."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401

    import re, base64
    from pathlib import Path
    from pywidevine import PSSH, Cdm, Device
    from curl_cffi import requests as cf_req

    data = request.get_json(silent=True) or {}
    curl_mpd = data.get("curl_mpd", "").strip()
    curl_lic = data.get("curl_lic", "").strip()
    title = data.get("title", "").strip() or "NBA Stream"
    image = data.get("image", "").strip() or "https://cdn.nba.com/logos/leagues/L/logo-nba.svg"
    cat_name = data.get("categoria", "").strip() or "NBA League Pass"

    if not curl_mpd or not curl_lic:
        return jsonify({"ok": False, "errore": "Inserisci sia il curl/link del manifest che della licenza!"}), 400

    def parse_curl_cmd(cmd_text):
        import shlex
        # Rimuovi backslash di continuazione riga
        clean_text = cmd_text.replace('\\\n', ' ').replace('\\\r\n', ' ').replace('\\', ' ')
        try:
            tokens = shlex.split(clean_text, posix=False)
        except Exception:
            tokens = clean_text.split()

        url = ""
        headers = {}
        cookies = ""

        i = 0
        while i < len(tokens):
            t = tokens[i].strip()
            if t in ("--url", "-u"):
                if i + 1 < len(tokens):
                    url = tokens[i+1].strip("\"'")
                    i += 1
            elif t in ("-H", "--header"):
                if i + 1 < len(tokens):
                    h_val = tokens[i+1].strip("\"'")
                    if ':' in h_val:
                        k, v = h_val.split(':', 1)
                        headers[k.strip().lower()] = v.strip()
                    i += 1
            elif t in ("-b", "--cookie"):
                if i + 1 < len(tokens):
                    cookies = tokens[i+1].strip("\"'")
                    i += 1
            elif t.startswith("http://") or t.startswith("https://") or t.startswith('"http') or t.startswith("'http"):
                if not url:
                    url = t.strip("\"'")
            i += 1

        # Fallback regex se tokenizzazione fallisce
        if not url:
            m_u = re.search(r'''(?:--url\s+)?['"](https?://[^'"]+)['"]''', cmd_text) or re.search(r'''(https?://[^\s'"]+)''', cmd_text)
            if m_u:
                url = m_u.group(1)
        if not cookies and "cookie" in headers:
            cookies = headers["cookie"]

        return url, headers, cookies

    mpd_url, mpd_headers, mpd_cookies = parse_curl_cmd(curl_mpd)
    lic_url, lic_headers_in, lic_cookies = parse_curl_cmd(curl_lic)

    if not mpd_url:
        return jsonify({"ok": False, "errore": "URL Manifest non trovato nel comando curl."}), 400
    if not lic_url:
        return jsonify({"ok": False, "errore": "URL Licenza non trovato nel comando curl."}), 400

    # 1. Scarico Manifest HLS/MPD (tentativo con e senza authorizationtoken/cookie)
    h_manifest = {
        "User-Agent": mpd_headers.get("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
        "Referer": mpd_headers.get("referer", "https://www.nba.com/"),
        "Origin": mpd_headers.get("origin", "https://www.nba.com"),
    }
    auth_tok = lic_headers_in.get("authorizationtoken") or mpd_headers.get("authorizationtoken", "")
    all_cookies = mpd_cookies or lic_cookies

    manifest_txt = ""
    last_err = ""

    # Strategia 1: Con headers completi e cookie
    h1 = dict(h_manifest)
    if auth_tok:
        h1["authorizationtoken"] = auth_tok
    if all_cookies:
        h1["Cookie"] = all_cookies

    try:
        r_man = cf_req.get(mpd_url, impersonate="chrome131", headers=h1, timeout=15)
        if r_man.status_code == 200:
            manifest_txt = r_man.text
        else:
            last_err = f"HTTP {r_man.status_code}: {r_man.text[:120]}"
    except Exception as e:
        last_err = str(e)

    # Strategia 2: Se 400, Akamai spesso rifiuta l'header authorizationtoken sul manifest HLS! Riprova senza.
    if not manifest_txt:
        h2 = dict(h_manifest)
        if all_cookies:
            h2["Cookie"] = all_cookies
        try:
            r_man2 = cf_req.get(mpd_url, impersonate="chrome131", headers=h2, timeout=15)
            if r_man2.status_code == 200:
                manifest_txt = r_man2.text
            else:
                last_err = f"HTTP {r_man2.status_code}: {r_man2.text[:120]}"
        except Exception as e:
            last_err = str(e)

    # Strategia 3: Pulito solo con User-Agent
    if not manifest_txt:
        try:
            r_man3 = cf_req.get(mpd_url, impersonate="chrome131", headers={"User-Agent": h_manifest["User-Agent"]}, timeout=15)
            if r_man3.status_code == 200:
                manifest_txt = r_man3.text
            else:
                last_err = f"HTTP {r_man3.status_code}: {r_man3.text[:120]}"
        except Exception as e:
            last_err = str(e)

    # Strategia 4 (RISOLUTIVA AKAMAI): Sanitizzazione parametri sporchi (es. _fw_*, hdnts, gpp)
    # Akamai va in HTTP 400 se riceve parametri pubblicitari/tracciamento non concordati
    if not manifest_txt and "?" in mpd_url:
        import urllib.parse
        try:
            p_parsed = urllib.parse.urlparse(mpd_url)
            p_qs = urllib.parse.parse_qs(p_parsed.query)
            # Conserva solo parametri essenziali per lo stream
            keep_keys = {"addUserInfo", "start", "end", "sessionId", "PostalCode", "Zip", "LatLongRadius", "ISO3166", "vpmute"}
            clean_qs = {k: v for k, v in p_qs.items() if k in keep_keys}
            if not clean_qs.get("addUserInfo"):
                clean_qs["addUserInfo"] = ["1"]
            if not clean_qs.get("sessionId") and p_qs.get("sessionId"):
                clean_qs["sessionId"] = p_qs.get("sessionId")

            sanitized_url = p_parsed.scheme + "://" + p_parsed.netloc + p_parsed.path + "?" + urllib.parse.urlencode(clean_qs, doseq=True)
            r_man4 = cf_req.get(sanitized_url, impersonate="chrome131", headers={"User-Agent": h_manifest["User-Agent"]}, timeout=15)
            if r_man4.status_code == 200:
                manifest_txt = r_man4.text
                mpd_url = sanitized_url
            else:
                last_err = f"HTTP {r_man4.status_code}: {r_man4.text[:120]}"
        except Exception as e:
            last_err = str(e)

    # Strategia 5: Fallback minimo con solo sessionId
    if not manifest_txt and "sessionId=" in mpd_url:
        import urllib.parse, re
        try:
            base_m3u8 = mpd_url.split("?")[0]
            m_s = re.search(r"sessionId=([^&]+)", mpd_url)
            if m_s:
                min_url = f"{base_m3u8}?addUserInfo=1&sessionId={m_s.group(1)}"
                r_man5 = cf_req.get(min_url, impersonate="chrome131", headers={"User-Agent": h_manifest["User-Agent"]}, timeout=15)
                if r_man5.status_code == 200:
                    manifest_txt = r_man5.text
                    mpd_url = min_url
        except Exception as e:
            last_err = str(e)

    if not manifest_txt:
        print(f"[ERROR NBA Manual] Manifest download failed: {last_err} | URL: {mpd_url}")
        return jsonify({"ok": False, "errore": f"Manifest non scaricabile ({last_err}). Copia TUTTO il cURL bash del file index.m3u8 (inclusi i parametri di sessione dopo il punto interrogativo)!"}), 400

    # 2. Estrazione PSSH Widevine
    WV_ID = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
    pssh_b64 = ""
    for line in manifest_txt.splitlines():
        if (WV_ID in line.lower() or "widevine" in line.lower()) and ("EXT-X-KEY" in line or "EXT-X-SESSION-KEY" in line):
            m = re.search(r'URI="data:text/plain;base64,([^"]+)"', line)
            if m:
                pssh_b64 = m.group(1)
                break
            m2 = re.search(r'URI="([^"]+)"', line)
            if m2 and "base64," in m2.group(1):
                pssh_b64 = m2.group(1).split("base64,")[1]
                break

    if not pssh_b64:
        return jsonify({"ok": False, "errore": "PSSH Widevine non trovato nel manifest M3U8."}), 400

    # 3. Richiesta Licenza Widevine
    wvd_path = BASE_DIR / "device.wvd"
    if not wvd_path.exists():
        return jsonify({"ok": False, "errore": "device.wvd mancante nel server."}), 500

    try:
        dev = Device.load(str(wvd_path))
        cdm = Cdm.from_device(dev)
        sess_w = cdm.open()
        challenge = cdm.get_license_challenge(sess_w, PSSH(base64.b64decode(pssh_b64)))

        lic_h = {
            "accept": "*/*",
            "accept-language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "applicationtoken": lic_headers_in.get("applicationtoken", "201822"),
            "authorizationtoken": auth_tok,
            "azukiimc": lic_headers_in.get("azukiimc", "IMC7.5.0_AN_D3.0.0_S0"),
            "content-type": "text/plain",
            "deviceprofile": lic_headers_in.get("deviceprofile", "eyJtb2RlbCI6IkRlc2t0b3AiLCJvc1ZlcnNpb24iOiIxMCIsInZlbmRvck5hbWUiOiJNaWNyb3NvZnQiLCJvc1ZlcnNpb24iOiIxMCIsInZlbmRvck5hbWUiOiJNaWNyb3NvZnQiLCJvc05hbWUiOiJIVE1MNSIsInN1cHBvcnRzSGV2Y0RlY29kZSI6ZmFsc2UsInd2TGV2ZWwiOiJMMyIsImRldmljZVVVSUQiOiJmOTdmMzYxMjBiNGQ0NjI3YTY2ZGFjYTZmMTUzZmIxNiJ9"),
            "origin": "https://www.nba.com",
            "referer": "https://www.nba.com/",
            "user-agent": lic_headers_in.get("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "x-locationdetails": lic_headers_in.get("x-locationdetails", "eyJQb3N0YWxDb2RlIjoiODAxMzMiLCJDb3VudHJ5Q29kZSI6IklUIn0=")
        }
        if lic_cookies:
            lic_h["Cookie"] = lic_cookies

        # Fallback 1: curl_cffi con content binario
        r_lic = None
        try:
            r_lic = cf_req.post(lic_url, content=bytes(challenge), headers=lic_h, impersonate="chrome131", timeout=15)
        except Exception:
            pass

        # Fallback 2: urllib nativo (come in nba_manual_push.py) se curl_cffi fallisce o restituisce 403
        if not r_lic or r_lic.status_code != 200:
            import urllib.request
            try:
                req_u = urllib.request.Request(lic_url, data=bytes(challenge), headers=lic_h, method="POST")
                with urllib.request.urlopen(req_u, timeout=15) as resp_u:
                    class _URLLibResp:
                        status_code = resp_u.status
                        content = resp_u.read()
                        text = content.decode("utf-8", errors="replace")
                    r_lic = _URLLibResp()
            except urllib.error.HTTPError as e:
                class _URLLibErr:
                    status_code = e.code
                    content = e.read()
                    text = content.decode("utf-8", errors="replace")
                # Se urllib restituisce un codice diverso, aggiorniamo se curl_cffi era fallito
                if not r_lic:
                    r_lic = _URLLibErr()
            except Exception:
                pass

        if not r_lic or r_lic.status_code != 200:
            cdm.close(sess_w)
            err_msg = r_lic.text[:200] if r_lic else "Nessuna risposta dal server licenze"
            st_code = r_lic.status_code if r_lic else 500
            return jsonify({"ok": False, "errore": f"Licenza HTTP {st_code}: {err_msg}"}), 400

        cdm.parse_license(sess_w, r_lic.content)
        keys = [f"{k.kid.hex}:{k.key.hex()}" for k in cdm.get_keys(sess_w) if k.type == "CONTENT"]
        cdm.close(sess_w)

        if not keys:
            return jsonify({"ok": False, "errore": "Nessuna content key nella risposta della licenza."}), 400
        content_key = keys[0]
    except Exception as e:
        return jsonify({"ok": False, "errore": f"Errore decifratura licenza: {e}"}), 400

    # 4. Estrazione media_id dall'URL se possibile
    m_id_match = re.search(r'/hls-wvpr/([^/?]+)', mpd_url) or re.search(r'mediaId=([^&]+)', lic_url)
    media_id = m_id_match.group(1) if m_id_match else "MANUAL"

    ev = {
        "name": title,
        "image": image,
        "start": "",
        "end": "",
        "mpd": mpd_url,
        "key": content_key,
        "ua": lic_h["user-agent"],
        "referer": "https://www.nba.com/",
        "media_id": media_id,
        "is_vod": False,
        "added_at": int(time.time()),
    }

    # Salva in dazn_event.json e pubblica su Upstash
    ev_file = BASE_DIR / "dazn_event.json"
    cat_data = {}
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
        except Exception:
            cat_data = {}
    if cat_name not in cat_data:
        cat_data[cat_name] = []
    cat_data[cat_name] = [e for e in cat_data[cat_name] if e.get("name") != title]
    cat_data[cat_name].append(ev)
    ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    try:
        from dazn_navigator2.cli.eventi_cmds import pubblica
        pubblica(f"NBA Manual: {title}", cat_data, asincrono=False)
    except Exception as e:
        print(f"[NBA Manual Sync Upstash] {e}")

    return jsonify({"ok": True, "event": ev})


@app.route("/api/nba/estrai", methods=["POST"])
def api_nba_estrai():
    """Estrai batch di eventi NBA (live / upcoming / vod / all)."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.services import nba_extractor
    data = request.get_json(silent=True) or {}
    tipo = data.get("tipo", "live")

    risultato = nba_extractor.estrai_eventi(tipo=tipo)
    if not risultato.get("ok"):
        return jsonify(risultato), 400

    eventi = risultato.get("eventi", [])
    if eventi:
        ev_file  = BASE_DIR / "dazn_event.json"
        cat_data = {}
        if ev_file.exists():
            try:
                cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
            except Exception:
                cat_data = {}
        if "NBA League Pass" not in cat_data:
            cat_data["NBA League Pass"] = []

        nomi = {e.get("name") for e in cat_data["NBA League Pass"]}
        for ev in eventi:
            name = ev.get("name")
            if name in nomi:
                cat_data["NBA League Pass"] = [
                    e if e.get("name") != name else ev
                    for e in cat_data["NBA League Pass"]
                ]
            else:
                cat_data["NBA League Pass"].append(ev)
                nomi.add(name)

        ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        try:
            from dazn_navigator2.cli.eventi_cmds import pubblica
            pubblica('Aggiornato NBA League Pass', cat_data, asincrono=False)
        except Exception as e:
            print(f"[NBA Batch Sync] {e}")

    return jsonify({"ok": True, "estratti": len(eventi), "eventi": eventi, "errori": risultato.get("errori", [])})


@app.route("/api/nba/capture-token", methods=["POST"])
def api_nba_capture_token():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    from dazn_navigator2.auth import nba_auth
    ok, msg = nba_auth.accedi("", "")
    if ok:
        return jsonify({"ok": True, "messaggio": msg})
    return jsonify({"ok": False, "errore": msg}), 400


@app.route("/api/nba/extracted", methods=["GET"])
def api_nba_extracted():
    """Ritorna gli eventi NBA League Pass già estratti nel dazn_event.json."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    ev_file = BASE_DIR / "dazn_event.json"
    cat_data = {}
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
        except Exception:
            cat_data = {}
    # Supporta sia la vecchia categoria 'NBA TV' che la nuova 'NBA League Pass'
    events = cat_data.get("NBA League Pass", []) or cat_data.get("NBA TV", [])
    return jsonify({"ok": True, "events": events})



@app.route("/api/lba/extracted", methods=["GET"])
def api_lba_extracted():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    ev_file = BASE_DIR / "dazn_event.json"
    items = []
    if ev_file.exists():
        try:
            cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
            items = cat_data.get("LBA TV", [])
        except Exception:
            items = []
    return jsonify({"ok": True, "events": items})

@app.route("/api/lba/estrai", methods=["POST"])
def api_lba_estrai():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "errore": "Non autenticato"}), 401
    
    from dazn_navigator2.services import lba_extractor
    data = request.get_json(silent=True) or {}
    tipo = data.get("tipo", "both")
    
    risultato = lba_extractor.estrai_eventi(tipo=tipo)
    if not risultato.get("ok"):
        return jsonify(risultato), 400
        
    eventi = risultato.get("eventi", [])
    if eventi:
        # Aggiungi a dazn_event.json sotto la categoria 'LBA TV'
        ev_file = BASE_DIR / "dazn_event.json"
        cat_data = {}
        if ev_file.exists():
            try:
                cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
            except Exception:
                cat_data = {}
        
        if "LBA TV" not in cat_data:
            cat_data["LBA TV"] = []
            
        # Aggiorna o inserisci
        nomi_esistenti = {e.get("name") for e in cat_data["LBA TV"]}
        for ev in eventi:
            if ev.get("name") in nomi_esistenti:
                # sostituisci
                cat_data["LBA TV"] = [e if e.get("name") != ev.get("name") else ev for e in cat_data["LBA TV"]]
            else:
                cat_data["LBA TV"].append(ev)
                nomi_esistenti.add(ev.get("name"))
                
        ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        
        # Sincronizza su Upstash
        try:
            from dazn_navigator2.cli.eventi_cmds import pubblica
            pubblica('Aggiornato LBA', cat_data, asincrono=False)
        except Exception as e:
            print(f"[LBA Sync Upstash] {e}")

    return jsonify({"ok": True, "estratti": len(eventi), "eventi": eventi})

@app.route("/script")
def script_page():
    if "user_profile_id" not in session:
        return redirect("/login")

    pid = session["user_profile_id"]
    pname = session.get("user_profile_name", "Profilo")
    cfg = load_profiles_config()
    saved_path = cfg.get(pid, {}).get("chrome_profile_path")
    
    has_folder = False
    if saved_path:
        p_obj = _resolve_profile_dir(saved_path)
        has_folder = _profile_dir_has_session(p_obj) if p_obj else False
    
    if not has_folder:
        fallback_dir = UPLOAD_PROFILES_DIR / f"profile_{pid}"
        if _profile_dir_has_session(fallback_dir):
            has_folder = True
            saved_path = str(fallback_dir.relative_to(BASE_DIR)).replace("\\", "/")

    return render_template(
        "script.html",
        profile_name=pname,
        chrome_profile_set=has_folder,
        chrome_profile_path=saved_path or "Nessun profilo caricato"
    )

@app.route("/logout")
def logout_action():
    session.clear()
    return redirect("/login")

def _robust_remove_dir(path: Path):
    """Chiude prima i browser e rimuove la cartella del profilo in modo sicuro su Windows."""
    try:
        from dazn_navigator2.services.browser import close_browser
        run_async(close_browser())
    except Exception:
        pass

    import time, stat

    def remove_readonly(func, p, exc_info):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass

    if path.exists():
        for f_name in ["dazn_session.json", "auth_token.json"]:
            f_p = path / f_name
            if f_p.exists():
                try:
                    f_p.unlink()
                except Exception:
                    pass

        try:
            shutil.rmtree(path, onerror=remove_readonly)
        except Exception:
            time.sleep(0.3)
            try:
                shutil.rmtree(path, ignore_errors=True)
            except Exception:
                pass

def _kill_edge_for_profile(profile_dir):
    """Rilascia i file del profilo: chiude il contesto Playwright persistente
    (warmup) che li tiene mappati e termina eventuali processi Edge headless
    rimasti appesi (bloccano eliminazione/estrazione del profilo)."""
    try:
        async def _close_matching_browser():
            try:
                from dazn_navigator2 import services
                _brmod = services.browser
                b = _brmod._browser_instance
                if b is not None:
                    bd = Path(b._user_data_dir or "")
                    if bd == Path(profile_dir):
                        await b.close()
                        # Azzera il singleton: una copia chiusa non deve piu'
                        # essere restituita da get_browser (context=None).
                        _brmod._browser_instance = None
            except Exception:
                pass
        run_async(_close_matching_browser(), timeout=30)
    except Exception as e:
        print(f"[Browser Close Warning] {e}")

    try:
        pd = str(Path(profile_dir)).replace("\\", "\\\\")
        ps_cmd = (
            "Get-CimInstance Win32_Process | Where-Object { "
            "$_.Name -eq 'msedge.exe' -and $_.CommandLine -like '*" + pd + "*' } | "
            "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
        )
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        print(f"[Edge Kill Warning] {e}")

# API PER UPLOAD DAZN_SESSION.JSON / JSON SESSION CON AUTO-PUSH SU GITHUB
@app.route("/api/upload-profile-zip", methods=["POST"])
def upload_profile_zip():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401

    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Nessun file inviato"}), 400

    uploaded_file = request.files["file"]
    fname = (uploaded_file.filename or "").lower()
    if not fname.endswith(".json") and not fname.endswith(".zip"):
        return jsonify({"ok": False, "error": "Seleziona il file dazn_session.json o auth_token.json"}), 400

    pid = session["user_profile_id"]
    profile_dest = UPLOAD_PROFILES_DIR / f"profile_{pid}"

    _PROFILE_BUSY_UNTIL = time.time() + 600
    _kill_edge_for_profile(profile_dest)
    if profile_dest.exists():
        _robust_remove_dir(profile_dest)
    profile_dest.mkdir(parents=True, exist_ok=True)

    if fname.endswith(".zip"):
        zip_path = profile_dest / "temp_profile.zip"
        uploaded_file.save(str(zip_path))
        staging = UPLOAD_PROFILES_DIR / f".staging_{pid}"
        try:
            _kill_edge_for_profile(profile_dest)
            if staging.exists():
                _robust_remove_dir(staging)
            staging.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(staging)

            # Appiattisce l'eventuale cartella radice unica (es. chrome_profile/)
            src = staging
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    top = {n.split("/", 1)[0] for n in zip_ref.namelist() if n.strip("/")}
                if len(top) == 1:
                    cand = staging / next(iter(top))
                    if cand.is_dir():
                        src = cand
            except Exception:
                pass

            zip_path.unlink(missing_ok=True)

            # Sostituisce il profilo con il nuovo contenuto appiattito (senza lock)
            for _ in range(3):
                _robust_remove_dir(profile_dest)
                if not profile_dest.exists():
                    break
                time.sleep(0.4)

            profile_dest.mkdir(parents=True, exist_ok=True)
            for item in list(src.iterdir()):
                shutil.move(str(item), str(profile_dest / item.name))
            _robust_remove_dir(staging)
        except Exception as e:
            _robust_remove_dir(staging)
            return jsonify({"ok": False, "error": f"Errore durante l'estrazione dello zip: {e}"}), 500
    else:
        # File JSON diretto (dazn_session.json o auth_token.json)
        dest_json = profile_dest / "dazn_session.json"
        uploaded_file.save(str(dest_json))
        try:
            data = json.loads(dest_json.read_text(encoding="utf-8"))
            tok = data.get("jwt")
            if tok:
                (profile_dest / "auth_token.json").write_text(json.dumps({"jwt": tok}), encoding="utf-8")
        except Exception:
            pass

    rel_profile_str = str(profile_dest.relative_to(BASE_DIR)).replace("\\", "/")
    cfg = load_profiles_config()
    if pid not in cfg:
        cfg[pid] = {}
    cfg[pid]["chrome_profile_path"] = rel_profile_str
    save_profiles_config(cfg)

    # SINCRONIZZA AUTOMATICAMENTE SU GITHUB PERMANENTEMENTE
    git_ok, git_msg = sync_to_github(f"persist: aggiorna sessione dazn per {pid}")

    return jsonify({
        "ok": True,
        "profile": session.get("user_profile_name", pid),
        "chrome_profile_path": rel_profile_str,
        "github_sync": git_ok,
        "github_msg": git_msg
    })

@app.route("/api/delete-profile-session", methods=["POST"])
def delete_profile_session():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401

    pid = session["user_profile_id"]
    profile_dest = UPLOAD_PROFILES_DIR / f"profile_{pid}"

    _PROFILE_BUSY_UNTIL = time.time() + 180
    _kill_edge_for_profile(profile_dest)
    if profile_dest.exists():
        _robust_remove_dir(profile_dest)

    cfg = load_profiles_config()
    if pid in cfg:
        cfg[pid]["chrome_profile_path"] = ""
        save_profiles_config(cfg)

    git_ok, git_msg = sync_to_github(f"persist: elimina sessione dazn per {pid}")

    return jsonify({
        "ok": True,
        "message": "Sessione del profilo eliminata con successo",
        "github_sync": git_ok,
        "github_msg": git_msg
    })

# Soglia e frequenza del rinnovo automatico del token JWT DAZN.
# I token durano circa 2 ore. Con una frequenza di controllo pari a FREQ e una
# soglia SOGLIA, nel caso peggiore il rinnovo parte quando restano SOGLIA - FREQ
# secondi: con questi numeri il margine e' di 30 minuti, quindi il token non
# arriva mai a scadere. Prima controllo 1 ora + soglia 1 ora: margine ZERO, e
# il token poteva finire scaduto e finire sul fallback dai cookie.
SOGLIA_RINNOVO_S = 45 * 60
FREQ_CONTROLLO_S = 15 * 60


def refresh_all_dazn_sessions(force=False):
    """Rinnova i token JWT DAZN in background. Se force=False salta i profili
    il cui token e' ancora valido per piu' di SOGLIA_RINNOVO_S secondi."""
    import requests
    import time

    cfg = load_profiles_config()
    refreshed_any = False

    for pid in list(PROFILES.keys()):
        profile_dir = Path(get_active_chrome_profile(pid))
        if not profile_dir.exists():
            continue

        session_file = profile_dir / "dazn_session.json"
        auth_file = profile_dir / "auth_token.json"

        if not session_file.exists() and not auth_file.exists():
            continue

        current_jwt = ""
        session_data = {}

        if session_file.exists():
            try:
                session_data = json.loads(session_file.read_text(encoding="utf-8"))
                current_jwt = session_data.get("jwt", "")
            except Exception:
                pass

        if not current_jwt and auth_file.exists():
            try:
                auth_data = json.loads(auth_file.read_text(encoding="utf-8"))
                current_jwt = auth_data.get("jwt", "")
            except Exception:
                pass

        if not current_jwt or not current_jwt.startswith("eyJ"):
            continue

        # Rinnovo automatico SOLO quando il token e' sceso sotto la soglia
        remaining = None
        try:
            import base64 as _b64
            parts = current_jwt.split(".")
            if len(parts) >= 2:
                payload_b64 = parts[1]
                payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
                claims = json.loads(_b64.b64decode(payload_b64).decode("utf-8"))
                exp_ts = claims.get("exp")
                if exp_ts:
                    remaining = int(exp_ts - time.time())
                    if remaining > SOGLIA_RINNOVO_S and not force:
                        continue  # ancora oltre la soglia: nessun rinnovo necessario
                    print(f"[Auto-Refresh DAZN] Profilo {pid}: token sotto soglia, rinnovo "
                          f"({remaining // 60} min rimasti, soglia {SOGLIA_RINNOVO_S // 60} min)")
        except Exception:
            # Se non riesco a leggere la scadenza non posso fidarmi del token:
            # in caso di dubbio rinnovo, con force=False pero' lascio perdere
            # un ciclo di 15 minuti invece di chiamare l'API alla cieca.
            if not force and remaining is not None:
                continue

        try:
            # 1) Chiamata API ufficiale RefreshAccessToken DAZN
            res = requests.post(
                "https://ott-authz-bff-prod.ar.indazn.com/v5/RefreshAccessToken",
                headers={
                    "authorization": f"Bearer {current_jwt}",
                    "content-type": "application/json",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                },
                timeout=12
            )

            if res.status_code == 200:
                data = res.json()
                new_token = data.get("AuthToken", {}).get("Token")
                if new_token and new_token != current_jwt:
                    print(f"[Auto-Refresh DAZN] Token rinnovato con successo per il profilo {pid}!")
                    if session_file.exists():
                        session_data["jwt"] = new_token
                        session_data["created_at"] = int(time.time())
                        session_file.write_text(json.dumps(session_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                    auth_file.write_text(json.dumps({"jwt": new_token}), encoding="utf-8")
                    refreshed_any = True
            elif res.status_code == 401:
                print(f"[Auto-Refresh DAZN] Token per {pid} scaduto. Tentativo rinnovo da cookies...")
                cookies = session_data.get("cookies", [])
                if cookies:
                    try:
                        c_dict = {c["name"]: c["value"] for c in cookies if "name" in c and "value" in c}
                        r_rail = requests.get(
                            "https://stage-dazn-web-api.indazn.com/v2/rail?id=Home",
                            cookies=c_dict,
                            headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                            timeout=10
                        )
                        fresh_jwt = r_rail.headers.get("authorization") or r_rail.headers.get("dazn-token")
                        if fresh_jwt and fresh_jwt.startswith("Bearer "):
                            fresh_jwt = fresh_jwt.replace("Bearer ", "").strip()
                        if fresh_jwt and fresh_jwt.startswith("eyJ"):
                            print(f"[Auto-Refresh DAZN] Token ripristinato con successo dai cookies per {pid}!")
                            session_data["jwt"] = fresh_jwt
                            session_data["created_at"] = int(time.time())
                            session_file.write_text(json.dumps(session_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                            auth_file.write_text(json.dumps({"jwt": fresh_jwt}), encoding="utf-8")
                            refreshed_any = True
                    except Exception as e_c:
                        print(f"[Auto-Refresh DAZN] Errore rinnovo da cookies: {e_c}")
        except Exception as e:
            print(f"[Auto-Refresh DAZN Error] {pid}: {e}")

    if refreshed_any:
        sync_to_github("auto-refresh: rinnovo automatico token DAZN per i profili")

def _background_dazn_refresher():
    """Thread in sottofondo: verifica i token ogni FREQ_CONTROLLO_S e rinnova
    automaticamente SOLO i profili il cui token e' sceso sotto SOGLIA_RINNOVO_S.
    Con i valori correnti il margine minimo prima della scadenza e' di 30
    minuti, quindi il token non dovrebbe mai arrivare a scadere da solo."""
    print(f"[Auto-Refresh DAZN] Controllo ogni {FREQ_CONTROLLO_S // 60} min, "
          f"rinnovo sotto i {SOGLIA_RINNOVO_S // 60} min")
    time.sleep(15)
    while True:
        try:
            refresh_all_dazn_sessions()
        except Exception as e:
            print(f"[Auto-Refresh Loop Error] {e}")
        time.sleep(FREQ_CONTROLLO_S)

_refresher_thread = threading.Thread(target=_background_dazn_refresher, daemon=True)
_refresher_thread.start()

@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({"ok": True})

@app.route("/api/session-status", methods=["GET"])
def api_session_status():
    """Restituisce lo stato e la scadenza (exp) del token JWT DAZN del profilo attivo."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401

    pid = _current_pid()
    profile_dir = Path(get_active_chrome_profile(pid))
    session_file = profile_dir / "dazn_session.json"
    auth_file = profile_dir / "auth_token.json"

    jwt_token = ""
    if session_file.exists():
        try:
            sdata = json.loads(session_file.read_text(encoding="utf-8"))
            jwt_token = sdata.get("jwt", "")
        except Exception:
            pass

    if not jwt_token and auth_file.exists():
        try:
            adata = json.loads(auth_file.read_text(encoding="utf-8"))
            jwt_token = adata.get("jwt", "")
        except Exception:
            pass

    if not jwt_token or not jwt_token.startswith("eyJ"):
        return jsonify({"ok": False, "has_token": False, "exp": None, "remaining": 0})

    try:
        import base64
        parts = jwt_token.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1]
            # Fix base64 padding
            payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
            payload_bytes = base64.b64decode(payload_b64)
            payload = json.loads(payload_bytes.decode("utf-8"))
            exp = payload.get("exp")
            if exp:
                remaining = max(0, int(exp - time.time()))
                return jsonify({
                    "ok": True,
                    "has_token": True,
                    "exp": exp,
                    "remaining": remaining,
                    "profile": pid
                })
    except Exception as e:
        print(f"[Session Status Error] {e}")

    return jsonify({"ok": True, "has_token": True, "exp": None, "remaining": None, "profile": pid})

@app.route("/api/refresh-dazn-session", methods=["POST"])
def api_refresh_dazn_session():
    """Endpoint per sollecitare il rinnovo manuale o via cron/webhook della sessione DAZN."""
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    pid = _current_pid()
    try:
        # 1. Prova prima con il rinnovo API
        refresh_all_dazn_sessions(force=True)
        
        # 2. Verifica se il token per il profilo corrente è effettivamente valido
        profile_dir = Path(get_active_chrome_profile(pid))
        session_file = profile_dir / "dazn_session.json"
        auth_file = profile_dir / "auth_token.json"
        
        current_jwt = ""
        if session_file.exists():
            try:
                current_jwt = json.loads(session_file.read_text(encoding="utf-8")).get("jwt", "")
            except Exception:
                pass
        if not current_jwt and auth_file.exists():
            try:
                current_jwt = json.loads(auth_file.read_text(encoding="utf-8")).get("jwt", "")
            except Exception:
                pass
                
        is_expired = True
        if current_jwt and current_jwt.startswith("eyJ"):
            try:
                import base64
                p_b64 = current_jwt.split(".")[1]
                p_b64 += "=" * ((4 - len(p_b64) % 4) % 4)
                exp = json.loads(base64.b64decode(p_b64).decode("utf-8")).get("exp", 0)
                if exp > time.time():
                    is_expired = False
            except Exception:
                pass
                
        # 3. Se ancora scaduto, avvia il browser del profilo in background per prendere il nuovo token live da DAZN!
        if is_expired:
            print(f"[Refresh Session] Token per {pid} scaduto o non rinnovato da API: recupero da browser...")
            async def _renew_via_browser():
                from dazn_navigator2.services.browser import get_browser
                b = await get_browser(user_data_dir=profile_dir)
                if b:
                    await b.ensure_session()
                    jwt_browser = await b.evaluate("localStorage.getItem('MISL.authToken')")
                    if jwt_browser and jwt_browser.startswith("eyJ"):
                        import base64
                        p_b64 = jwt_browser.split(".")[1]
                        p_b64 += "=" * ((4 - len(p_b64) % 4) % 4)
                        b_exp = json.loads(base64.b64decode(p_b64).decode("utf-8")).get("exp", 0)
                        if b_exp > time.time():
                            s_data = {}
                            if session_file.exists():
                                try:
                                    s_data = json.loads(session_file.read_text(encoding="utf-8"))
                                except Exception:
                                    pass
                            s_data["jwt"] = jwt_browser
                            s_data["created_at"] = int(time.time())
                            session_file.write_text(json.dumps(s_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                            auth_file.write_text(json.dumps({"jwt": jwt_browser}), encoding="utf-8")
                            print(f"[Refresh Session] Token rinnovato con successo dal browser per {pid}!")
                            return True
                return False
                
            success = run_async(_renew_via_browser(), timeout=40)
            if not success:
                return jsonify({"ok": False, "error": "Impossibile rinnovare il token (cookie DAZN scaduti nel profilo)"}), 400

        return jsonify({"ok": True, "message": "Rinnovo sessione eseguito con successo!"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/events", methods=["GET"])
def get_saved_events():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401
    
    local_data = _load() or {}
    try:
        import requests
        url = "https://ace-seal-162556.upstash.io/get/stream:eventi_mpd"
        headers = {"Authorization": f"Bearer {_upstash_token()}"}
        res = requests.get(url, headers=headers, timeout=5)
        if res.ok:
            raw = res.json().get("result")
            if raw and raw != "null":
                upstash_data = json.loads(raw)
                if isinstance(upstash_data, dict):
                    # Teniamo upstash_data come fonte primaria
                    return jsonify(upstash_data)
    except Exception as e:
        print(f"[Upstash API Error] {e}")
    return jsonify(local_data)

@app.route("/api/events/repair-ua", methods=["POST"])
def api_repair_event_ua():
    """Riapplica l'User-Agent accettato dalla CDN a tutti gli eventi salvati.

    Il token CDN e' legato all'UA: se un evento e' stato estratto con un altro
    profilo TLS la CDN risponde 401 e Kodi non avvia il video.
    """
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    from dazn_navigator2.cli.eventi_cmds import ripara_user_agent
    data, report = ripara_user_agent()
    return jsonify({
        "ok": True,
        "checked": len(report),
        "fixed": [
            {"comp": c, "name": n, "old": o, "new": v} for c, n, o, v, ch in report if ch
        ],
        "unresolved": [
            {"comp": c, "name": n, "old": o} for c, n, o, v, ch in report if not v
        ],
    })

@app.route("/api/events/rename", methods=["POST"])
def rename_saved_event():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    body = request.get_json() or {}
    comp = body.get("comp")
    index = body.get("index")
    new_name = (body.get("new_name") or "").strip()
    old_name = body.get("old_name")
    
    if not new_name:
        return jsonify({"ok": False, "error": "Parametri non validi"}), 400
    
    data = _fetch_from_upstash()
    renamed = False
    
    if comp and comp in data and index is not None:
        idx = int(index)
        if 0 <= idx < len(data[comp]):
            data[comp][idx]["name"] = new_name
            renamed = True

    if not renamed and old_name:
        for c, items in data.items():
            for ev in items:
                if ev.get("name") == old_name:
                    ev["name"] = new_name
                    renamed = True
                    break
            if renamed:
                break

    if renamed:
        pubblica(f"Rinomina evento {new_name}", data, asincrono=True)
        return jsonify({"ok": True})
        
    return jsonify({"ok": False, "error": "Evento non trovato"}), 404

@app.route("/api/events/delete", methods=["POST"])
def delete_saved_event():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    body = request.get_json(silent=True) or {}
    
    # 1. Cancellazione totale
    if body.get("all"):
        pubblica("Cancellati tutti gli eventi", {}, asincrono=False)
        return jsonify({"ok": True})
    
    comp = (body.get("comp") or "").strip()
    index = body.get("index")
    ev_name = (body.get("name") or "").strip()
    
    # Prendi lo stato corrente da dazn_event.json / Upstash
    data = _fetch_from_upstash() or {}
    removed = False

    # 1. Cancellazione per Nome (più affidabile e univoco)
    if ev_name:
        for c, items in list(data.items()):
            if comp and c.strip().lower() != comp.lower():
                continue
            orig_len = len(items)
            new_items = [e for e in items if (e.get("name") or "").strip() != ev_name]
            if len(new_items) < orig_len:
                if new_items:
                    data[c] = new_items
                else:
                    del data[c]
                removed = True
                break
        
        # Se non trovato con filtro comp, cerca in tutte le categorie
        if not removed:
            for c, items in list(data.items()):
                orig_len = len(items)
                new_items = [e for e in items if (e.get("name") or "").strip() != ev_name]
                if len(new_items) < orig_len:
                    if new_items:
                        data[c] = new_items
                    else:
                        del data[c]
                    removed = True
                    break

    # 2. Cancellazione per Categoria + Indice (se nome non specificato o fallback)
    if not removed and comp and index is not None:
        idx = int(index)
        # Cerca comp esatto o case-insensitive
        target_comp = None
        if comp in data:
            target_comp = comp
        else:
            for c in data.keys():
                if c.strip().lower() == comp.lower():
                    target_comp = c
                    break
        
        if target_comp and 0 <= idx < len(data[target_comp]):
            del data[target_comp][idx]
            if not data[target_comp]:
                del data[target_comp]
            removed = True

    if removed:
        pubblica("Rimosso evento", data, asincrono=False)
        return jsonify({"ok": True, "data": data})
        
    return jsonify({"ok": False, "error": "Evento non trovato"}), 404

@app.route("/api/events/sort", methods=["POST"])
def sort_saved_events():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    from dazn_navigator2.cli.eventi_cmds import _iter_entries, _sort_key
    data = _fetch_from_upstash()
    entries = list(_iter_entries(data))
    if not entries:
        return jsonify({"ok": True})
    
    ordinato = sorted(entries, key=_sort_key)
    nuovo_data = {}
    for _, comp, _, ev_ in ordinato:
        nuovo_data.setdefault(comp, []).append(ev_)
    pubblica("Eventi riordinati per data", nuovo_data, asincrono=True)
    return jsonify({"ok": True})

@app.route("/api/events/pool", methods=["GET"])
def event_pool():
    """Eventi uniti delle due fonti del menu Pz8 (tasti 3 e 4), solo canali MPD.

    Ogni voce e' UN evento con dentro tutti i suoi canali DASH: i due tasti
    inviano gli stessi eventi in due formati diversi, quindi vengono normalizzati
    e deduplicati per id. m3u8/HLS e link senza manifest DASH sono esclusi.
    """
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    try:
        from dazn_navigator2.cli.eventi_cmds import build_event_pool
        pool, stats = build_event_pool()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Errore nel pool eventi: {e}"}), 500
    return jsonify({"ok": True, "events": pool, "stats": stats})

@app.route("/api/events/add-from-pool", methods=["POST"])
def add_event_from_pool():
    """Inserisce in Eventi Estratti UN canale specifico di un evento del pool.

    Il canale arriva dal client, quindi viene riverificato contro il pool letto
    dal server: non si puo' cosi' infilare un m3u8 o un URL arbitrario. 'mpd' viene
    ricalcolato dal canale scelto e non preso dalla richiesta.
    """
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    body = request.get_json() or {}
    event_id = body.get("id")
    channel_index = body.get("channel_index")
    if event_id is None or channel_index is None:
        return jsonify({"ok": False, "error": "Parametri non validi"}), 400

    try:
        from dazn_navigator2.cli.eventi_cmds import build_event_pool, pool_entry
        pool, _ = build_event_pool()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Errore nel pool eventi: {e}"}), 500

    target = None
    for ev in pool:
        if str(ev.get("id")) == str(event_id):
            target = ev
            break
    if not target:
        return jsonify({"ok": False, "error": "Evento non trovato nel pool"}), 404

    try:
        idx = int(channel_index)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Canale non valido"}), 400
    if not (0 <= idx < len(target["channels"])):
        return jsonify({"ok": False, "error": "Canale non valido"}), 400

    channel = target["channels"][idx]
    entry = pool_entry(target, channel)
    if not entry:
        return jsonify({"ok": False, "error": "Il canale selezionato non e' in formato MPD"}), 400

    from dazn_navigator2.cli.eventi_cmds import add_event
    competition = target.get("category") or "Eventi"
    add_event(competition, entry, _current_pid(), asincrono=True)
    return jsonify({
        "ok": True,
        "event": entry["name"],
        "channel": channel["name"],
        "competition": competition,
    })

@app.route("/api/events/sync_next", methods=["POST"])
def sync_events_to_next():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401
    
    import re, unicodedata, requests
    
    pid = _current_pid()
    local_data = _load(pid)
    # Se il profilo corrente non ha eventi o è diverso da mpd, consideriamo anche dazn_event_mpd.json se esiste
    if not local_data and pid != "mpd":
        local_data = _load("mpd")
    
    if not local_data:
        return jsonify({"ok": False, "error": "Nessun evento estratto da sincronizzare"}), 400

    upstash_url = "https://ace-seal-162556.upstash.io"
    headers = {"Authorization": f"Bearer {_upstash_token()}"}
    
    try:
        r = requests.get(f"{upstash_url}/get/stream:eventi", headers=headers, timeout=10)
        res_json = r.json()
        raw_val = res_json.get("result")
        next_data = json.loads(raw_val) if raw_val else {}
    except Exception as e:
        return jsonify({"ok": False, "error": f"Errore lettura Next DB: {e}"}), 500

    def _normalize_title(s: str) -> str:
        if not s:
            return ""
        s = re.sub(r'[\(\[\{].*?[\)\]\}]', ' ', s) # rimuove (WARP), ecc.
        s = s.replace("vs.", " ").replace("vs", " ").replace("-", " ").replace("|", " ")
        s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('utf-8')
        words = sorted([w for w in re.sub(r'[^a-zA-Z0-9]', ' ', s).lower().split() if w])
        return " ".join(words)

    updated_count = 0
    added_count = 0

    for comp, items in local_data.items():
        if not isinstance(items, list):
            continue
        for ev in items:
            ev_name = ev.get("name", "").strip()
            norm_name = _normalize_title(ev_name)
            if not norm_name:
                continue

            matched_item = None
            
            # Cerca prima nella stessa competizione (o competizione simile)
            candidate_categories = [comp] if comp in next_data else []
            candidate_categories += [c for c in next_data.keys() if c != comp]

            for cat_key in candidate_categories:
                for target_ev in next_data.get(cat_key, []):
                    t_name = target_ev.get("name", "").strip()
                    norm_t = _normalize_title(t_name)
                    if not norm_t:
                        continue
                    
                    # Match esatto dei token oppure inclusione sicura
                    if norm_name == norm_t:
                        matched_item = target_ev
                        break
                    set_a = set(norm_name.split())
                    set_b = set(norm_t.split())
                    if len(set_a) >= 2 and set_a.issubset(set_b) or (len(set_b) >= 2 and set_b.issubset(set_a)):
                        matched_item = target_ev
                        break
                if matched_item:
                    break

            if matched_item:
                # AGGIORNA SOLO I PARAMETRI DELLO STREAM, PRESERVANDO IMMAGINE E NOME ESISTENTI!
                if ev.get("mpd"):
                    matched_item["mpd"] = ev["mpd"]
                    matched_item["url"] = ev["mpd"]
                if ev.get("key"):
                    matched_item["key"] = ev["key"]
                    matched_item["kid_key"] = ev["key"]
                if ev.get("ua"):
                    matched_item["ua"] = ev["ua"]
                if ev.get("start") and not matched_item.get("start"):
                    matched_item["start"] = ev["start"]
                if ev.get("end") and not matched_item.get("end"):
                    matched_item["end"] = ev["end"]
                updated_count += 1
            else:
                # Evento non trovato in Next: aggiungilo nella categoria corrispondente
                next_data.setdefault(comp, []).append(ev)
                added_count += 1

    # Salva il dizionario unificato su Upstash Redis stream:eventi
    try:
        payload = json.dumps(next_data, ensure_ascii=False)
        save_res = requests.post(f"{upstash_url}/set/stream:eventi", headers=headers, data=payload, timeout=10)
        if save_res.status_code != 200:
            return jsonify({"ok": False, "error": f"Errore scrittura Redis: {save_res.text}"}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": f"Errore salvataggio Next DB: {e}"}), 500

    # Sincronizza anche localmente su next/public/test.json, htdocs/test.json e kodi_repo/test.json (AES-256-GCM)
    try:
        import os
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.backends import default_backend
        import base64

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"zadonkais_secure_salt_2026",
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive("2941".encode("utf-8"))
        aesgcm = AESGCM(key)
        iv = os.urandom(12)
        plaintext = json.dumps(next_data, ensure_ascii=False).encode("utf-8")
        ciphertext = aesgcm.encrypt(iv, plaintext, None)
        enc_payload = base64.b64encode(iv + ciphertext).decode("ascii")
        json_enc_text = json.dumps({"enc": enc_payload}, indent=2, ensure_ascii=False) + "\n"

        target_paths = [
            Path(r"C:\Users\alecl\Desktop\next\public\test.json"),
            Path(r"C:\Users\alecl\Desktop\htdocs\test.json"),
            Path(r"C:\Users\alecl\Desktop\kodi_repo\test.json")
        ]
        for tp in target_paths:
            if tp.parent.exists():
                try:
                    tp.write_text(json_enc_text, encoding="utf-8")
                except Exception:
                    pass
    except Exception as e:
        print(f"[Sync test.json Local Error] {e}")

    return jsonify({
        "ok": True,
        "updated": updated_count,
        "added": added_count,
        "message": f"Sincronizzazione completata: {updated_count} eventi aggiornati con chiavi/mpd, {added_count} nuovi aggiunti."
    })


@app.route("/api/live", methods=["GET"])
def get_live_events():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401

    async def _fetch():
        explorer = DaznExplorer()
        tiles = await explorer.get_tiles("Live")
        items = [_format_tile_item(t) for t in tiles]
        # get_tiles restituisce [] sia quando DAZN risponde e non c'e' niente
        # in onda, sia quando DAZN ha risposto con un errore. Sono due
        # situazioni opposte: la prima e' normale e non deve sembrare un
        # guasto, la seconda va segnalata come tale.
        err = getattr(explorer, "last_error", None)
        await explorer.close()
        return items, err

    try:
        data, err = run_async(_fetch(), timeout=45)
        if err and not data:
            return jsonify({"error": f"DAZN ha risposto con un errore: {err}"}), 502
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/vod", methods=["GET"])
def get_vod_events():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401

    async def _fetch():
        explorer = DaznExplorer()
        tiles = await explorer.get_tiles("Catchup")
        items = [_format_tile_item(t) for t in tiles]
        await explorer.close()
        return items

    try:
        data = run_async(_fetch(), timeout=45)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/vod/categories", methods=["GET"])
def get_vod_categories():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401

    global _VOD_CACHE, _VOD_CACHE_TIME
    with _VOD_CACHE_LOCK:
        if _VOD_CACHE is not None and (time.monotonic() - _VOD_CACHE_TIME) < _VOD_CACHE_TTL:
            return jsonify(_VOD_CACHE)

    async def _fetch():
        explorer = DaznExplorer()
        res = await explorer.get_vod_categories()
        await explorer.close()

        ultimi = [_format_tile_item(t) for t in res.get("ultimi", [])]

        categorie = {}
        for t in res.get("all", []):
            item = _format_tile_item(t)
            cname = item.get("competition") or "Altro"
            categorie.setdefault(cname, []).append(item)

        return {"ultimi": ultimi, "categorie": categorie}

    try:
        data = run_async(_fetch(), timeout=150)
        with _VOD_CACHE_LOCK:
            _VOD_CACHE = data
            _VOD_CACHE_TIME = time.monotonic()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/linear", methods=["GET"])
def get_linear_channels():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401

    async def _fetch():
        explorer = DaznExplorer()
        tiles = await explorer.get_tiles("epg")
        if not tiles:
            tiles = await explorer.get_tiles("LinearChannels")
        items = [_format_tile_item(t) for t in tiles]
        
        # Risolvi per ciascun canale il corrispettivo tile 'Live' da search per garantire l'AssetId abilitato
        for it in items:
            t_name = it.get("title", "").strip()
            try:
                s_res = await explorer.search(t_name)
                match = [x for x in s_res if x.tile_type == 'Live' and x.title.strip().lower() == t_name.lower()]
                if match:
                    it["asset_id"] = match[0].asset_id or match[0].id
                    if match[0].image:
                        it["image"] = _image_url(match[0].image)
                    if match[0].raw.get("Start"):
                        it["start"] = _to_local_time(match[0].raw.get("Start"))
                    if match[0].raw.get("End"):
                        it["end"] = _to_local_time(match[0].raw.get("End"))
            except Exception:
                pass

        await explorer.close()
        return items

    try:
        data = run_async(_fetch(), timeout=45)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route("/api/search", methods=["GET"])
def search_events():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401

    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify([])

    async def _fetch():
        explorer = DaznExplorer()
        tiles = await explorer.search(q)
        items = [_format_tile_item(t) for t in tiles]
        await explorer.close()
        return items

    try:
        data = run_async(_fetch(), timeout=45)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/worker-option", methods=["GET", "POST"])
def worker_option_api():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        enabled = bool(payload.get("enabled", False))
        set_worker_option_enabled(enabled)
        return jsonify({"ok": True, "enabled": is_worker_option_enabled()})
    return jsonify({"enabled": is_worker_option_enabled()})

@app.route("/api/extraction-method", methods=["GET", "POST"])
def extraction_method_api():
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401
    from dazn_navigator2.settings import get_extraction_method, set_extraction_method
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        method = str(payload.get("method") or "").strip().lower()
        if method not in ("web", "mobile"):
            return jsonify({"ok": False, "error": "Metodo non valido (web/mobile)"}), 400
        set_extraction_method(method)
        return jsonify({"ok": True, "method": get_extraction_method()})
    return jsonify({"method": get_extraction_method()})

@app.route("/api/diagnose")
def diagnose():
    # Questa route restituiva, a CHIUNQUE, anche senza sessione: le chiavi
    # dell'oggetto di risposta sono i nomi dei profili, che sono anche le
    # password di login (cfr. /login), piu' i percorsi assoluti del disco e i
    # device_id dei token. Ora e' chiusa come tutte le altre route dati.
    if "user_profile_id" not in session:
        return jsonify({"error": "Non autenticato"}), 401
    import time as _time
    now = _time.time()
    from dazn_navigator2.services.extractor import _CACHED_SERVICES
    results = {
        "_playback_endpoint": _CACHED_SERVICES.get("Playback", ""),
    }
    for pid in PROFILES:
        profile_dir = Path(get_active_chrome_profile(pid))
        auth_file = profile_dir / "auth_token.json"
        info = {"profile_dir": str(profile_dir), "auth_file_exists": auth_file.exists()}
        if auth_file.exists():
            try:
                data = json.loads(auth_file.read_text(encoding="utf-8"))
                tok = data.get("jwt", "")
                ext = HeadlessExtractor.__new__(HeadlessExtractor)
                pl = ext._decode_jwt_payload(tok) if tok.startswith("eyJ") else None
                if pl:
                    remaining = int(pl.get("exp", 0) - now)
                    info["country"] = pl.get("country")
                    info["exp"] = pl.get("exp")
                    info["remaining_seconds"] = remaining
                    info["valid_it"] = pl.get("country") == "it" and remaining > 0
                    info["device_id"] = (pl.get("deviceId") or "")[:40]
                else:
                    info["token_parse_error"] = True
            except Exception as e:
                info["error"] = str(e)
        results[pid] = info
    return jsonify(results)

@app.route("/api/extract", methods=["POST"])
def extract_stream():
    if "user_profile_id" not in session:
        return jsonify({"ok": False, "error": "Non autenticato"}), 401

    pid = session["user_profile_id"]
    global _LAST_PID
    _LAST_PID = pid
    body = request.get_json() or {}
    asset_id = body.get("asset_id") or body.get("id")
    title = body.get("title", "Evento")
    image = body.get("image", "")
    competition = (body.get("competition") or "").strip()
    start = body.get("start") or ""
    end = body.get("end") or ""

    if not asset_id:
        return jsonify({"ok": False, "error": "asset_id mancante"}), 400

    target_profile_dir = get_active_chrome_profile(pid)

    async def _do_extract():
        nonlocal competition, start, end
        # Rilevamento automatico se si tratta di canale lineare
        title_lower = (title or "").strip().lower()
        is_linear_chan = any(k in title_lower for k in ("dazn 1", "dazn 2", "dazn 3", "dazn 4", "dazn 5", "eurosport", "zona dazn", "milan tv", "inter tv", "juve tv"))

        # Risoluzione metadati e categoria reale se mancante o generica
        if not start or not end or not competition or competition in ("Eventi Live", "Eventi"):
            if is_linear_chan:
                competition = "Live TV"
            else:
                try:
                    explorer = DaznExplorer()
                    s_res = await explorer.search(title)
                    for match_item in s_res:
                        raw = getattr(match_item, 'raw', {}) or {}
                        c_title = ""
                        comp_raw = raw.get("Competition")
                        if isinstance(comp_raw, dict):
                            c_title = comp_raw.get("Title") or ""
                        elif comp_raw:
                            c_title = str(comp_raw)

                        if c_title and c_title not in ("Eventi Live", "Eventi"):
                            competition = c_title
                            if not start and raw.get("Start"):
                                start = _to_local_time(raw.get("Start"))
                            if not end and raw.get("End"):
                                end = _to_local_time(raw.get("End"))
                            break

                    if not competition or competition in ("Eventi Live", "Eventi"):
                        for match_item in s_res:
                            raw = getattr(match_item, 'raw', {}) or {}
                            s_title = ""
                            sport_raw = raw.get("Sport")
                            if isinstance(sport_raw, dict):
                                s_title = sport_raw.get("Title") or ""
                            elif sport_raw:
                                s_title = str(sport_raw)
                            if s_title:
                                competition = s_title
                                break
                    await explorer.close()
                except Exception:
                    pass

        if not competition:
            competition = "Live TV" if is_linear_chan else "Eventi"

        ext = HeadlessExtractor()
        res = await ext.estrai(target_profile_dir, asset_id, title)
        if res.get("ok"):
            mpd_url = res.get("mpd_url", "")
            dazn_token = res.get("dazn_token", "")
            cdn_name = res.get("cdn_name", "dazn-token")
            mpd_auth = _build_mpd_auth(mpd_url, dazn_token, cdn_name)
            keys_str = ",".join(res.get("keys", []))
            from dazn_navigator2.services.extractor import detect_user_agent
            ua_str = (res.get("ua") or "").strip() or detect_user_agent()
            logo = image or _image_url(res.get("image"))
            # Se il frontend non ha mandato la locandina, la si recupera dai
            # rail usando l'asset_id: senza questo la scheda resta senza img.
            if not logo:
                try:
                    logo = await _resolve_image_by_asset(asset_id, title)
                except Exception:
                    logo = ""

            base_titolo = (title or "").strip() or res.get("titolo") or "Evento"
            event_name = normalize_event_title(base_titolo)

            # Canali lineari / Live TV: start ed end devono essere sempre VUOTI
            if is_linear_chan or competition == "Live TV" or "3000-" in str(end):
                ev_start = ""
                ev_end = ""
            else:
                ev_start = _to_local_time(start)
                ev_end = _to_local_time(end)
                if not ev_start:
                    from datetime import datetime as _dt_start
                    ev_start = _dt_start.now().strftime("%Y-%m-%dT%H:%M:%S")
                if not ev_end:
                    try:
                        from dazn_navigator2.services.playlist_ed import token_expiry as _token_expiry
                        _exp_dt = _token_expiry(dazn_token)
                        if _exp_dt:
                            ev_end = _exp_dt.strftime("%Y-%m-%dT%H:%M:%S")
                    except Exception:
                        pass

            # Versione WARP (Diretto pulito)
            from dazn_navigator2.settings import get_extraction_method
            _meth = get_extraction_method()
            entry_warp = {
                "name": event_name,
                "image": logo,
                "start": ev_start,
                "end": ev_end,
                "mpd": mpd_auth,
                "key": keys_str,
                "ua": ua_str,
                "method": _meth
            }

            if is_worker_option_enabled():
                worker_prefix = get_next_worker_prefix()
                final_mpd_worker = f"{worker_prefix}{urllib.parse.quote(mpd_auth, safe='')}&ua={urllib.parse.quote(ua_str, safe='')}"
                entry_worker = {
                    "name": event_name,
                    "image": logo,
                    "start": ev_start,
                    "end": ev_end,
                    "mpd": final_mpd_worker,
                    "key": keys_str,
                    "ua": ua_str,
                    "method": _meth
                }
                events_to_add = [entry_worker]
            else:
                events_to_add = [entry_warp]

            add_events(competition, events_to_add, _current_pid(), asincrono=True)
            sync_to_github(f"extract: salvato evento {event_name} ({_current_pid()})")
            res["mpd_url"] = mpd_auth
            res["mpd_auth"] = mpd_auth
            res["entry"] = entry_warp
            res["competition"] = competition
        return res

    try:
        result = run_async(_do_extract(), timeout=180)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



def _auto_extract_worker():
    """Scansiona e estrae automaticamente e istantaneamente tutti gli eventi Live delle competizioni target."""
    global _LBA_LAST_AUTO_RUN
    import time
    import re
    from datetime import datetime, timezone
    
    TARGET_COMPETITIONS = {
        "serie a enilive",
        "serie bkt",
        "laliga ea sports"
    }
    
    time.sleep(5)  # Attende l'avvio completo del server
    print("[AutoExtract] Servizio estrazione automatica competizioni attivo (Serie A, Serie B, LaLiga).")
    
    while True:
        if time.time() < _PROFILE_BUSY_UNTIL:
            # Operazione sul profilo in corso (upload/delete): aspetta senza interferire
            time.sleep(5)
            continue
        try:
            async def _check_and_extract():
                pid = "mpd"  # Salva direttamente nel profilo mpd (sincronizzato con stream:eventi_mpd)
                target_profile_dir = get_active_chrome_profile(pid)
                if not target_profile_dir:
                    return
                
                # Leggi eventi gia' estratti
                current_data = _load(pid)
                extracted_names = set()
                for comp, ev_list in current_data.items():
                    for ev in ev_list:
                        n = (ev.get("name") or "").strip().lower()
                        if n and (ev.get("mpd") or ev.get("manifest")):
                            extracted_names.add(n)
                
                explorer = DaznExplorer()
                # Recupera prima di tutto i live dalla Rail ufficiale 'Live' (stessa sorgente della UI)
                tiles = []
                try:
                    live_rail = await explorer.get_tiles("Live")
                    tiles.extend(live_rail)
                except Exception:
                    pass
                for query in ("Serie A", "Serie B", "LaLiga"):
                    try:
                        res = await explorer.search(query)
                        tiles.extend(res)
                    except Exception:
                        pass
                
                now = datetime.now(timezone.utc)
                candidates = []
                seen_assets = set()
                
                for t in tiles:
                    asset_id = t.asset_id or t.id
                    if not asset_id or asset_id in seen_assets:
                        continue
                    seen_assets.add(asset_id)
                    
                    raw = getattr(t, "raw", {}) or {}
                    comp_raw = raw.get("Competition", {})
                    c_title = comp_raw.get("Title") if isinstance(comp_raw, dict) else str(comp_raw or "")
                    c_title_clean = c_title.strip().lower()
                    
                    if c_title_clean not in TARGET_COMPETITIONS:
                        continue
                    
                    # Controlla se e' in corso / live
                    start_str = raw.get("Start") or ""
                    end_str = raw.get("End") or ""
                    is_live = False
                    
                    if t.tile_type == "Live":
                        is_live = True
                    elif start_str:
                        try:
                            st_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                            if end_str:
                                en_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                                is_live = (st_dt <= now <= en_dt)
                            else:
                                # Se iniziato da meno di 3 ore
                                is_live = (st_dt <= now and (now - st_dt).total_seconds() < 10800)
                        except Exception:
                            pass
                    
                    if not is_live:
                        continue
                    
                    # Verifica se e' gia' stato estratto con titolo uniformato
                    ev_title = t.title.strip()
                    clean_title = normalize_event_title(ev_title)
                    # Normalizza anche i nomi gia' estratti rimuovendo suffissi come TEST

                    clean_for_check = _norm_for_dedup(clean_title)
                    existing_normalized = {_norm_for_dedup(n) for n in extracted_names}
                    if clean_for_check and clean_for_check in existing_normalized:
                        continue
                    # Match fuzzy: se un nome esistente contiene o e' contenuto nel candidato, salta
                    if clean_for_check and any(clean_for_check in ex or ex in clean_for_check for ex in existing_normalized if ex):
                        continue
                    
                    candidates.append({
                        "asset_id": asset_id,
                        "title": clean_title,
                        "competition": c_title,
                        "image": _image_url(t.image),
                        "start": _to_local_time(start_str),
                        "end": _to_local_time(end_str)
                    })
                
                await explorer.close()

                # Deduplica e ordina i candidati:
                # Se è presente sia "Fantacalcio Night | Squadra 1 - Squadra 2" che "Squadra 1 - Squadra 2",
                # scarta la versione base e tieni solo quella Fantacalcio Night.
                filtered_candidates = []
                all_cand_titles = [c["title"].lower() for c in candidates]
                for cand in candidates:
                    c_low = cand["title"].lower()
                    # Se questo è un titolo base (senza fantacalcio night) ma esiste una versione Fantacalcio Night per la stessa partita:
                    has_fantacalcio_counterpart = any(
                        "fantacalcio night" in other and c_low in other
                        for other in all_cand_titles
                    )
                    if has_fantacalcio_counterpart and "fantacalcio night" not in c_low:
                        print(f"[AutoExtract] Scartata versione base '{cand['title']}' in favore di Fantacalcio Night")
                        continue
                    filtered_candidates.append(cand)
                
                # Estrai ciascun evento mancante
                for cand in filtered_candidates:
                    print(f"[AutoExtract] Rilevato evento LIVE da estrarre: {cand['title']} ({cand['competition']})")
                    try:
                        ext = HeadlessExtractor()
                        res = await asyncio.wait_for(ext.estrai(target_profile_dir, cand["asset_id"], cand["title"]), timeout=120)
                        if res.get("ok"):
                            mpd_url = res.get("mpd_url", "")
                            dazn_token = res.get("dazn_token", "")
                            mpd_auth = _build_mpd_auth(mpd_url, dazn_token)
                            keys_str = ",".join(res.get("keys", []))
                            from dazn_navigator2.services.extractor import detect_user_agent
                            ua_str = (res.get("ua") or "").strip() or detect_user_agent()
                            logo = cand["image"] or _image_url(res.get("image"))
                            
                            ev_start = _to_local_time(cand["start"])
                            ev_end = _to_local_time(cand["end"])
                            # Fallback: nessun evento deve restare senza scadenza.
                            if not ev_start:
                                from datetime import datetime as _dt_start
                                ev_start = _dt_start.now().strftime("%Y-%m-%dT%H:%M:%S")
                            if not ev_end:
                                try:
                                    from dazn_navigator2.services.playlist_ed import token_expiry as _token_expiry
                                    _exp_dt = _token_expiry(dazn_token)
                                    if _exp_dt:
                                        ev_end = _exp_dt.strftime("%Y-%m-%dT%H:%M:%S")
                                except Exception:
                                    pass

                            # Versione WARP (Diretto pulito)
                            from dazn_navigator2.settings import get_extraction_method
                            _meth = get_extraction_method()
                            entry_warp = {
                                "name": cand["title"],
                                "image": logo,
                                "start": ev_start,
                                "end": ev_end,
                                "mpd": mpd_auth,
                                "key": keys_str,
                                "ua": ua_str,
                                "method": _meth
                            }

                            if is_worker_option_enabled():
                                worker_prefix = get_next_worker_prefix()
                                final_mpd_worker = f"{worker_prefix}{urllib.parse.quote(mpd_auth, safe='')}&ua={urllib.parse.quote(ua_str, safe='')}"
                                entry_worker = {
                                    "name": cand["title"],
                                    "image": logo,
                                    "start": ev_start,
                                    "end": ev_end,
                                    "mpd": final_mpd_worker,
                                    "key": keys_str,
                                    "ua": ua_str,
                                    "method": _meth
                                }
                                events_to_add = [entry_worker]
                            else:
                                events_to_add = [entry_warp]

                            add_events(cand["competition"], events_to_add, pid, asincrono=True)
                            sync_to_github(f"auto-extract: {cand['title']} ({cand['competition']})")
                            print(f"[AutoExtract] Estratto e sincronizzato con successo: {cand['title']}")
                            extracted_names.add(cand["title"].lower())
                    except Exception as err:
                        print(f"[AutoExtract Error] Errore estrazione {cand['title']}: {err}")
                    time.sleep(2)
            
            run_async(_check_and_extract(), timeout=900)  # 15 min: copre fino a ~7 eventi × 120 s
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f"[AutoExtract Loop Error] {type(e).__name__}: {e}", flush=True)

        # -------------------------------------------------------------
        # AUTO-EXTRACT LBA TV (LIVE EVENTS)
        # -------------------------------------------------------------
        try:
            from dazn_navigator2.services import lba_extractor
            # Auto-extract LBA disabilitato: si estrae solo su richiesta (UI),
            # cosi' in 'LBA TV' resta unicamente l'evento scelto dall'utente.
            if not _LBA_AUTO_EXTRACT_ENABLED:
                time.sleep(45)
                continue
            # Esegui l'auto-extract LBA al massimo ogni 120s: il backend DGE
            # rate-limita le richieste ravvicinate (401 "Invalid timestamp").
            if time.time() - _LBA_LAST_AUTO_RUN < 120:
                time.sleep(45)
                continue
            _LBA_LAST_AUTO_RUN = time.time()
            cat = lba_extractor.get_catalog()
            live_items = cat.get("live", [])
            if live_items:
                ev_file = BASE_DIR / "dazn_event.json"
                cat_data = {}
                if ev_file.exists():
                    try:
                        cat_data = json.loads(ev_file.read_text(encoding="utf-8"))
                    except Exception:
                        cat_data = {}
                if "LBA TV" not in cat_data:
                    cat_data["LBA TV"] = []
                
                # Nomi già presenti con chiave non vuota
                existing_lba = {e.get("lba_id"): e for e in cat_data["LBA TV"] if e.get("lba_id")}
                lba_aggiornato = False
                _extracted_this_cycle = 0

                for l_it in live_items:
                    l_id = l_it.get("id")
                    if not l_id:
                        continue
                    # Max 1 estrazione per ciclo: non saturare l'API.
                    if _extracted_this_cycle >= 1:
                        break
                    # Se già estratto con chiave valida, salta.
                    curr_entry = existing_lba.get(l_id)
                    if curr_entry and curr_entry.get("key") and curr_entry.get("mpd"):
                        continue

                    print(f"[AutoExtract LBA] Rilevato evento LIVE LBA da estrarre: {l_it.get('title')} (ID: {l_id})")
                    if time.time() < _LBA_AUTO_FAIL_UNTIL.get(l_id, 0):
                        continue
                    res, err = lba_extractor.estrai_stream_e_chiave(l_id, is_vod=False)
                    if res and res.get("mpd"):
                        ev_obj = {
                            "name": l_it.get("title") or f"LBA Event {l_id}",
                            "image": l_it.get("image") or "",
                            "start": l_it.get("start") or "",
                            "end": "",
                            "mpd": res.get("mpd"),
                            "key": res.get("key") or "",
                            "ua": res.get("ua") or "",
                            "referer": res.get("referer") or "",
                            "lba_id": l_id,
                            "is_vod": False
                        }
                        # Rimuovi eventuale duplicato per nome/id e inserisci
                        cat_data["LBA TV"] = [e for e in cat_data["LBA TV"] if e.get("lba_id") != l_id and e.get("name") != ev_obj["name"]]
                        cat_data["LBA TV"].append(ev_obj)
                        lba_aggiornato = True
                        print(f"[AutoExtract LBA] Estratto con successo: {ev_obj['name']} | Key: {ev_obj['key'][:16]}...")
                    else:
                        print(f"[AutoExtract LBA Error] ID {l_id}: {err}")
                        # Rate limit: backoff lungo. Evento non live/offline: 5 min.
                        if "401" in str(err):
                            _LBA_AUTO_FAIL_UNTIL[l_id] = time.time() + 180
                        else:
                            _LBA_AUTO_FAIL_UNTIL[l_id] = time.time() + 300
                    _extracted_this_cycle += 1
                    time.sleep(15)

                if lba_aggiornato:
                    ev_file.write_text(json.dumps(cat_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                    try:
                        from dazn_navigator2.cli.eventi_cmds import pubblica
                        pubblica("AutoExtract LBA Live", cat_data, asincrono=True)
                        print("[AutoExtract LBA] Sincronizzato su Upstash e JSON locale.")
                    except Exception as e_pub:
                        print(f"[AutoExtract LBA Sync Error] {e_pub}")
        except Exception as e_lba:
            print(f"[AutoExtract LBA Loop Error] {e_lba}")
        
        time.sleep(45)  # Riesegue il controllo ogni 45 secondi

# Avvia il worker in background
_auto_thread = threading.Thread(target=_auto_extract_worker, daemon=True)
_auto_thread.start()


# (Keepalive MPD LBA rimosso: si usa l'estrazione normale con MPD diretto.)

# Watchdog del loop asyncio: se si blocca, lo ricrea
def _async_watchdog():
    import time
    while True:
        time.sleep(60)
        try:
            run_async(asyncio.sleep(0), timeout=5)
        except Exception:
            global _ASYNC_LOOP, _ASYNC_THREAD
            with _LOOP_LOCK:
                try:
                    if _ASYNC_LOOP is not None:
                        _ASYNC_LOOP.stop()
                except Exception:
                    pass
                _ASYNC_LOOP = asyncio.new_event_loop()
                _ASYNC_THREAD = threading.Thread(target=_start_background_loop, args=(_ASYNC_LOOP,), daemon=True)
                _ASYNC_THREAD.start()
            try:
                from dazn_navigator2.services.browser import _browser_instance
                # reset browser instance
                import dazn_navigator2.services.browser as _bm
                _bm._browser_instance = None
            except Exception:
                pass
            print("[Watchdog] Loop asyncio ricreato", flush=True)

threading.Thread(target=_async_watchdog, daemon=True).start()

# Supervisore del thread auto-extract: se muore, lo riavvia
def _auto_extract_supervisor():
    import time
    global _auto_thread
    while True:
        time.sleep(30)
        if _auto_thread is None or not _auto_thread.is_alive():
            try:
                _auto_thread = threading.Thread(target=_auto_extract_worker, daemon=True)
                _auto_thread.start()
                print("[Supervisor] Auto-extract thread riavviato", flush=True)
            except Exception as e:
                print(f"[Supervisor] Errore riavvio: {e}", flush=True)
threading.Thread(target=_auto_extract_supervisor, daemon=True).start()


def _profilo_con_token():
    """Primo profilo che ha un JWT italiano valido salvato.

    Serve al warmup: avviare il contesto del profilo sbagliato e' inutile
    (il server licenze risponde 403 "Device not allowed" con i cookie di un
    altro profilo) e costa ~6s per niente. Si prova col lettore token
    dell'estrattore, che e' quello che userà davvero l'estrazione.
    """
    from dazn_navigator2.services.extractor import HeadlessExtractor
    for key in PROFILES:
        try:
            d = get_active_chrome_profile(key)
        except Exception:
            continue
        if not d or not Path(d).exists():
            continue
        try:
            if HeadlessExtractor()._read_jwt_from_disk(Path(d)):
                return d
        except Exception:
            continue
    return None


def _bg_profile_dir(pid=None):
    """Directory del profilo risolta SENZA la sessione Flask.

    Serve ai thread in background (warmup, auto-extract): li' non esiste una
    richiesta attiva, quindi session['user_profile_id'] solleva. Si usa
    l'ultimo profilo usato dal sito e, se non c'e', il primo che ha un token
    italiano valente gia' salvato su disco.

    Non e' _resolve_profile_dir(): quello risolve un path di configurazione,
    non un id profilo, e rientrare qui da get_active_chrome_profile
    ricorserebbe all'infinito.
    """
    for cand in (pid, _LAST_PID):
        if cand and cand in PROFILES:
            try:
                d = get_active_chrome_profile(cand)
                if d:
                    return d
            except Exception:
                pass
    return _profilo_con_token()


def _warmup_browser_context(pid=None):
    """Tieni pronto il contesto del browser in background.

    Il server licenze DRM accetta la richiesta solo con i cookie di sessione
    del profilo, quindi serve il contesto del browser. Avviarlo costa ~6s: se
    lo si fa qui, il primo click su "Estrai" non deve aspettarlo (si scende
    da ~9s a ~1s). Non naviga su dazn.com e non blocca l'avvio del server.

    Usa _bg_profile_dir() e non _current_pid(): qui non c'e' una richiesta
    Flask attiva, quindi session['user_profile_id'] non esiste e il profilo va
    risolto dal disco.
    """
    try:
        profile = _bg_profile_dir(pid)
        if not profile:
            return

        async def _warm():
            from dazn_navigator2.services.browser import get_browser
            try:
                await get_browser(user_data_dir=Path(profile))
            except Exception as e:
                print(f"[Warmup] Contesto browser non avviato: {e}")

        run_async(_warm(), timeout=45)
    except Exception as e:
        print(f"[Warmup] Saltato: {e}")

@app.route("/stream/live/<path:event_name>")
def stream_live_event(event_name):
    """Ponte live per Kodi e player: decifra MPD con ClearKey al volo via FFmpeg e lo serve in MPEG-TS continuo."""
    import urllib.parse
    target = urllib.parse.unquote(event_name).strip()
    ev = None
    try:
        p = Path(__file__).resolve().parent / "dazn_event.json"
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                local_data = json.load(f)
            for cat, items in local_data.items():
                for it in items:
                    name_clean = it.get("name", "").replace(" (TEST ACE)", "").replace(" (ACE)", "").strip()
                    target_clean = target.replace(" (TEST ACE)", "").replace(" (ACE)", "").strip()
                    if (it.get("name") == target or name_clean == target_clean) and it.get("key"):
                        ev = it
                        break
                if ev:
                    break
    except Exception:
        pass

    if not ev:
        return "Evento non trovato o senza licenza", 404

    mpd_url = ev.get("mpd")
    clearkey = ev.get("key")
    ua = ev.get("ua", "")
    key_hex = clearkey.split(":")[-1].strip() if ":" in clearkey else clearkey.strip()
    ffmpeg_exe = str(Path(r"C:\Users\alecl\Desktop\PROJECT_HUB\tools\ffmpeg.exe"))

    cmd = [
        ffmpeg_exe,
        "-cenc_decryption_key", key_hex,
        "-headers", f"User-Agent: {ua}\r\nReferer: https://www.dazn.com/\r\nOrigin: https://www.dazn.com\r\n",
        "-probesize", "500000",
        "-analyzeduration", "1000000",
        "-i", mpd_url,
        "-map", "0:v:4",
        "-map", "0:a:0",
        "-c", "copy",
        "-f", "mpegts",
        "pipe:1"
    ]

    def _generate():
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            chunk = proc.stdout.read(65536)
            if chunk:
                yield chunk
            while True:
                chunk = proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            proc.terminate()

    from flask import Response
    resp = Response(_generate(), mimetype="video/mp2t")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
# ─── SISTEMA STATISTICHE E CONTEGGIO SPETTATORI LIVE (IN TEMPO REALE) ───
import time as _t_mod
_LIVE_VIEWERS = {}          # { event_key: { client_id: timestamp } }
_HISTORICAL_VIEWERS = {}    # { event_key: set([client_id, ...]) }
_HISTORICAL_ALL = set()     # set([client_id, ...])
_TOTAL_PLAY_CLICKS = {}     # { event_key: count }
_LAST_CLICK_TIME = {}       # { (event_key, client_id): timestamp }
_VIEWERS_LOCK = threading.Lock()

def _record_viewer(event_key, client_id, is_play_click=False):
    now = _t_mod.time()
    with _VIEWERS_LOCK:
        if event_key not in _LIVE_VIEWERS:
            _LIVE_VIEWERS[event_key] = {}
        _LIVE_VIEWERS[event_key][client_id] = now

        if event_key not in _HISTORICAL_VIEWERS:
            _HISTORICAL_VIEWERS[event_key] = set()
        _HISTORICAL_VIEWERS[event_key].add(client_id)
        _HISTORICAL_ALL.add(client_id)

        if is_play_click:
            # Debounce: lo stesso client sullo stesso evento non può contare un nuovo click
            # se si riconnette entro 12 secondi (evita i re-fetch di chiusura/seek/probe del player)
            key_pair = (event_key, client_id)
            last_click = _LAST_CLICK_TIME.get(key_pair, 0)
            if now - last_click >= 12:
                _LAST_CLICK_TIME[key_pair] = now
                _TOTAL_PLAY_CLICKS[event_key] = _TOTAL_PLAY_CLICKS.get(event_key, 0) + 1

def _clean_and_get_stats():
    now = _t_mod.time()
    ttl = 45  # spettatore attivo negli ultimi 45 secondi
    live_events = {}
    total_live = 0
    with _VIEWERS_LOCK:
        for ev_key, clients in list(_LIVE_VIEWERS.items()):
            active = [cid for cid, last_seen in clients.items() if now - last_seen <= ttl]
            _LIVE_VIEWERS[ev_key] = {cid: clients[cid] for cid in active}
            count = len(active)
            if count > 0:
                live_events[ev_key] = count
                total_live += count

        # Unione di tutti gli eventi conosciuti
        all_event_keys = sorted(list(set(list(_HISTORICAL_VIEWERS.keys()) + list(_TOTAL_PLAY_CLICKS.keys()) + list(live_events.keys()))))
        detailed_events = {}
        total_clicks = sum(_TOTAL_PLAY_CLICKS.values())
        total_unique = len(_HISTORICAL_ALL)

        for ev in all_event_keys:
            detailed_events[ev] = {
                "live_viewers": live_events.get(ev, 0),
                "total_unique_viewers": len(_HISTORICAL_VIEWERS.get(ev, set())),
                "play_clicks": _TOTAL_PLAY_CLICKS.get(ev, 0)
            }

    return {
        "total_viewers_live": total_live,
        "total_unique_viewers": total_unique,
        "total_play_clicks": total_clicks,
        "events": detailed_events,
        "timestamp": int(now)
    }

@app.route("/api/viewers/ping", methods=["GET", "POST"])
def api_viewers_ping():
    """Registra l'heartbeat di uno spettatore attivo."""
    event = (request.values.get("event") or request.values.get("name") or "default").strip()
    client_id = (request.values.get("cid") or request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For") or request.remote_addr or "anon").strip()
    if client_id:
        client_id = client_id.split(",")[0].strip()
    _record_viewer(event, client_id, is_play_click=False)
    return jsonify({"ok": True, "event": event, "status": "active"})

@app.route("/api/viewers/click", methods=["GET", "POST"])
def api_viewers_click():
    """Registra esplicitamente un click sul tasto play."""
    event = (request.values.get("event") or request.values.get("name") or "default").strip()
    client_id = (request.values.get("cid") or request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For") or request.remote_addr or "anon").strip()
    if client_id:
        client_id = client_id.split(",")[0].strip()
    _record_viewer(event, client_id, is_play_click=True)
    return jsonify({"ok": True, "event": event, "status": "clicked"})

@app.route("/api/viewers/stats", methods=["GET"])
def api_viewers_stats():
    """Endpoint pubblico per visualizzare quante persone sono collegate e i totali."""
    data = _clean_and_get_stats()
    return jsonify(data)

@app.route("/live/stream.mpd", methods=["GET"])
def live_stream_redirect():
    """Endpoint trasparente: registra click + spettatore e reindirizza (302) al vero MPD."""
    event = (request.args.get("name") or request.args.get("event") or request.args.get("id") or "Live Event").strip()
    target = (request.args.get("target") or request.args.get("url") or "").strip()
    
    b64_target = request.args.get("b64")
    if b64_target:
        try:
            import base64
            target = base64.b64decode(b64_target).decode("utf-8")
        except Exception:
            pass

    client_ip = (request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For") or request.remote_addr or "anon").strip()
    if client_ip:
        client_ip = client_ip.split(",")[0].strip()

    # Conta sia come click play sia come viewer
    _record_viewer(event, client_ip, is_play_click=True)

    if target:
        from flask import redirect
        return redirect(target, code=302)
    return Response("Target MPD not specified", status=400, mimetype="text/plain")


@app.route("/nba/master.m3u8", methods=["GET"])
def nba_master_m3u8():
    """Restituisce al volo un master playlist HLS con video + audio uniti per Kodi/ISA."""
    v = request.args.get("v", "").strip()
    a = request.args.get("a", "").strip()
    if not v:
        return Response("Missing video url (v)", status=400, mimetype="text/plain")

    content = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n"
    if a:
        content += (
            f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,'
            f'AUTOSELECT=YES,CHANNELS="2",URI="{a}"\n'
            f'#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080,'
            f'CODECS="avc1.4d401f,mp4a.40.2",AUDIO="audio"\n'
            f'{v}\n'
        )
    else:
        content += (
            f'#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080,CODECS="avc1.4d401f"\n'
            f'{v}\n'
        )

    res = Response(content, mimetype="application/vnd.apple.mpegurl")
    res.headers["Access-Control-Allow-Origin"] = "*"
    return res

@app.route("/api/viewers", methods=["GET"])
def api_viewers_ui():
    """Dashboard visuale completa per monitorare spettatori live, totali unici e click play."""
    stats = _clean_and_get_stats()
    ev_html = ""
    for ev, info in stats["events"].items():
        live_badge = f"<span style='color:#00e59b;font-weight:bold;font-size:1.15rem;'>{info['live_viewers']}</span>" if info['live_viewers'] > 0 else "<span style='color:#64748b;'>0</span>"
        ev_html += f"""<tr>
            <td style='padding:12px;border-bottom:1px solid #1e293b;font-weight:600;'>{ev}</td>
            <td style='padding:12px;border-bottom:1px solid #1e293b;text-align:center;'>{live_badge}</td>
            <td style='padding:12px;border-bottom:1px solid #1e293b;text-align:center;color:#38bdf8;font-weight:bold;'>{info['total_unique_viewers']}</td>
            <td style='padding:12px;border-bottom:1px solid #1e293b;text-align:center;color:#f59e0b;font-weight:bold;'>{info['play_clicks']}</td>
        </tr>"""
    if not ev_html:
        ev_html = "<tr><td colspan='4' style='padding:24px;text-align:center;color:#64748b;'>Nessun dato registrato al momento. I dati si aggiornano appena qualcuno clicca o apre uno stream.</td></tr>"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Live Viewers & Play Analytics - Script2</title>
    <meta http-equiv="refresh" content="5">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0b0f14; color: #f8fafc; margin: 0; padding: 24px; display: flex; flex-direction: column; align-items: center; }}
        .card {{ background: #111827; border: 1px solid #1f2937; border-radius: 14px; padding: 24px; width: 100%; max-width: 820px; box-shadow: 0 10px 30px rgba(0,0,0,0.6); }}
        h1 {{ margin: 0 0 18px 0; font-size: 1.45rem; display: flex; align-items: center; gap: 10px; }}
        .badge {{ background: #ef4444; color: #fff; font-size: 0.72rem; padding: 4px 8px; border-radius: 6px; text-transform: uppercase; letter-spacing: 1px; font-weight: 800; animation: pulse 2s infinite; }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} }}
        
        .grid-stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 22px; }}
        .stat-box {{ background: #1e293b; border-radius: 10px; padding: 14px; text-align: center; border: 1px solid #334155; }}
        .stat-box.green {{ border-color: rgba(0, 229, 155, 0.4); background: rgba(0, 229, 155, 0.06); }}
        .stat-box.blue {{ border-color: rgba(56, 189, 248, 0.4); background: rgba(56, 189, 248, 0.06); }}
        .stat-box.orange {{ border-color: rgba(245, 158, 11, 0.4); background: rgba(245, 158, 11, 0.06); }}
        .stat-label {{ color: #94a3b8; font-size: 0.78rem; text-transform: uppercase; font-weight: 700; letter-spacing: 0.5px; }}
        .stat-num {{ font-size: 2rem; font-weight: 800; margin-top: 4px; }}
        .num-green {{ color: #00e59b; }}
        .num-blue {{ color: #38bdf8; }}
        .num-orange {{ color: #f59e0b; }}

        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th {{ padding: 10px; font-size: 0.8rem; text-transform: uppercase; color: #94a3b8; border-bottom: 2px solid #334155; letter-spacing: 0.5px; }}
        .footer {{ margin-top: 18px; font-size: 0.8rem; color: #64748b; text-align: center; }}
    </style>
</head>
<body>
    <div class="card">
        <h1><span class="badge">LIVE</span> Statistiche e Monitor Spettatori</h1>
        
        <div class="grid-stats">
            <div class="stat-box green">
                <div class="stat-label">In Riproduzione ORA</div>
                <div class="stat-num num-green">{stats['total_viewers_live']}</div>
            </div>
            <div class="stat-box blue">
                <div class="stat-label">Spettatori Totali Unici</div>
                <div class="stat-num num-blue">{stats['total_unique_viewers']}</div>
            </div>
            <div class="stat-box orange">
                <div class="stat-label">Click Totali su Play</div>
                <div class="stat-num num-orange">{stats['total_play_clicks']}</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th style="text-align:left;">Evento / Canale</th>
                    <th style="text-align:center;">In Live Ora</th>
                    <th style="text-align:center;">Totali Unici</th>
                    <th style="text-align:center;">Click Play</th>
                </tr>
            </thead>
            <tbody>
                {ev_html}
            </tbody>
        </table>
        
        <div class="footer">Aggiornamento automatico ogni 5 secondi &bull; Rilevamento in tempo reale tramite Script2 API</div>
    </div>
</body>
</html>"""
    return Response(html, mimetype="text/html")


if not os.environ.get("SCRIPT2_NO_WARMUP"):
    threading.Thread(target=_warmup_browser_context, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)



