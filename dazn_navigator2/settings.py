import json
from pathlib import Path

CONFIG_FILE = Path(__file__).resolve().parent.parent / "config.json"

DEFAULT_CONFIG = {
    "print_extension_link": True,
    "browser_timeout": 0,
    "debug_mode": False,
    "extraction_engine": "curl_cffi",
    "include_ua_curl": True,
    "include_ua_headless": True,
    "extraction_method": "web",
}

EXTRACTION_METHODS = ("web", "mobile")

def load_config():
    if CONFIG_FILE.exists():
        try:
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            changed = False
            for k, v in DEFAULT_CONFIG.items():
                if k not in data:
                    data[k] = v
                    changed = True
            # rimuovi chiavi legacy github
            for legacy in ["github_deploy", "include_ua_in_json"]:
                if legacy in data:
                    del data[legacy]
                    changed = True
            if changed:
                save_config(data)
            return data
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()

def save_config(data):
    CONFIG_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def get_setting(key, default=None):
    return load_config().get(key, default if default is not None else DEFAULT_CONFIG.get(key))

def toggle_setting(key):
    cfg = load_config()
    cfg[key] = not cfg.get(key, DEFAULT_CONFIG.get(key, False))
    save_config(cfg)
    return cfg[key]


def get_extraction_method():
    """Metodo di estrazione attivo: "web" (default) oppure "mobile"."""
    method = str(get_setting("extraction_method") or "web").strip().lower()
    return method if method in EXTRACTION_METHODS else "web"


def set_extraction_method(method):
    """Salva il metodo di estrazione. Valori ammessi: web, mobile."""
    method = str(method or "").strip().lower()
    if method not in EXTRACTION_METHODS:
        raise ValueError("Metodo di estrazione non valido")
    cfg = load_config()
    cfg["extraction_method"] = method
    save_config(cfg)
    return method
