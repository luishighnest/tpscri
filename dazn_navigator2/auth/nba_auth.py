import json, time, os, re, subprocess, sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SESSION_FILE = BASE_DIR / "nba_session.json"
WORKER_SCRIPT = BASE_DIR / "nba_login_worker.py"

WORKER_CODE = r'''
import asyncio, json, time, re, sys
from pathlib import Path
from playwright.async_api import async_playwright

async def main():
    timeout = int(sys.argv[1])
    session_file = sys.argv[2]
    debug_dir = str(Path(session_file).parent / "nba_debug")
    Path(debug_dir).mkdir(exist_ok=True)

    captured = {"auth_token": None, "session_id": None, "manifest_url": None, "all_auth": [], "all_m3u8": [], "all_headers": []}

    def on_request(request):
        try:
            headers = request.headers
            url = request.url
            # Check ALL headers for any auth-like value
            for hname, hval in headers.items():
                if "auth" in hname.lower() or "token" in hname.lower():
                    if len(hval) > 50:
                        captured["all_auth"].append({"url": url[:90], "header": hname, "len": len(hval)})
                        if not captured["auth_token"]:
                            captured["auth_token"] = hval
                            print("CAPTURED AUTH (header=" + hname + ") from " + url[:60], flush=True)
            if "sessionId=" in url:
                m = re.search(r"sessionId=([a-f0-9-]+)", url)
                if m:
                    captured["session_id"] = m.group(1)
            if ".m3u8" in url:
                captured["all_m3u8"].append(url[:90])
                if "lpnba" in url and not captured["manifest_url"]:
                    captured["manifest_url"] = url
                    print("CAPTURED MANIFEST (lpnba)", flush=True)
                elif not captured["manifest_url"]:
                    captured["manifest_url"] = url
                    print("CAPTURED MANIFEST (other): " + url[:80], flush=True)
        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path="C:/Program Files/Google/Chrome/Application/chrome.exe",
            headless=False,
            args=["--no-sandbox", "--disable-gpu", "--window-size=1280,720"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
        )
        # Use context-level request capture (captures service workers too)
        context.on("request", on_request)
        page = await context.new_page()
        page.on("request", on_request)

        print("OPENING NBA.COM - Please log in if needed, then go to nba.com/watch/nba-tv", flush=True)
        await page.goto("https://www.nba.com", timeout=30000)
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)
        await page.screenshot(path=debug_dir + "/01_home.png")
        print("PAGE: " + page.url, flush=True)

        # Wait for user to log in
        print("WAITING FOR LOGIN (up to 120s)...", flush=True)
        start = time.time()
        while time.time() - start < 120:
            if "sign-in" not in page.url and "login" not in page.url.lower():
                print("LOGGED IN! URL: " + page.url, flush=True)
                break
            await page.wait_for_timeout(2000)

        # Navigate to NBA TV
        print("NAVIGATING TO NBA TV...", flush=True)
        await page.goto("https://www.nba.com/watch/nba-tv", timeout=60000)
        await page.wait_for_load_state("domcontentloaded", timeout=30000)
        await page.wait_for_timeout(8000)
        await page.screenshot(path=debug_dir + "/02_nba_tv.png")
        print("PAGE: " + page.url, flush=True)

        # Try to play video
        for sel in ['button[aria-label*="play" i]', 'button:has-text("Play")', '.play-button', '[data-testid*="play"]', '.vjs-big-play-button', 'button.play']:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=2000):
                    await btn.click(timeout=5000)
                    print("CLICKED PLAY: " + sel, flush=True)
                    await page.wait_for_timeout(5000)
                    break
            except Exception:
                continue

        try:
            video = page.locator('video').first
            if await video.is_visible(timeout=2000):
                await video.click()
                print("CLICKED VIDEO", flush=True)
                await page.wait_for_timeout(5000)
        except Exception:
            pass

        try:
            await page.evaluate("() => { const v = document.querySelector('video'); if (v) v.play(); }")
            print("JS PLAY ATTEMPTED", flush=True)
            await page.wait_for_timeout(5000)
        except Exception:
            pass

        await page.screenshot(path=debug_dir + "/03_after_play.png")

        # Wait for token capture
        print("WAITING FOR TOKEN (up to 120s)...", flush=True)
        start = time.time()
        while time.time() - start < 120:
            if captured["auth_token"] and captured["manifest_url"]:
                print("TOKEN + MANIFEST CAPTURED!", flush=True)
                break
            if captured["auth_token"] and not captured["manifest_url"]:
                try:
                    manifest = await page.evaluate("""() => {
                        const entries = performance.getEntriesByType('resource');
                        for (const e of entries) {
                            if (e.name.includes('.m3u8')) return e.name;
                        }
                        return null;
                    }""")
                    if manifest:
                        captured["manifest_url"] = manifest
                        print("MANIFEST FROM PERFORMANCE API", flush=True)
                        break
                except Exception:
                    pass
            await page.wait_for_timeout(2000)

        await page.screenshot(path=debug_dir + "/04_final.png")
        await browser.close()

    if captured["auth_token"]:
        dati = {
            "authorizationToken": captured["auth_token"],
            "sessionId": captured.get("session_id", ""),
            "saved_at": int(time.time()),
            "extra": {"manifest_url": captured.get("manifest_url", "")},
        }
        Path(session_file).write_text(json.dumps(dati, indent=2), encoding="utf-8")
        print("OK")
        print(captured.get("session_id", ""))
        print(captured.get("manifest_url", "")[:300])
    else:
        print("NO_TOKEN")
        print("Auth requests with auth header: " + str(len(captured["all_auth"])))
        print("m3u8 requests: " + str(len(captured["all_m3u8"])))
        for r in captured["all_auth"][-10:]:
            print("  AUTH: " + r["url"] + " (header=" + r["header"] + ", len=" + str(r["len"]) + ")")
        for r in captured["all_m3u8"][-5:]:
            print("  M3U8: " + r)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
'''

WORKER_SCRIPT.write_text(WORKER_CODE, encoding="utf-8")


def carica_sessione() -> dict:
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def salva_sessione(auth_token: str, session_id: str = "", extra: dict = None) -> bool:
    dati = {
        "authorizationToken": auth_token,
        "sessionId": session_id,
        "saved_at": int(time.time()),
        "extra": extra or {},
    }
    try:
        SESSION_FILE.write_text(json.dumps(dati, indent=2), encoding="utf-8")
        return True
    except Exception as e:
        print(f"[NBA Auth] Errore salvataggio: {e}")
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
    if not sess or not sess.get("authorizationToken"):
        return {"presente": False, "valido": False, "resto": 0}
    saved = sess.get("saved_at", 0)
    resto = (saved + 7200) - int(time.time())
    return {
        "presente": True,
        "valido": resto > 300,
        "resto": max(0, resto),
    }


def accedi(email: str = "", password: str = "", timeout: int = 180) -> tuple[bool, str]:
    sess = carica_sessione()
    if sess.get("authorizationToken"):
        return True, "Sessione NBA già presente."

    WORKER_SCRIPT.write_text(WORKER_CODE, encoding="utf-8")

    try:
        result = subprocess.run(
            [sys.executable, str(WORKER_SCRIPT), str(timeout), str(SESSION_FILE)],
            capture_output=True,
            text=True,
            timeout=timeout + 30,
            env={**os.environ, "PYTHONPATH": str(BASE_DIR), "PYTHONIOENCODING": "utf-8"},
        )
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if result.returncode != 0:
            return False, f"Errore: {stderr[-500:] if stderr else 'processo fallito'}"

        lines = stdout.splitlines()
        if lines and lines[0] == "OK":
            sess = carica_sessione()
            if sess.get("authorizationToken"):
                return True, "Token NBA catturato con successo!"
            return False, "Token non salvato correttamente."
        elif "NO_TOKEN" in stdout:
            debug_info = "\n".join(lines[-20:]) if len(lines) > 1 else ""
            return False, f"Nessun token catturato.\n{debug_info}"
        else:
            return False, f"Output inatteso: {stdout[:300]}"

    except subprocess.TimeoutExpired:
        return False, "Timeout: il login non è stato completato in tempo."
    except Exception as e:
        return False, f"Errore: {e}"
