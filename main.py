"""
Seedance Pro — FastAPI Backend Proxy (v4 — Parallel IP Rotation)
True parallel video generation via IP rotation:
- Tor SOCKS5 proxy pool (4 circuits = 4 different IPs)
- Direct connection (1 more IP)
- Auto circuit rotation for fresh IPs
- Smart queue fallback when all slots busy
- Telegram bot with batch mode
"""

import os
import re
import time
import random
import asyncio
import logging
from typing import Optional, List

import httpx
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("seedance")

app = FastAPI(title="Seedance Pro API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

AJAX_URL = "https://veoaifree.com/wp-admin/admin-ajax.php"

NONCE_SOURCES = [
    "https://veoaifree.com/seedance-2-0-video-generator-free/",
    "https://veoaifree.com/veo-video-generator/",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
]

TG_API = "https://api.telegram.org/bot{token}/{method}"

# ====== IP Pool (Tor + Direct) ======

TOR_SOCKS_PORTS = [9050, 9052, 9053, 9054]
TOR_CONTROL_PORT = 9051
TOR_CONTROL_PASSWORD = os.environ.get("TOR_PASSWORD", "devinproxy")


class IPSlot:
    """One IP slot = one proxy endpoint (or direct). Each has its own lock."""
    def __init__(self, name: str, proxy_url: Optional[str] = None):
        self.name = name
        self.proxy_url = proxy_url
        self.lock = asyncio.Lock()
        self.busy = False
        self.last_used = 0.0

    def __repr__(self):
        return f"<IPSlot {self.name} busy={self.busy}>"


_ip_pool: List[IPSlot] = []
_tor_available = False
_nonce_cache: dict = {"nonce": None, "time": 0}
_telegram_config: dict = {"bot_token": None, "chat_id": None}
_bot_task: Optional[asyncio.Task] = None
_bot_running = False
_batch_sessions: dict = {}


def _random_ua() -> str:
    return random.choice(USER_AGENTS)


def _make_page_headers() -> dict:
    return {
        "User-Agent": _random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }


def _make_ajax_headers() -> dict:
    return {
        "User-Agent": _random_ua(),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": random.choice(NONCE_SOURCES),
        "Origin": "https://veoaifree.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }


def _make_client(proxy_url: Optional[str] = None, timeout: int = 30) -> httpx.AsyncClient:
    """Create httpx client with optional proxy."""
    kwargs = {"follow_redirects": True, "timeout": timeout}
    if proxy_url:
        kwargs["proxy"] = proxy_url
    return httpx.AsyncClient(**kwargs)


async def _check_tor() -> bool:
    """Check if Tor SOCKS5 is available."""
    for port in TOR_SOCKS_PORTS:
        try:
            async with _make_client(f"socks5://127.0.0.1:{port}", timeout=10) as client:
                resp = await client.get("https://check.torproject.org/api/ip")
                data = resp.json()
                if data.get("IsTor"):
                    return True
        except Exception:
            continue
    return False


async def _rotate_tor_circuit():
    """Request new Tor circuit for fresh IPs."""
    try:
        from stem import Signal
        from stem.control import Controller
        with Controller.from_port(port=TOR_CONTROL_PORT) as ctrl:
            ctrl.authenticate(password=TOR_CONTROL_PASSWORD)
            ctrl.signal(Signal.NEWNYM)
            log.info("Tor circuit rotated")
    except Exception as e:
        log.warning(f"Tor circuit rotation failed: {e}")


async def _init_ip_pool():
    """Initialize the IP pool with Tor circuits + direct connection."""
    global _ip_pool, _tor_available

    _ip_pool = []

    # Always add direct connection
    _ip_pool.append(IPSlot("direct", proxy_url=None))

    # Check Tor availability
    _tor_available = await _check_tor()
    if _tor_available:
        for port in TOR_SOCKS_PORTS:
            try:
                async with _make_client(f"socks5://127.0.0.1:{port}", timeout=10) as client:
                    resp = await client.get("https://check.torproject.org/api/ip")
                    data = resp.json()
                    ip = data.get("IP", "?")
                    _ip_pool.append(IPSlot(f"tor:{port}({ip})", proxy_url=f"socks5://127.0.0.1:{port}"))
                    log.info(f"Tor slot added: port {port}, IP {ip}")
            except Exception as e:
                log.warning(f"Tor port {port} failed: {e}")
    else:
        log.warning("Tor not available — running in sequential mode (direct IP only)")

    # Also check for user-provided proxies via env
    custom_proxies = os.environ.get("PROXY_LIST", "").strip()
    if custom_proxies:
        for p in custom_proxies.split(","):
            p = p.strip()
            if p:
                _ip_pool.append(IPSlot(f"custom:{p}", proxy_url=p))
                log.info(f"Custom proxy added: {p}")

    log.info(f"IP Pool initialized: {len(_ip_pool)} slots ({len(_ip_pool)-1} proxies + direct)")


async def acquire_slot() -> IPSlot:
    """Get a free IP slot (non-blocking). Returns the least-recently-used free slot."""
    # Try to find a free slot
    free_slots = [s for s in _ip_pool if not s.lock.locked()]
    if free_slots:
        slot = min(free_slots, key=lambda s: s.last_used)
        return slot

    # All slots busy — wait for any to free up
    while True:
        for slot in _ip_pool:
            if not slot.lock.locked():
                return slot
        await asyncio.sleep(1)


# ====== Core API helpers ======

async def fetch_nonce(force: bool = False, proxy_url: Optional[str] = None) -> str:
    now = time.time()
    if not force and _nonce_cache["nonce"] and (now - _nonce_cache["time"]) < 120:
        return _nonce_cache["nonce"]

    last_error = ""
    for attempt in range(1, 5):
        source = NONCE_SOURCES[attempt % len(NONCE_SOURCES)]
        try:
            async with _make_client(proxy_url, timeout=20) as client:
                resp = await client.get(source, headers=_make_page_headers())
                if resp.status_code == 200:
                    m = re.search(r'ajax_object\s*=\s*\{[^}]*"nonce"\s*:\s*"([a-f0-9]+)"', resp.text)
                    if m:
                        nonce = m.group(1)
                        _nonce_cache["nonce"] = nonce
                        _nonce_cache["time"] = time.time()
                        return nonce
                    last_error = "Nonce pattern not found"
                else:
                    last_error = f"HTTP {resp.status_code}"
        except Exception as e:
            last_error = str(e)
        if attempt < 4:
            await asyncio.sleep(0.5 * attempt)

    if _nonce_cache["nonce"]:
        return _nonce_cache["nonce"]
    raise Exception(f"Failed to fetch nonce: {last_error}")


async def api_post(fields: dict, timeout: int = 120, proxy_url: Optional[str] = None) -> str:
    """POST to API with auto-retry and nonce refresh."""
    last_error = ""
    for attempt in range(1, 5):
        try:
            async with _make_client(proxy_url, timeout=timeout) as client:
                resp = await client.post(AJAX_URL, data=fields, headers=_make_ajax_headers())
                if resp.status_code == 200:
                    text = resp.text.strip()

                    if text in ("-1", "0"):
                        log.warning(f"Nonce expired (got {text}), refreshing...")
                        _nonce_cache["nonce"] = None
                        _nonce_cache["time"] = 0
                        new_nonce = await fetch_nonce(force=True, proxy_url=proxy_url)
                        fields["nonce"] = new_nonce
                        last_error = "Nonce expired"
                        await asyncio.sleep(0.5)
                        continue

                    if "Error establishing a database connection" in text:
                        last_error = "Upstream database error"
                        if attempt < 4:
                            await asyncio.sleep(attempt * 2)
                            continue

                    if "rate-limit-exceed" in text.lower() or "In Progress" in text:
                        log.warning(f"Rate limited via {proxy_url or 'direct'} (attempt {attempt})")
                        last_error = "Rate limited"
                        if attempt < 4:
                            await asyncio.sleep(attempt * 10)
                            new_nonce = await fetch_nonce(force=True, proxy_url=proxy_url)
                            fields["nonce"] = new_nonce
                            continue

                    return text
                last_error = f"HTTP {resp.status_code}"
        except Exception as e:
            last_error = str(e)
        if attempt < 4:
            await asyncio.sleep(attempt)

    raise Exception(f"API failed after 4 attempts: {last_error}")


async def _single_enhance(prompt: str) -> str:
    nonce = await fetch_nonce()
    response = await api_post({
        "action": "veo_video_generator",
        "nonce": nonce,
        "prompt": prompt,
        "actionType": "main-prompt-generation",
    }, timeout=30)
    result = response.strip()
    result = re.sub(r'\s*-\d+\s*$', '', result)
    if not result or result == "empty":
        raise Exception("Enhancement returned empty")
    return result


async def _do_video_generate(prompt: str, aspect: str, proxy_url: Optional[str] = None) -> dict:
    """Initiate video generation via specific IP slot."""
    last_error = ""
    for attempt in range(1, 4):
        try:
            nonce = await fetch_nonce(proxy_url=proxy_url)
            response = await api_post({
                "action": "veo_video_generator",
                "nonce": nonce,
                "prompt": prompt,
                "totalVariations": "1",
                "aspectRatio": aspect,
                "actionType": "full-video-generate",
            }, proxy_url=proxy_url)
            response = response.strip()

            if "rate-limit-exceed" in response.lower() or "In Progress" in response:
                log.warning(f"Rate limited on attempt {attempt} via {proxy_url or 'direct'}")
                last_error = "Rate limited — slot busy"
                if attempt < 3:
                    await asyncio.sleep(attempt * 12)
                    continue
                return {"success": False, "error": last_error, "retryable": True}

            if "Error establishing a database connection" in response:
                last_error = "Upstream database error"
                if attempt < 3:
                    await asyncio.sleep(attempt * 5)
                    continue
                return {"success": False, "error": last_error, "retryable": True}

            if "<h1>" in response or (len(response) < 20 and ("Error" in response or "error" in response.lower())):
                last_error = f"API error: {response[:200]}"
                if attempt < 3:
                    await asyncio.sleep(attempt * 3)
                    continue
                return {"success": False, "error": last_error, "retryable": False}

            if not response:
                last_error = "Empty response"
                if attempt < 3:
                    await asyncio.sleep(attempt * 3)
                    continue
                return {"success": False, "error": last_error, "retryable": True}

            return {"success": True, "sceneData": response}

        except Exception as e:
            last_error = str(e)
            log.warning(f"Video generate attempt {attempt} failed: {e}")
            if attempt < 3:
                await asyncio.sleep(attempt * 3)

    return {"success": False, "error": f"Generation failed after retries: {last_error}", "retryable": True}


async def _do_video_poll(scene_data: str, proxy_url: Optional[str] = None) -> dict:
    nonce = await fetch_nonce(proxy_url=proxy_url)
    response = await api_post({
        "action": "veo_video_generator",
        "nonce": nonce,
        "sceneData": scene_data,
        "actionType": "final-video-results",
    }, timeout=60, proxy_url=proxy_url)
    response = response.strip()

    if "<h1>" in response:
        return {"success": False, "error": "Upstream server error"}
    if "Rate Limit" in response or "Error" in response:
        clean = re.sub(r"<[^>]+>", "", response)
        return {"success": False, "error": f"API error: {clean[:200]}"}
    if not response:
        return {"success": True, "videoUrl": None, "status": "pending"}

    if len(response) > 15:
        url = response.replace("videos/", "video/")
        return {"success": True, "videoUrl": url}

    return {"success": True, "videoUrl": None, "status": "pending"}


async def _full_video_pipeline(prompt: str, aspect: str = "VIDEO_ASPECT_RATIO_PORTRAIT") -> str:
    """Full pipeline used by Telegram bot. Acquires slot, generates, polls."""
    slot = await acquire_slot()
    async with slot.lock:
        slot.last_used = time.time()
        log.info(f"Pipeline using slot: {slot.name}")

        result = await _do_video_generate(prompt, aspect, proxy_url=slot.proxy_url)
        if not result["success"]:
            raise Exception(result["error"])

        scene_data = result["sceneData"]
        await asyncio.sleep(50)

        for _ in range(20):
            poll = await _do_video_poll(scene_data, proxy_url=slot.proxy_url)
            if poll.get("videoUrl"):
                return poll["videoUrl"]
            if not poll["success"] and "pending" not in poll.get("error", "pending"):
                raise Exception(poll.get("error", "Poll failed"))
            await asyncio.sleep(10)

        raise Exception("Timed out (250s)")


async def _generate_image(prompt: str, aspect: str = "IMAGE_ASPECT_RATIO_PORTRAIT", variations: int = 1) -> list:
    v = max(1, min(variations, 4))
    nonce = await fetch_nonce()
    response = await api_post({
        "action": "veo_video_generator",
        "nonce": nonce,
        "promptIMG": prompt,
        "totalVariationsIMG": str(v),
        "aspectRatioIMG": aspect,
        "actionType": "banan-image-generator",
    }, timeout=60)
    response = response.strip()
    if not response:
        raise Exception("Empty response from image API")
    if "<h1>" in response.lower() or "error" in response.lower():
        raise Exception("Image API error")
    parts = response.split(",")
    images = [p.strip() for p in parts if len(p.strip()) > 200 and not re.search(r"<[a-z]|[{}\[\]]", p)]
    if not images:
        raise Exception("No valid images in response")
    return images


# ====== Telegram Bot ======

async def tg_call(token: str, method: str, data: dict = None, files: dict = None, timeout: int = 60):
    url = TG_API.format(token=token, method=method)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if files:
            resp = await client.post(url, data=data or {}, files=files)
        else:
            resp = await client.post(url, json=data or {})
        return resp.json()


async def tg_send(token: str, chat_id, text: str, parse_mode: str = "HTML"):
    if len(text) > 4000:
        text = text[:4000] + "..."
    return await tg_call(token, "sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": parse_mode})


async def tg_send_video(token: str, chat_id, video_url: str, caption: str = ""):
    max_attempts = 3
    last_error = ""
    for attempt in range(1, max_attempts + 1):
        try:
            result = await tg_call(token, "sendVideo", {
                "chat_id": chat_id, "video": video_url,
                "caption": (caption or "Generated by Seedance Pro")[:1024], "parse_mode": "HTML",
            }, timeout=120)
            if result.get("ok"):
                return result
            last_error = result.get("description", "Unknown Telegram error")
            log.warning(f"Telegram sendVideo attempt {attempt} failed: {last_error}")
            if any(x in last_error.lower() for x in ["bot was blocked", "chat not found", "unauthorized"]):
                return result
        except Exception as e:
            last_error = str(e)
            log.warning(f"Telegram sendVideo attempt {attempt} exception: {e}")
        if attempt < max_attempts:
            await asyncio.sleep(attempt * 3)
    return {"ok": False, "description": f"Failed after {max_attempts} attempts: {last_error}"}


async def tg_send_photo_base64(token: str, chat_id, base64_data: str, caption: str = ""):
    import base64
    img_bytes = base64.b64decode(base64_data)
    return await tg_call(token, "sendPhoto",
        data={"chat_id": str(chat_id), "caption": (caption or "Generated")[:1024]},
        files={"photo": ("image.png", img_bytes, "image/png")}, timeout=60)


async def handle_tg_message(token: str, message: dict):
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id or not text:
        return

    log.info(f"TG: [{message.get('from', {}).get('first_name', '?')}] {text[:80]}")

    if text == "/start":
        pool_info = f"{len(_ip_pool)} IP slots ({len([s for s in _ip_pool if s.proxy_url])} proxies + 1 direct)"
        await tg_send(token, chat_id,
            f"⚡ <b>Seedance Pro Bot v4</b>\n"
            f"IP Pool: {pool_info}\n\n"
            "<b>Commands:</b>\n"
            "/video &lt;prompt&gt; — Generate video\n"
            "/batch — Start batch mode (parallel!)\n"
            "/image &lt;prompt&gt; — Generate image\n"
            "/enhance &lt;prompt&gt; — Enhance prompt\n"
            "/random — Random prompt\n"
            "/pool — Show IP pool status\n"
            "/rotate — Rotate Tor circuits\n"
            "/help — Help\n\nOr just send text = video prompt!")
        return

    if text in ("/help",):
        return

    if text == "/pool":
        lines = []
        for s in _ip_pool:
            status = "BUSY" if s.lock.locked() else "FREE"
            lines.append(f"• {s.name}: {status}")
        await tg_send(token, chat_id, f"<b>IP Pool ({len(_ip_pool)} slots):</b>\n" + "\n".join(lines))
        return

    if text == "/rotate":
        if _tor_available:
            await _rotate_tor_circuit()
            await asyncio.sleep(3)
            await tg_send(token, chat_id, "Tor circuits rotated! New IPs assigned.")
        else:
            await tg_send(token, chat_id, "Tor not available.")
        return

    if text == "/status":
        try:
            await fetch_nonce(force=True)
            await tg_send(token, chat_id, f"API online! Pool: {len(_ip_pool)} slots, Tor: {'Yes' if _tor_available else 'No'}")
        except Exception:
            await tg_send(token, chat_id, "API down.")
        return

    if text == "/random":
        await tg_send(token, chat_id, "Generating random prompt...")
        try:
            seeds = ["cinematic nature scene", "futuristic city at night", "underwater ocean", "fantasy dragon", "space astronaut nebula", "cyberpunk neon rain", "magical forest fireflies"]
            result = await _single_enhance(random.choice(seeds))
            await tg_send(token, chat_id, f"<b>Random Prompt:</b>\n\n{result}")
        except Exception as e:
            await tg_send(token, chat_id, f"Failed: {e}")
        return

    if text.startswith("/enhance"):
        prompt = text[len("/enhance"):].strip()
        if not prompt:
            await tg_send(token, chat_id, "Usage: /enhance &lt;idea&gt;")
            return
        await tg_send(token, chat_id, "Enhancing (2x)...")
        try:
            s1 = await _single_enhance(prompt)
            s2 = await _single_enhance(s1)
            await tg_send(token, chat_id, f"<b>Enhanced (2x):</b>\n\n{s2}")
        except Exception as e:
            await tg_send(token, chat_id, f"Failed: {e}")
        return

    if text.startswith("/image"):
        prompt = text[len("/image"):].strip()
        if not prompt:
            await tg_send(token, chat_id, "Usage: /image &lt;prompt&gt;")
            return
        await tg_send(token, chat_id, "Generating image...")
        try:
            images = await _generate_image(prompt)
            for img in images:
                await tg_send_photo_base64(token, chat_id, img, f"{prompt[:200]}")
        except Exception as e:
            await tg_send(token, chat_id, f"Image failed: {e}")
        return

    if text == "/batch":
        _batch_sessions[chat_id] = {"prompts": [], "aspect": "VIDEO_ASPECT_RATIO_PORTRAIT"}
        await tg_send(token, chat_id,
            f"<b>Batch Mode! ({len(_ip_pool)} parallel slots available)</b>\n"
            "Send prompts one by one.\n"
            "/landscape or /portrait to set ratio\n"
            "/go — Generate ALL in parallel!\n/cancel — Cancel")
        return

    if text == "/cancel" and chat_id in _batch_sessions:
        del _batch_sessions[chat_id]
        await tg_send(token, chat_id, "Batch cancelled.")
        return

    if text == "/landscape" and chat_id in _batch_sessions:
        _batch_sessions[chat_id]["aspect"] = "VIDEO_ASPECT_RATIO_LANDSCAPE"
        await tg_send(token, chat_id, "Set to Landscape (16:9)")
        return
    if text == "/portrait" and chat_id in _batch_sessions:
        _batch_sessions[chat_id]["aspect"] = "VIDEO_ASPECT_RATIO_PORTRAIT"
        await tg_send(token, chat_id, "Set to Portrait (9:16)")
        return

    if text == "/go" and chat_id in _batch_sessions:
        session = _batch_sessions.pop(chat_id)
        prompts = session["prompts"]
        aspect = session["aspect"]
        if not prompts:
            await tg_send(token, chat_id, "No prompts! Start with /batch")
            return
        await tg_send(token, chat_id, f"Launching {len(prompts)} videos in PARALLEL ({len(_ip_pool)} IP slots)...")

        async def gen_one(i, p):
            try:
                await tg_send(token, chat_id, f"[{i+1}/{len(prompts)}] Starting: {p[:60]}...")
                url = await _full_video_pipeline(p, aspect)
                await tg_send_video(token, chat_id, url, f"[{i+1}] {p[:200]}")
                return True
            except Exception as e:
                await tg_send(token, chat_id, f"[{i+1}] Failed: {e}")
                return False

        results = await asyncio.gather(*[gen_one(i, p) for i, p in enumerate(prompts)])
        success = sum(1 for r in results if r)
        await tg_send(token, chat_id, f"Batch done! {success}/{len(prompts)} succeeded")
        return

    if chat_id in _batch_sessions and not text.startswith("/"):
        if len(text) < 15:
            await tg_send(token, chat_id, f"Too short (min 15 chars). Got: {len(text)}")
            return
        _batch_sessions[chat_id]["prompts"].append(text)
        c = len(_batch_sessions[chat_id]["prompts"])
        await tg_send(token, chat_id, f"Prompt #{c} added! Send more or /go")
        return

    prompt = text[len("/video"):].strip() if text.startswith("/video") else text
    if not prompt or len(prompt) < 15:
        await tg_send(token, chat_id, "Prompt must be at least 15 characters.")
        return
    await tg_send(token, chat_id, f"Generating video...\n\n<i>{prompt[:200]}</i>\n\n~60-90s wait.")
    try:
        url = await _full_video_pipeline(prompt)
        await tg_send_video(token, chat_id, url, f"{prompt[:200]}")
    except Exception as e:
        await tg_send(token, chat_id, f"Failed: {e}")


async def telegram_bot_loop(token: str):
    global _bot_running
    _bot_running = True
    offset = 0
    log.info("Telegram bot started")
    while _bot_running:
        try:
            result = await tg_call(token, "getUpdates", {"offset": offset, "timeout": 30, "allowed_updates": ["message"]}, timeout=60)
            if result.get("ok") and result.get("result"):
                for update in result["result"]:
                    offset = update["update_id"] + 1
                    msg = update.get("message")
                    if msg:
                        asyncio.create_task(handle_tg_message(token, msg))
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"Bot error: {e}")
            await asyncio.sleep(5)
    log.info("Telegram bot stopped")


def start_bot(token: str):
    global _bot_task
    if _bot_task and not _bot_task.done():
        _bot_task.cancel()
    _bot_task = asyncio.create_task(telegram_bot_loop(token))


def stop_bot():
    global _bot_running, _bot_task
    _bot_running = False
    if _bot_task and not _bot_task.done():
        _bot_task.cancel()


@app.on_event("startup")
async def startup():
    await _init_ip_pool()
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if token:
        _telegram_config["bot_token"] = token
        if chat_id:
            _telegram_config["chat_id"] = chat_id
        start_bot(token)
        log.info("Telegram bot auto-started from env")


@app.on_event("shutdown")
async def shutdown():
    stop_bot()


# ====== Request Model ======

class GenericReq(BaseModel):
    type: str
    prompt: Optional[str] = None
    prompts: Optional[List[str]] = None
    aspectRatio: Optional[str] = None
    variations: Optional[str] = None
    sceneData: Optional[str] = None
    enhanceLevel: Optional[int] = None
    botToken: Optional[str] = None
    chatId: Optional[str] = None
    videoUrl: Optional[str] = None
    caption: Optional[str] = None
    proxy: Optional[str] = None


# ====== HTTP Routes ======

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "Seedance Pro API v4",
        "tor_available": _tor_available,
        "ip_pool_size": len(_ip_pool),
        "ip_slots": [{"name": s.name, "busy": s.lock.locked()} for s in _ip_pool],
        "telegram_bot": _bot_running,
    }


@app.post("/api")
async def handle_request(req: GenericReq):
    t = req.type

    if t == "video_generate":
        return await handle_video_generate(req.prompt or "", req.aspectRatio or "VIDEO_ASPECT_RATIO_PORTRAIT")
    elif t == "video_poll":
        return await handle_video_poll(req.sceneData or "", req.proxy)
    elif t == "image_generate":
        return await handle_image_generate(req.prompt or "", req.aspectRatio or "IMAGE_ASPECT_RATIO_PORTRAIT", req.variations or "1")
    elif t == "enhance_prompt":
        return await handle_enhance_prompt(req.prompt or "", req.enhanceLevel or 1)
    elif t == "generate_prompt":
        return await handle_generate_prompt()
    elif t == "batch_validate":
        return await handle_batch_validate(req.prompts or [])
    elif t == "telegram_setup":
        return await handle_telegram_setup(req.botToken or "", req.chatId or "")
    elif t == "telegram_send":
        return await handle_telegram_send(req.videoUrl or "", req.caption or "")
    elif t == "telegram_status":
        return {"success": True, "configured": bool(_telegram_config["bot_token"] and _telegram_config["chat_id"]), "bot_running": _bot_running}
    elif t == "pool_status":
        return {
            "success": True,
            "total": len(_ip_pool),
            "tor": _tor_available,
            "slots": [{"name": s.name, "busy": s.lock.locked()} for s in _ip_pool],
        }
    elif t == "rotate_circuits":
        if _tor_available:
            await _rotate_tor_circuit()
            await asyncio.sleep(3)
            return {"success": True, "message": "Circuits rotated"}
        return {"success": False, "error": "Tor not available"}
    elif t == "fetch_nonce":
        nonce = await fetch_nonce(force=True)
        return {"success": True, "nonce": nonce}
    else:
        raise HTTPException(status_code=400, detail=f"Unknown type: {t}")


async def handle_video_generate(prompt: str, aspect: str):
    """Acquire an IP slot and initiate video generation. Retries across slots on retryable errors."""
    prompt = prompt.strip()
    if len(prompt) < 15:
        return {"success": False, "error": "Prompt must be at least 15 characters"}

    tried_slots = set()
    max_slot_attempts = min(len(_ip_pool), 3)

    for slot_attempt in range(max_slot_attempts):
        slot = await acquire_slot()
        # Try a different slot if we already failed on this one
        if slot.name in tried_slots:
            # Wait for any other slot
            for s in _ip_pool:
                if s.name not in tried_slots and not s.lock.locked():
                    slot = s
                    break
            else:
                # All tried or busy — just use the acquired slot
                pass

        tried_slots.add(slot.name)
        async with slot.lock:
            slot.last_used = time.time()
            log.info(f"Video generate using slot: {slot.name} (attempt {slot_attempt + 1})")
            result = await _do_video_generate(prompt, aspect, proxy_url=slot.proxy_url)
            if result.get("success"):
                result["slot"] = slot.name
                result["proxy"] = slot.proxy_url
                return result
            if not result.get("retryable", False):
                return result
            log.warning(f"Slot {slot.name} retryable error: {result.get('error')} — trying next slot")

    return {"success": False, "error": "All available slots failed", "retryable": True}


async def handle_video_poll(scene_data: str, proxy_hint: Optional[str] = None):
    scene_data = (scene_data or "").strip()
    if not scene_data:
        return {"success": False, "error": "Missing sceneData"}
    return await _do_video_poll(scene_data, proxy_url=proxy_hint)


async def handle_image_generate(prompt: str, aspect: str, variations: str):
    prompt = prompt.strip()
    if not prompt:
        return {"success": False, "error": "Prompt is required"}
    try:
        images = await _generate_image(prompt, aspect, int(variations))
        return {"success": True, "images": images}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def handle_enhance_prompt(prompt: str, level: int = 1):
    prompt = prompt.strip()
    if not prompt:
        return {"success": False, "error": "Prompt is required"}
    level = max(1, min(level, 5))
    current = prompt
    steps = []
    for i in range(level):
        try:
            enhanced = await _single_enhance(current)
            steps.append({"level": i + 1, "result": enhanced})
            current = enhanced
        except Exception as e:
            if not steps:
                return {"success": False, "error": f"Enhancement failed at step {i+1}: {str(e)}"}
            break
    return {"success": True, "enhanced": current, "steps": steps, "totalLevels": len(steps)}


async def handle_generate_prompt():
    seed_ideas = [
        "cinematic nature scene with dramatic lighting",
        "futuristic city with flying cars at night",
        "underwater ocean exploration with marine life",
        "fantasy dragon flying through clouds",
        "slow motion abstract art fluid dynamics",
        "vintage retro aesthetic street photography",
        "space exploration astronaut floating in nebula",
        "magical forest with glowing fireflies at dusk",
        "cyberpunk neon city rain reflections",
        "time lapse of blooming flowers in garden",
    ]
    seed = random.choice(seed_ideas)
    try:
        result = await _single_enhance(seed)
        return {"success": True, "prompt": result, "seed": seed}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def handle_batch_validate(prompts: list):
    if not prompts:
        return {"success": False, "error": "No prompts provided"}
    valid = [p.strip() for p in prompts if len(p.strip()) >= 15]
    if not valid:
        return {"success": False, "error": "No valid prompts (min 15 chars)"}
    return {"success": True, "validPrompts": valid, "count": len(valid)}


async def handle_telegram_setup(bot_token: str, chat_id: str):
    bot_token = bot_token.strip()
    chat_id = chat_id.strip()
    if not bot_token or not chat_id:
        return {"success": False, "error": "Both required"}
    try:
        result = await tg_call(bot_token, "getMe")
        if not result.get("ok"):
            return {"success": False, "error": "Invalid bot token"}
        bot_name = result["result"].get("username", "?")
    except Exception as e:
        return {"success": False, "error": f"Could not verify bot: {e}"}
    _telegram_config["bot_token"] = bot_token
    _telegram_config["chat_id"] = chat_id
    start_bot(bot_token)
    return {"success": True, "message": f"Bot @{bot_name} connected!"}


async def handle_telegram_send(video_url: str, caption: str):
    token = _telegram_config.get("bot_token")
    chat_id = _telegram_config.get("chat_id")
    if not token or not chat_id:
        return {"success": False, "error": "Telegram not configured"}
    if not video_url:
        return {"success": False, "error": "No video URL"}
    try:
        result = await tg_send_video(token, chat_id, video_url, caption or "Generated by Seedance Pro")
        if result.get("ok"):
            return {"success": True, "message": "Video sent to Telegram!"}
        return {"success": False, "error": result.get("description", "Send failed after retries")}
    except Exception as e:
        return {"success": False, "error": str(e)}


# Serve static frontend
STATIC_DIR = Path(__file__).parent / "static"

@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    file_path = STATIC_DIR / full_path
    if file_path.is_file():
        return FileResponse(file_path)
    return FileResponse(STATIC_DIR / "index.html")
