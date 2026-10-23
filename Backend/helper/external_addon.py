"""Optional external Stremio stream provider; URLs stay in server configuration."""
import asyncio
import json
import os
from urllib.parse import quote, urlsplit

import httpx

from Backend.logger import LOGGER

MAX_RESPONSE_BYTES = 2 * 1024 * 1024


async def external_streams(media_type: str, media_id: str) -> list[dict]:
    manifest = os.getenv("EXTERNAL_ADDON_MANIFEST_URL", "").strip()
    if not manifest:
        return []
    try:
        parsed = urlsplit(manifest)
    except ValueError:
        return []
    if (parsed.scheme != "https" or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or not parsed.path.endswith("/manifest.json")):
        LOGGER.warning("External addon configuration must be an HTTPS manifest URL")
        return []
    kind = "series" if media_type == "tv" else media_type
    if kind not in {"movie", "series"}:
        return []
    # Synthetic combined-season IDs are private to Telegram-Stremio.
    base = manifest[:-len("/manifest.json")]
    endpoint = f"{base}/stream/{kind}/{quote(media_id, safe='')}.json"
    try:
        async with asyncio.timeout(10):
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0), follow_redirects=False) as client:
                async with client.stream("GET", endpoint) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_RESPONSE_BYTES:
                            raise ValueError("Response too large")
        data = json.loads(body)
        entries = data.get("streams", []) if isinstance(data, dict) else []
        if not isinstance(entries, list):
            return []
        streams = []
        seen = set()
        for entry in entries[:100]:
            if not isinstance(entry, dict):
                continue
            if not any(isinstance(entry.get(k), str) and entry[k]
                       for k in ("url", "infoHash", "externalUrl")):
                continue
            # Preserve stream fields (headers, subtitles, torrent data, etc.).
            item = dict(entry)
            name = item.get("name")
            item["name"] = f"PencariMovie · {name}" if isinstance(name, str) and name else "PencariMovie"
            fingerprint = json.dumps(item, sort_keys=True)
            if fingerprint not in seen:
                streams.append(item)
                seen.add(fingerprint)
        return streams
    except Exception as exc:
        # Do not log the exception text: it may contain the secret manifest URL.
        LOGGER.warning("External addon unavailable (%s); keeping Telegram results", type(exc).__name__)
        return []
