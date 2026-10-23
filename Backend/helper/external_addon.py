"""Optional external Stremio providers; credential URLs stay in configuration."""
import asyncio
import json
import os
from urllib.parse import quote, urlsplit

import httpx

from Backend.logger import LOGGER

MAX_RESPONSE_BYTES = 2 * 1024 * 1024


def _configured_addons() -> list[tuple[str, str]]:
    addons = []
    seen = set()
    for number in range(1, 6):
        prefix = "EXTERNAL_ADDON" if number == 1 else f"EXTERNAL_ADDON_{number}"
        manifest = os.getenv(f"{prefix}_MANIFEST_URL", "").strip()
        if not manifest or manifest in seen:
            continue
        label = os.getenv(f"{prefix}_NAME", "").strip() or f"Addon {number}"
        # Keep credentials out of the source and logs, including malformed URLs.
        addons.append((label[:80], manifest))
        seen.add(manifest)
    return addons


async def external_streams(media_type: str, media_id: str) -> list[dict]:
    addons = _configured_addons()
    if not addons:
        return []
    # Query independently and concurrently: one failed provider cannot hide another.
    results = await asyncio.gather(*(
        _provider_streams(label, manifest, media_type, media_id)
        for label, manifest in addons
    ))
    return [stream for result in results for stream in result]


async def _provider_streams(label: str, manifest: str, media_type: str, media_id: str) -> list[dict]:
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
            item["name"] = f"{label} · {name}" if isinstance(name, str) and name else label
            fingerprint = json.dumps(item, sort_keys=True)
            if fingerprint not in seen:
                streams.append(item)
                seen.add(fingerprint)
        return streams
    except Exception as exc:
        # Do not log the exception text: it may contain the secret manifest URL.
        LOGGER.warning("External addon unavailable (%s); keeping Telegram results", type(exc).__name__)
        return []
