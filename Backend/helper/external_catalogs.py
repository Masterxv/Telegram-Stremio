"""Expose configured providers' movie catalogs through the authenticated addon."""
import asyncio
import base64
import hashlib
import json
import time
from urllib.parse import quote, urlsplit

import httpx

from Backend.helper.external_addon import _configured_addons, _provider_streams, MAX_RESPONSE_BYTES
from Backend.logger import LOGGER

_manifest_cache = {}


def _key(url):
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def _wrap(prefix, key, value):
    encoded = base64.urlsafe_b64encode(value.encode()).decode().rstrip('=')
    return f'{prefix}_{key}_{encoded}'


def _unwrap(prefix, value):
    try:
        tag, key, encoded = value.split('_', 2)
        if tag != prefix or len(encoded) > 4096:
            return None
        raw = base64.b64decode(encoded + '=' * (-len(encoded) % 4), altchars=b'-_', validate=True).decode()
        for label, url in _configured_addons():
            if _key(url) == key:
                return label, url, raw
    except (ValueError, UnicodeError):
        pass
    return None


async def _json(url, suffix=''):
    try:
        parsed = urlsplit(url)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment
                or not parsed.path.endswith('/manifest.json')):
            return {}
        endpoint = url[:-len('/manifest.json')] + suffix if suffix else url
        async with asyncio.timeout(10):
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0), follow_redirects=False) as client:
                async with client.stream('GET', endpoint) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_RESPONSE_BYTES:
                            return {}
        result = json.loads(body)
        return result if isinstance(result, dict) else {}
    except Exception as exc:
        LOGGER.warning('External catalog unavailable (%s)', type(exc).__name__)
        return {}


async def _manifest(url):
    cached = _manifest_cache.get(url)
    if cached and cached[0] > time.monotonic():
        return cached[1]
    result = await _json(url)
    # Bound memory even if configuration changes while the process is running.
    if len(_manifest_cache) >= 5:
        _manifest_cache.clear()
    _manifest_cache[url] = (time.monotonic() + (300 if result else 15), result)
    return result


async def movie_catalogs():
    addons = _configured_addons()
    manifests = await asyncio.gather(*(_manifest(url) for _, url in addons))
    result = []
    for (label, url), manifest in zip(addons, manifests):
        catalogs = manifest.get('catalogs', [])
        if not isinstance(catalogs, list):
            continue
        for catalog in catalogs[:100]:
            if not isinstance(catalog, dict) or catalog.get('type') != 'movie':
                continue
            cid = catalog.get('id')
            if not isinstance(cid, str) or not cid:
                continue
            item = dict(catalog)
            item['id'] = _wrap('extcat', _key(url), cid)
            item['name'] = f"{label} · {catalog.get('name') or cid}"
            result.append(item)
    return result


def _movie_item(item, url):
    if not isinstance(item, dict) or not isinstance(item.get('id'), str):
        return None
    result = dict(item)
    raw = item['id']
    # Keep shared IMDb IDs so streams can be combined across all providers.
    if not (raw.startswith('tt') and raw[2:].isdigit()):
        result['id'] = _wrap('ext', _key(url), raw)
    result['type'] = 'movie'
    return result


async def catalog_response(catalog_id, extra=None):
    provider = _unwrap('extcat', catalog_id)
    if not provider:
        return {'metas': []}
    _, url, raw = provider
    suffix = '/catalog/movie/' + quote(raw, safe='')
    if extra:
        suffix += '/' + quote(extra, safe='=&')
    data = await _json(url, suffix + '.json')
    items = data.get('metas', [])
    if not isinstance(items, list):
        return {'metas': []}
    return {'metas': [mapped for item in items[:500]
                      if (mapped := _movie_item(item, url)) is not None]}


async def movie_meta(media_id):
    provider = _unwrap('ext', media_id)
    providers = [provider] if provider else [(label, url, media_id) for label, url in _configured_addons()]
    results = await asyncio.gather(*(
        _json(url, '/meta/movie/' + quote(raw, safe='') + '.json')
        for _, url, raw in providers
    ))
    for (_, url, _), data in zip(providers, results):
        meta = _movie_item(data.get('meta'), url)
        if meta:
            meta['id'] = media_id
            return {'meta': meta}
    return {'meta': {}}


async def custom_movie_streams(media_id):
    provider = _unwrap('ext', media_id)
    if not provider:
        return []
    label, url, raw = provider
    return await _provider_streams(label, url, 'movie', raw)


def custom_movie_id(media_id):
    provider = _unwrap('ext', media_id)
    return provider[2] if provider else None
