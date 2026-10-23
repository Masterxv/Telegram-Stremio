"""Resolve source streams plus other providers using verified shared IMDb IDs."""
import asyncio
import re
import time
import unicodedata

from Backend.helper.external_addon import _configured_addons, _provider_streams
from Backend.helper.external_catalogs import _json, _unwrap, _typed_id, movie_meta

_matches = {}


def _imdb(value):
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r'tt\d+', value.strip())
    if match:
        return match.group()
    match = re.search(r'https?://(?:www\.)?imdb\.com/title/(tt\d+)(?:/|\?|$)', value)
    return match.group(1) if match else None


def _metadata_imdb(meta):
    for key in ('imdb_id', 'imdbId', 'imdb', 'id'):
        found = _imdb(meta.get(key))
        if found:
            return found
    for link in meta.get('links', []) if isinstance(meta.get('links'), list) else []:
        if isinstance(link, dict):
            found = _imdb(link.get('url'))
            if found:
                return found
    return None


def _year(meta):
    for field in ('release_year', 'year', 'releaseInfo', 'released'):
        match = re.search(r'\b(?:19|20)\d{2}\b', str(meta.get(field) or ''))
        if match:
            return int(match.group())
    return None


def _name(value):
    return ''.join(c for c in unicodedata.normalize('NFKC', str(value)).casefold() if c.isalnum())


async def _matching_imdb(document, metadata, kind):
    direct = _metadata_imdb(metadata) or _imdb(document.get('external_imdb_id')) or _imdb(document.get('imdb_id'))
    if direct:
        return direct
    title = metadata.get('name') or document.get('title')
    year = _year(metadata) or _year(document)
    if not title or not year:
        return None
    key = (kind, _name(title), year)
    cached = _matches.get(key)
    if cached and cached[0] > time.monotonic():
        return cached[1]
    # Exact normalized title AND year only; ambiguous/fuzzy matches are rejected.
    from urllib.parse import quote
    result = await _json('https://v3-cinemeta.strem.io/manifest.json',
                         '/catalog/' + kind + '/top/search=' + quote(str(title), safe='') + '.json')
    candidates = result.get('metas', [])
    found = set()
    if isinstance(candidates, list):
        for item in candidates[:100]:
            if not isinstance(item, dict) or _name(item.get('name')) != _name(title):
                continue
            imdb_id = _imdb(item.get('id'))
            if not imdb_id:
                continue
            candidate_year = _year(item)
            if candidate_year is None:
                details = await _json('https://v3-cinemeta.strem.io/manifest.json', '/meta/' + kind + '/' + imdb_id + '.json')
                candidate_year = _year(details.get('meta') or {})
            if candidate_year == year:
                found.add(imdb_id)
    match = next(iter(found)) if len(found) == 1 else None
    if len(_matches) >= 2000:
        _matches.clear()
    _matches[key] = (time.monotonic() + (3600 if match else 60), match)
    return match


async def combined_library_streams(document, metadata=None, video_id=''):
    source_id = document.get('external_media_id')
    source = _unwrap('ext', video_id or source_id or '')
    if not source:
        return {'streams': [], 'providers': [{'name': 'CNCVerse', 'status': 'source_not_configured', 'count': 0}]}
    label, source_url, encoded = source
    source_type, raw_id = _typed_id(encoded)
    if metadata is None:
        metadata = (await movie_meta(source_id)).get('meta') or {}
    videos = metadata.get('videos') or []
    kind = 'series' if document.get('media_type') == 'tv' else 'movie'
    if isinstance(videos, list) and any(isinstance(v, dict) and v.get('season') is not None for v in videos):
        kind = 'series'
    statuses = [{'name': label, 'status': 'loading', 'count': 0}]
    # Start the original provider immediately, while shared-ID matching runs.
    source_task = asyncio.create_task(_provider_streams(label, source_url, source_type, raw_id, statuses[0]))
    try:
        others = [(name, url) for name, url in _configured_addons() if url != source_url]
        match_status = 'no_verified_imdb_match'
        matching_doc = dict(document)
        if _imdb(raw_id):
            matching_doc['external_imdb_id'] = _imdb(raw_id)
        try:
            imdb_id = await _matching_imdb(matching_doc, metadata, kind) if others else None
        except Exception:
            imdb_id = None
            match_status = 'matching_unavailable'
        shared_id = imdb_id
        if kind == 'series':
            selected = next((v for v in videos if isinstance(v, dict) and v.get('id') == video_id), None) if isinstance(videos, list) else None
            if selected and isinstance(selected.get('season'), int) and isinstance(selected.get('episode'), int) and imdb_id:
                shared_id = f"{imdb_id}:{selected['season']}:{selected['episode']}"
            else:
                shared_id = None  # Never guess a series episode.
        tasks = []
        for name, url in others:
            state = {'name': name, 'status': 'loading', 'count': 0}
            statuses.append(state)
            if not shared_id:
                state['status'] = 'episode_not_selected' if kind == 'series' and imdb_id else match_status
                continue
            tasks.append(_provider_streams(name, url, kind, shared_id, state))
        results = await asyncio.gather(source_task, *tasks)
    except BaseException:
        source_task.cancel()
        await asyncio.gather(source_task, return_exceptions=True)
        raise
    return {'streams': [stream for streams in results for stream in streams], 'providers': statuses}
