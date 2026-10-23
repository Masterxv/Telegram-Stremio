"""Import CNCVerse catalog metadata into the actual TG Stremio Media Library."""
import asyncio
import hashlib
import json
import os
import re
from datetime import datetime
from urllib.parse import quote, urlencode, urlsplit

from Backend.helper.external_addon import _configured_addons
from Backend.helper.external_catalogs import _json, _key, _wrap
from Backend.logger import LOGGER


def _number(name, default, maximum):
    try:
        return max(1, min(maximum, int(os.getenv(name, str(default)))))
    except ValueError:
        return default


def _sources():
    explicit = os.getenv('CNCVERSE_IMPORT_MANIFEST_URL', '').strip()
    if explicit:
        return [('CNCVerse', explicit)]
    result = []
    for label, url in _configured_addons():
        try:
            if urlsplit(url).hostname == 'cncverse.dpdns.org':
                result.append((label, url))
        except ValueError:
            pass
    return result


def _release_year(item):
    for field in ('release_year', 'year', 'releaseInfo', 'released', 'name'):
        match = re.search(r'\b(?:19|20)\d{2}\b', str(item.get(field) or ''))
        if match:
            return int(match.group())
    return None


def _languages(item):
    aliases = {'english': 'en', 'eng': 'en', 'en': 'en',
               'telugu': 'te', 'tel': 'te', 'te': 'te',
               'hindi': 'hi', 'hin': 'hi', 'hi': 'hi'}
    found = set()
    for field in ('language', 'languages', 'original_language', 'audio_languages'):
        value = item.get(field)
        values = value if isinstance(value, list) else [value]
        for entry in values:
            if isinstance(entry, str):
                for word in re.findall(r'[a-z]+', entry.lower()):
                    if word in aliases:
                        found.add(aliases[word])
    # Only full language names in filenames, not ambiguous two-letter words.
    title = re.sub(r'\b(?:english|telugu|hindi)\s+(?:subtitles?|subs?)\b', '', str(item.get('name') or ''), flags=re.I)
    for word in re.findall(r'\b(?:english|telugu|hindi)\b', title, re.I):
        found.add(aliases[word.lower()])
    return found


def _eligible(item):
    return _release_year(item) == 2026 and bool(_languages(item))


async def _filtered_item(item, url, kind):
    if not isinstance(item, dict):
        return None
    year = _release_year(item)
    if year is not None and year != 2026:
        return None
    if _eligible(item):
        return item
    raw = item.get('id')
    if not isinstance(raw, str) or not raw:
        return None
    source_type = item.get('type') or kind
    if source_type not in {'movie', 'series', 'other'}:
        return None
    data = await _json(url, '/meta/' + source_type + '/' + quote(raw, safe='') + '.json')
    metadata = data.get('meta')
    if not isinstance(metadata, dict):
        return None
    merged = {**item, **metadata, 'id': raw, 'type': source_type}
    # Do not replace known preview fields with empty provider metadata.
    for key, value in item.items():
        if merged.get(key) in (None, '', []):
            merged[key] = value
    return merged if _eligible(merged) else None


def _document(item, url, kind):
    if not _eligible(item):
        return None
    raw = item.get('id')
    title = item.get('name')
    if not isinstance(raw, str) or not raw or not isinstance(title, str) or not title:
        return None
    # Other is a mixed shelf; retain its source type and resolve full metadata
    # from the provider when opening the title instead of guessing a TMDb match.
    source_type = item.get('type') or kind
    if source_type not in {'movie', 'series', 'other'}:
        return None
    digest = hashlib.sha256((_key(url) + '\0' + source_type + '\0' + raw).encode()).hexdigest()
    try:
        rating = float(item.get('imdbRating') or 0) or None
    except (ValueError, TypeError):
        rating = None
    now = datetime.utcnow()
    document = {
        '_id': 'cncverse_' + digest,
        'tmdb_id': -(int(digest[:13], 16) + 1),
        'imdb_id': 'cncimport' + digest,
        'db_index': 1,
        'title': title,
        'poster': item.get('poster') or '',
        'backdrop': item.get('background') or '',
        'description': item.get('description') or '',
        'genres': item.get('genres') if isinstance(item.get('genres'), list) else [],
        'rating': rating,
        'release_year': 2026,
        'external_import_languages': sorted(_languages(item)),
        'media_type': 'tv' if source_type == 'series' else 'movie',
        'telegram': [],
        'visibility': 'public',
        'allowed_tokens': [],
        'external_source': 'CNCVerse',
        'external_source_type': source_type,
        'external_media_id': _wrap('ext', _key(url), json.dumps([source_type, raw])),
        'created_on': now,
        'updated_on': now,
    }
    if document['media_type'] == 'tv':
        document['seasons'] = []
    return document


async def _save(db, item, url, kind, catalog_id):
    item = await _filtered_item(item, url, kind)
    if item is None:
        return None
    doc = _document(item, url, kind)
    if not doc:
        return None
    col = db.dbs['storage_1'][doc['media_type']]
    # Stable source identity makes refreshes/restarts idempotent. Preserve edits
    # and all native Telegram documents; imported titles have their own IDs.
    await col.update_one({'_id': doc['_id']}, {
        '$setOnInsert': doc,
        '$set': {'external_last_seen': datetime.utcnow()},
        '$addToSet': {'external_catalogs': catalog_id},
    }, upsert=True)
    return doc['_id']


async def sync_library(db):
    sources = _sources()
    if not sources:
        LOGGER.info('CNCVerse import: no CNCVerse manifest configured')
        return
    pages = _number('CNCVERSE_IMPORT_MAX_PAGES', 10, 100)
    max_items = _number('CNCVERSE_IMPORT_MAX_ITEMS', 50000, 200000)
    counts = {'processed': 0, 'pages': 0, 'empty_or_failed_pages': 0, 'skipped': 0}
    seen = set()
    checked = set()
    tracking = db.dbs['tracking']['state']
    await tracking.update_one({'_id': 'cncverse_import'}, {'$set': {
        'status': 'running', 'started_at': datetime.utcnow(), **counts,
    }}, upsert=True)
    LOGGER.info('CNCVerse import: started; filter=2026 + English/Telugu/Hindi; matching titles will appear on Media')
    for label, url in sources:
        manifest = await _json(url)
        catalogs = manifest.get('catalogs') or []
        if not isinstance(catalogs, list):
            continue
        catalogs = [c for c in catalogs if isinstance(c, dict) and c.get('type') in {'movie', 'series', 'other'} and isinstance(c.get('id'), str)]
        # Import every base shelf first, then category/genre shelves. This makes
        # the initial Media Library populate without waiting for all categories.
        jobs = [(c, None) for c in catalogs]
        for c in catalogs:
            extras = c.get('extra') or []
            for extra in extras if isinstance(extras, list) else []:
                if isinstance(extra, dict) and extra.get('name') == 'genre':
                    options = extra.get('options') or []
                    if isinstance(options, list):
                        jobs.extend((c, genre) for genre in options[:100] if isinstance(genre, str))
        for catalog, genre in jobs:
            offset = 0
            page_ids_seen = set()
            for page in range(pages):
                params = {}
                if genre is not None:
                    params['genre'] = genre
                if offset:
                    params['skip'] = str(offset)
                suffix = '/catalog/' + catalog['type'] + '/' + quote(catalog['id'], safe='')
                if params:
                    suffix += '/' + urlencode(params, quote_via=quote)
                data = await _json(url, suffix + '.json')
                items = data.get('metas') or []
                if not isinstance(items, list) or not items:
                    counts['empty_or_failed_pages'] += 1
                    break
                counts['pages'] += 1
                fingerprint = tuple(item.get('id') for item in items if isinstance(item, dict) and isinstance(item.get('id'), str))
                if not fingerprint or fingerprint in page_ids_seen:
                    break  # Providers may ignore skip and repeat the first page.
                page_ids_seen.add(fingerprint)
                for item in items[:500]:
                    if not isinstance(item, dict):
                        continue
                    identity = (url, item.get('type') or catalog['type'], str(item.get('id') or ''))
                    if identity in checked:
                        continue
                    checked.add(identity)
                    doc_id = await _save(db, item, url, catalog['type'], catalog['id'])
                    if not doc_id:
                        counts['skipped'] += 1
                    if len(checked) % 25 == 0:
                        LOGGER.info('CNCVerse import: checked %s titles, %s matched so far', len(checked), counts['processed'])
                    if doc_id and doc_id not in seen:
                        seen.add(doc_id)
                        counts['processed'] += 1
                    if counts['processed'] >= max_items:
                        break
                await tracking.update_one({'_id': 'cncverse_import'}, {'$set': counts})
                LOGGER.info('CNCVerse import: %s matching titles, %s skipped, %s catalog pages', counts['processed'], counts['skipped'], counts['pages'])
                if counts['processed'] >= max_items:
                    break
                # Continue only where the catalog advertises skip pagination.
                extras = catalog.get('extra') or []
                supports_skip = isinstance(extras, list) and any(isinstance(e, dict) and e.get('name') == 'skip' for e in extras)
                supports_skip = supports_skip or 'skip' in (catalog.get('extraSupported') or [])
                if not supports_skip:
                    break
                offset += len(items)
                await asyncio.sleep(0.25)
            if counts['processed'] >= max_items:
                break
        if counts['processed'] >= max_items:
            break
    await tracking.update_one({'_id': 'cncverse_import'}, {'$set': {
        'status': 'finished', 'finished_at': datetime.utcnow(), **counts,
    }})
    LOGGER.info('CNCVerse import finished: %s unique titles processed. Refresh Media.', counts['processed'])


async def run_auto_import(db):
    if os.getenv('CNCVERSE_AUTO_IMPORT', 'true').lower() not in {'true', '1', 'yes'}:
        return
    await asyncio.sleep(5)
    while True:
        try:
            await sync_library(db)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.error('CNCVerse import failed (%s); retrying next cycle', type(exc).__name__)
            try:
                await db.dbs['tracking']['state'].update_one({'_id': 'cncverse_import'}, {'$set': {
                    'status': 'error', 'error_type': type(exc).__name__, 'finished_at': datetime.utcnow(),
                }}, upsert=True)
            except Exception:
                pass
        await asyncio.sleep(_number('CNCVERSE_IMPORT_INTERVAL_HOURS', 6, 168) * 3600)
