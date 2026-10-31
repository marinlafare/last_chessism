"""Generation-pinned JSON API batches; bounded retries and thread-owned sessions."""
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
import random
import re
import threading
from urllib.parse import quote, urlencode
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from .cloud import BUCKET, CleanupError, TRANSIENT_STATUSES, MAX_TRANSIENT_RETRIES

ENDPOINT = 'https://storage.googleapis.com/batch/storage/v1'
MAX_RESPONSE = 2 * 1024 * 1024


def encode_batch(objects):
    if not 1 <= len(objects) <= 100:
        raise ValueError('Deletion batches require 1–100 objects')
    boundary = 'chessism_' + uuid4().hex
    parts, identities = [], {}
    for index, obj in enumerate(objects):
        if (not isinstance(obj.get('name'), str) or not obj['name']
                or not isinstance(obj.get('generation'), str)
                or not re.fullmatch(r'[0-9]+', obj.get('generation', ''))):
            raise ValueError('Deletion requires an exact object name and generation')
        ident = f'delete-{index}'
        identities[ident] = obj
        query = urlencode({'generation': obj['generation'], 'ifGenerationMatch': obj['generation']})
        path = f'/storage/v1/b/{BUCKET}/o/{quote(obj["name"], safe="")}?{query}'
        parts.append(f'--{boundary}\r\nContent-Type: application/http\r\nContent-ID: <{ident}>\r\n\r\n'
                     f'DELETE {path} HTTP/1.1\r\n\r\n')
    raw = (''.join(parts) + f'--{boundary}--\r\n').encode()
    if len(raw) >= 10 * 1024 * 1024:
        raise ValueError('Deletion batch exceeds the Google payload limit')
    return raw, f'multipart/mixed; boundary={boundary}', identities


def decode_batch(content_type, raw, identities):
    if len(raw) > MAX_RESPONSE or '\r' in content_type or '\n' in content_type:
        raise ValueError('Invalid deletion response')
    message = BytesParser(policy=policy.default).parsebytes(
        b'Content-Type: ' + content_type.encode('ascii') + b'\r\nMIME-Version: 1.0\r\n\r\n' + raw)
    if message.get_content_type() != 'multipart/mixed' or not message.is_multipart() or message.defects:
        raise ValueError('Missing multipart deletion response')
    results = {}
    for part in message.iter_parts():
        ident = str(part.get('Content-ID', ''))
        match = re.fullmatch(r'<response-(delete-[0-9]+)>', ident)
        if not match or match[1] not in identities or match[1] in results or part.defects:
            raise ValueError('Missing, duplicate or unknown deletion response identity')
        if part.get_content_type() != 'application/http':
            raise ValueError('Unexpected deletion response type')
        payload = part.get_payload(decode=True)
        first, separator, rest = payload.partition(b'\r\n')
        status = re.fullmatch(rb'HTTP/1\.[01] ([0-9]{3})(?: .*)?', first)
        if not separator or not status:
            raise ValueError('Malformed deletion status')
        headers = BytesParser(policy=policy.default).parsebytes(rest, headersonly=True)
        results[match[1]] = (int(status[1]), headers.get('Retry-After'))
    if results.keys() != identities.keys():
        raise ValueError('Incomplete deletion response')
    return results


def retry_delay(attempt, values):
    delay = 2 ** attempt + random.uniform(0, .25)
    for value in values:
        if not value:
            continue
        try:
            seconds = (int(value) if value.isdigit() else
                       (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            continue
        if seconds > 30:
            raise CleanupError('Deletion retry delay exceeds the bounded retry budget')
        delay = max(delay, seconds)
    return delay


class BatchTransport:
    def __init__(self, cloud, abort, *, session_factory=requests.Session):
        self.cloud, self.abort, self.session_factory = cloud, abort, session_factory
        self.local, self.lock, self.sessions = threading.local(), threading.Lock(), []
        self.requests = self.retries = 0

    def stopped(self):
        stopping = getattr(self.cloud, 'stopping', None)
        return self.abort.is_set() or bool(stopping is not None and stopping.is_set())

    def session(self):
        if not hasattr(self.local, 'session'):
            session = self.session_factory()
            session.trust_env = False
            session.mount('https://', HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0))
            with self.lock:
                self.sessions.append(session)
            self.local.session = session
        return self.local.session

    def post(self, raw, content_type, token):
        with self.cloud._request_slots:
            with self.lock:
                self.requests += 1
            with self.session().post(ENDPOINT, data=raw, headers={
                'Authorization': 'Bearer ' + token, 'Content-Type': content_type,
                'User-Agent': 'chessism-cleaning-job/2'}, stream=True,
                timeout=(10, 30), allow_redirects=False) as response:
                body = bytearray()
                for chunk in response.iter_content(65536):
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE:
                        raise ValueError('Oversized deletion response')
                return response.status_code, response.headers, bytes(body)

    def remove(self, objects, acknowledge):
        pending, attempts, refreshed = list(objects), 0, False
        token, _ = self.cloud.tokens.get()
        while pending:
            if self.stopped():
                raise CleanupError('Deletion interrupted; exact-generation plan retained')
            raw, content_type, identities = encode_batch(pending)
            retry_after, auth = [], False
            results = None
            try:
                code, headers, body = self.post(raw, content_type, token)
                if code == 200:
                    results = decode_batch(headers.get('Content-Type', ''), body, identities)
                else:
                    results = {key: (code, headers.get('Retry-After')) for key in identities}
            except (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError, ValueError) as exc:
                if isinstance(exc, requests.exceptions.SSLError):
                    raise CleanupError('Deletion TLS verification failed') from None
                # The outcome is unknown. Retrying identical generations cannot
                # delete replacements; never treat a broken envelope as success.
            if results is not None:
                successful, remaining, permanent = [], [], []
                for key, (status, delay) in results.items():
                    if status in {200, 204, 404} and (code == 200):
                        successful.append(identities[key])
                    elif status == 401 and not refreshed:
                        auth = True
                        remaining.append(identities[key])
                    elif status in TRANSIENT_STATUSES:
                        remaining.append(identities[key])
                        retry_after.append(delay)
                    else:
                        permanent.append(status)
                if successful:
                    acknowledge(successful)
                if permanent:
                    raise CleanupError(f'Deletion refused (HTTP {permanent[0]}); exact-generation plan retained')
                pending = remaining
                if not pending:
                    return
            if auth:
                token, _ = self.cloud.tokens.get(force=True, rejected_token=token)
                refreshed = True
                continue
            if attempts >= MAX_TRANSIENT_RETRIES:
                raise CleanupError('Deletion retry budget exhausted; exact-generation plan retained')
            delay = retry_delay(attempts, retry_after)
            attempts += 1
            with self.lock:
                self.retries += 1
            if self.abort.wait(delay):
                raise CleanupError('Deletion interrupted; exact-generation plan retained')

    def close(self):
        for session in self.sessions:
            session.close()
