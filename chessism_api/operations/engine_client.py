"""One bounded HTTP retry policy, shared by queued and immediate analysis."""
import asyncio
import math

import httpx
from stockfish_core.http_policy import CONNECT_SECONDS, MAX_ATTEMPTS, RETRY_DELAYS, request_seconds

ENGINE_URL = 'http://stockfish-service:9999/analyze'
RETRYABLE_STATUS = {408, 429, 502, 503, 504}
RETRYABLE_TRANSPORT = (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                       httpx.RemoteProtocolError, httpx.TimeoutException)


class EngineServiceRequestError(RuntimeError):
    """An unsuccessful engine request; callers must not reselect/retry its batch."""
    def __init__(self, message, status_code=502):
        super().__init__(message)
        self.status_code = status_code


def response_results(response, fens):
    try:
        result = response.json()
    except ValueError as error:
        raise EngineServiceRequestError('Malformed Stockfish JSON response') from error
    if (not isinstance(result, list) or len(result) != len(fens)
            or any(not isinstance(row, dict) or row.get('fen') != fen
                   or type(row.get('is_valid')) is not bool
                   or not isinstance(row.get('analysis'), (dict, list))
                   for row, fen in zip(result, fens))):
        raise EngineServiceRequestError('Incomplete or mismatched Stockfish response')
    # Invalid positions retain the immediate endpoint's existing result envelope.
    # A queued persistence consumer rejects them instead of committing a partial batch.
    for row in result:
        if not row['is_valid']:
            continue
        lines = row['analysis'] if isinstance(row['analysis'], list) else [row['analysis']]
        if not lines or any(not isinstance(line, dict)
                            or type(line.get('score')) not in (int, float)
                            or not -2147483648 <= line['score'] <= 2147483647
                            or not math.isfinite(line['score'])
                            or line['score'] != int(line['score']) for line in lines):
            raise EngineServiceRequestError('Stockfish response has no valid score')
    return result


async def call_engine_service(client, url, fens, nodes, *, multipv=4,
                              progress_job_id=None, progress_attempt_id=None,
                              progress_total=None, progress_detail_prefix=None):
    payload = {'fens': fens, 'nodes_limit': nodes, 'multipv': multipv}
    if progress_job_id:
        payload.update(progress_job_id=progress_job_id, progress_attempt_id=progress_attempt_id,
                       progress_total=progress_total or len(fens),
                       progress_detail_prefix=progress_detail_prefix)
    deadline = request_seconds(len(fens), nodes)
    timeout = httpx.Timeout(deadline, connect=CONNECT_SECONDS, write=CONNECT_SECONDS, pool=CONNECT_SECONDS)

    async def request():
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await client.post(url, json=payload, timeout=timeout)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                failure = EngineServiceRequestError(
                    f'Stockfish HTTP {status}: {error.response.text}', status)
                if status not in RETRYABLE_STATUS or attempt == MAX_ATTEMPTS - 1:
                    raise failure from error
            except RETRYABLE_TRANSPORT as error:
                status = 504 if isinstance(error, httpx.TimeoutException) else 503
                failure = EngineServiceRequestError(f'Stockfish transport failed: {error}', status)
                if attempt == MAX_ATTEMPTS - 1:
                    raise failure from error
            except httpx.RequestError as error:
                raise EngineServiceRequestError(f'Invalid Stockfish request: {error}') from error
            else:
                return response_results(response, fens)
            await asyncio.sleep(RETRY_DELAYS[attempt])

    try:
        return await asyncio.wait_for(request(), timeout=deadline)
    except asyncio.TimeoutError as error:
        raise EngineServiceRequestError('Stockfish request deadline exceeded', 504) from error
