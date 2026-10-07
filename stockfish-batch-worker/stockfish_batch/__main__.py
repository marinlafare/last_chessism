import asyncio
import signal
import sys

from .config import parse_args
from .worker import emit, run


STALL_EXIT_CODE = 124


def has_timeout(error):
    """TaskGroup wraps worker/watchdog failures; keep the CLI classification."""
    if isinstance(error, BaseExceptionGroup):
        return any(has_timeout(child) for child in error.exceptions)
    return isinstance(error, TimeoutError)


async def execute(config):
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    received = []

    def stop(number):
        if not received:
            received.append(number)
            emit("stopping", signal=number)
            task.cancel()

    signals = (signal.SIGTERM, signal.SIGINT)
    for number in signals:
        loop.add_signal_handler(number, stop, number)
    try:
        await run(config)
        return 0
    except asyncio.CancelledError:
        emit("interrupted", message="Committed checkpoints remain reusable; unfinished FENs may run again")
        return 128 + (received[0] if received else signal.SIGTERM)
    except Exception as exc:
        code = STALL_EXIT_CODE if has_timeout(exc) else 1
        emit("failed", error=repr(exc), exit_code=code,
             failure_kind="timeout" if code == STALL_EXIT_CODE else "application_error",
             message="No successful completion reported; inspect saved checkpoints")
        return code
    finally:
        for number in signals:
            loop.remove_signal_handler(number)


def main():
    return asyncio.run(execute(parse_args()))


if __name__ == "__main__":
    sys.exit(main())
