"""Release leases atomically; never remove a replacement owner's lease."""

RELEASE_OWNED_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


async def release_owned(redis, key: str, owner: str) -> bool:
    return bool(await redis.eval(RELEASE_OWNED_LUA, 1, key, owner))
