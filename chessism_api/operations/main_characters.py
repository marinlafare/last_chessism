from typing import Dict, Any

from chessism_api.database.ask_db import (
    get_main_character_time_control_counts,
    get_top_main_characters_by_time_control
)


async def get_main_character_time_control_counts_payload() -> Dict[str, int]:
    """
    Operations-layer wrapper for main character mode counts.
    """
    return await get_main_character_time_control_counts()


async def get_top_main_characters_by_time_control_payload(
    time_control: str,
    limit: int = 5
) -> Dict[str, Any]:
    """
    Operations-layer wrapper for top main characters by time control.
    """
    return await get_top_main_characters_by_time_control(
        time_control=time_control,
        limit=limit
    )
