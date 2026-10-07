from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_CLOUD_FENS = 200_000
MAX_CLOUD_RUNS = MAX_CLOUD_FENS // 1000


class CloudVmCountRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n_vms: int = Field(ge=1, le=10, strict=True)


class CloudJobRequest(BaseModel):
    # Engine settings belong to the server, never to a submitted UI payload.
    model_config = ConfigDict(extra="forbid")

    mode: Literal["all", "player", "loop", "games"] = "all"
    backend: Literal["batch_spot", "cloud_run"] = "batch_spot"
    n_cpus: int = Field(4, ge=1, le=64, strict=True)
    n_vms: int = Field(1, ge=1, le=10, strict=True)
    player_name: str = Field("", max_length=200)
    total_fens: int | None = Field(None, ge=1, le=MAX_CLOUD_FENS)
    positions_per_run: int = Field(1000, ge=1, le=1000)
    runs: int = Field(1, ge=1, le=MAX_CLOUD_RUNS)
    plan_id: str | None = Field(None, pattern=r"^[a-f0-9]{32}$")

    @model_validator(mode="after")
    def validate_selection(self):
        self.player_name = self.player_name.strip()
        if self.mode == "all":
            self.player_name = ""
        if self.mode == "player" and not self.player_name:
            raise ValueError("A player name is required")
        if self.mode == "games" and not self.plan_id:
            raise ValueError("Preview and confirm a game selection first")
        if self.mode == "games" and self.total_fens is not None:
            raise ValueError("The FEN count for game selections is calculated by the server")
        return self

    @property
    def target(self):
        if self.mode == "games":
            raise ValueError("Resolve the FEN count from the saved game preview")
        return self.positions_per_run * self.runs if self.mode == "loop" else (
            self.total_fens if self.total_fens is not None else 1000
        )
