"""
Engine configuration and operating modes.
Separates control-plane concerns from the execution runtime.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass


@dataclass
class EngineConfig:
    mode: str
    num_partitions: int
    shard_count: int
    max_queue_size: int
    batch_size: int
    base_rate: float
    flash_sale_multiplier: float
    flash_sale_duration_seconds: float
    session_delay_scale: float
    reset_on_start: bool
    recover_on_start: bool
    log_dir: str
    wal_path: str
    snapshot_path: str
    wal_compact_interval_seconds: float
    log_max_bytes_per_partition: int

    @classmethod
    def from_env(cls) -> "EngineConfig":
        mode = os.getenv("ENGINE_MODE", "demo").lower()
        num_partitions = int(os.getenv("ENGINE_NUM_PARTITIONS", "8"))
        shard_default = str(num_partitions)
        shard_count = int(os.getenv("ENGINE_SHARD_COUNT", shard_default))

        presets = {
            "dev": {
                "base_rate": 6_000.0,
                "flash_sale_multiplier": 1.1,
                "flash_sale_duration_seconds": 10.0,
                "reset_on_start": True,
                "recover_on_start": False,
            },
            "demo": {
                "base_rate": 10_000.0,
                "flash_sale_multiplier": 1.2,
                "flash_sale_duration_seconds": 20.0,
                "reset_on_start": True,
                "recover_on_start": False,
            },
            "stress": {
                "base_rate": 12_000.0,
                "flash_sale_multiplier": 1.3,
                "flash_sale_duration_seconds": 25.0,
                "reset_on_start": True,
                "recover_on_start": False,
            },
            "recovery": {
                "base_rate": 8_000.0,
                "flash_sale_multiplier": 1.1,
                "flash_sale_duration_seconds": 15.0,
                "reset_on_start": False,
                "recover_on_start": True,
            },
        }
        preset = presets.get(mode, presets["demo"])

        log_dir = os.getenv("ENGINE_LOG_DIR", "logs")
        wal_path = os.getenv("ENGINE_WAL_PATH", "checkpoints/state.wal")
        snapshot_path = os.getenv("ENGINE_SNAPSHOT_PATH", "checkpoints/state.snapshot.json")

        return cls(
            mode=mode,
            num_partitions=num_partitions,
            shard_count=max(1, min(shard_count, num_partitions)),
            max_queue_size=int(os.getenv("ENGINE_MAX_QUEUE_SIZE", "100000")),
            batch_size=int(os.getenv("ENGINE_BATCH_SIZE", "256")),
            base_rate=float(os.getenv("ENGINE_BASE_RATE", str(preset["base_rate"]))),
            flash_sale_multiplier=float(
                os.getenv("ENGINE_FLASH_MULTIPLIER", str(preset["flash_sale_multiplier"]))
            ),
            flash_sale_duration_seconds=float(
                os.getenv(
                    "ENGINE_FLASH_DURATION_SECONDS",
                    str(preset["flash_sale_duration_seconds"]),
                )
            ),
            session_delay_scale=float(os.getenv("ENGINE_SESSION_DELAY_SCALE", "0.0")),
            reset_on_start=os.getenv(
                "ENGINE_RESET_ON_START",
                "1" if preset["reset_on_start"] else "0",
            ) == "1",
            recover_on_start=os.getenv(
                "ENGINE_RECOVER_ON_START",
                "1" if preset["recover_on_start"] else "0",
            ) == "1",
            log_dir=log_dir,
            wal_path=wal_path,
            snapshot_path=snapshot_path,
            wal_compact_interval_seconds=float(os.getenv("ENGINE_WAL_COMPACT_INTERVAL", "300")),
            log_max_bytes_per_partition=int(os.getenv("ENGINE_LOG_MAX_BYTES", str(50 * 1024 * 1024))),
        )

    def to_dict(self) -> dict:
        return asdict(self)
