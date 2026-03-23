from pathlib import Path


class Settings:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.runtime_dir = self.base_dir / "runtime"
        self.upload_dir = self.runtime_dir / "uploads"
        self.parse_dir = self.runtime_dir / "parsed"
        self.snapshot_dir = self.runtime_dir / "snapshots"
        self.meta_file = self.runtime_dir / "reports_index.json"

    def ensure_dirs(self) -> None:
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.parse_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.ensure_dirs()
