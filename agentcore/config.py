"""Application configuration loaded from config.ini."""

import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

_CONFIG_PATH = Path(__file__).parent.parent / "config.ini"
_DEFAULT_DOMAINS_DIR = Path(__file__).parent.parent / "domains"


class ConfigError(Exception):
    """Raised when config.ini is missing or contains invalid values."""


@dataclass
class DatabaseConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: int

    def as_dict(self) -> dict[str, str | int]:
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    def connection_url(self, dialect: str = "postgresql+psycopg2") -> str:
        """SQLAlchemy connection URL. Change dialect for a different database backend."""
        return f"{dialect}://{self.user}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.dbname}"


@dataclass
class AppConfig:
    api_key: str
    database: DatabaseConfig
    domains_dir: Path


def load_config(path: Path = _CONFIG_PATH) -> AppConfig:
    """Load and validate configuration from config.ini."""
    cfg = configparser.ConfigParser()
    if path.exists():
        cfg.read(path)
    elif not os.environ.get("ANTHROPIC_API_KEY"):
        raise ConfigError(
            f"config.ini not found at {path}\n"
            "Either create config.ini or set the ANTHROPIC_API_KEY environment variable."
        )

    api_key = (
        os.environ.get("ANTHROPIC_API_KEY")
        or cfg.get("anthropic", "api_key", fallback="")
    )
    if not api_key or api_key.startswith("sk-ant-..."):
        raise ConfigError(
            "Anthropic API key not found.\n"
            "Set the ANTHROPIC_API_KEY environment variable, or add api_key to config.ini.\n"
            "Get your API key from: https://console.anthropic.com/"
        )

    if cfg.has_section("network"):
        for env_var, key in [("HTTP_PROXY", "http_proxy"), ("HTTPS_PROXY", "https_proxy")]:
            value = cfg.get("network", key, fallback="")
            if value:
                os.environ[env_var] = value

    raw_domains = cfg.get("paths", "domains_dir", fallback="").strip()
    if raw_domains:
        domains_dir = Path(raw_domains)
        if not domains_dir.is_absolute():
            domains_dir = path.parent / domains_dir
    else:
        domains_dir = _DEFAULT_DOMAINS_DIR

    return AppConfig(
        api_key=api_key,
        database=DatabaseConfig(
            dbname=cfg.get("database", "dbname"),
            user=cfg.get("database", "user"),
            password=cfg.get("database", "password"),
            host=cfg.get("database", "host"),
            port=cfg.getint("database", "port"),
        ),
        domains_dir=domains_dir,
    )
