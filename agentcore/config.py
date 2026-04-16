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
class ModelConfig:
    chat: str
    seed_data: str
    analyzer: str


@dataclass
class ChatConfig:
    max_tokens: int
    max_retries: int
    retry_delay: int
    max_iterations: int


@dataclass
class ArchitectConfig:
    max_tokens: int
    max_concurrency: int
    sdk_max_retries: int
    max_validation_attempts: int
    rows_per_table: int
    junction_rows: int


@dataclass
class AppConfig:
    api_key: str
    models: ModelConfig
    chat: ChatConfig
    architect: ArchitectConfig
    data_sources: dict[str, DatabaseConfig]          # From [data_source:*] sections
    domains_dir: Path
    database: DatabaseConfig | None = None            # Active connection (set at runtime)


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

    # Parse [models] section — chat, seed_data, analyzer are required.
    # architect is optional (only needed with build_schema.py --llm).
    if not cfg.has_section("models"):
        raise ConfigError(
            "Missing [models] section in config.ini.\n"
            "Add it with: chat, seed_data, analyzer."
        )
    _model_keys = ("chat", "seed_data", "analyzer")
    _missing = [k for k in _model_keys if not cfg.get("models", k, fallback="").strip()]
    if _missing:
        raise ConfigError(
            f"Missing model(s) in [models] section: {', '.join(_missing)}.\n"
            "Every model must be explicitly configured."
        )
    models = ModelConfig(
        chat=cfg.get("models", "chat"),
        seed_data=cfg.get("models", "seed_data"),
        analyzer=cfg.get("models", "analyzer"),
    )

    # Parse [chat] section — all keys required.
    if not cfg.has_section("chat"):
        raise ConfigError(
            "Missing [chat] section in config.ini.\n"
            "Add it with: max_tokens, max_retries, retry_delay, max_iterations."
        )
    _chat_keys = ("max_tokens", "max_retries", "retry_delay", "max_iterations")
    _missing = [k for k in _chat_keys if not cfg.get("chat", k, fallback="").strip()]
    if _missing:
        raise ConfigError(
            f"Missing key(s) in [chat] section: {', '.join(_missing)}.\n"
            "Every key must be explicitly configured."
        )
    chat_cfg = ChatConfig(
        max_tokens=cfg.getint("chat", "max_tokens"),
        max_retries=cfg.getint("chat", "max_retries"),
        retry_delay=cfg.getint("chat", "retry_delay"),
        max_iterations=cfg.getint("chat", "max_iterations"),
    )

    # Parse [architect] section — all keys required.
    if not cfg.has_section("architect"):
        raise ConfigError(
            "Missing [architect] section in config.ini.\n"
            "Add it with: max_tokens, max_concurrency, sdk_max_retries, "
            "max_validation_attempts, rows_per_table, junction_rows."
        )
    _arch_keys = (
        "max_tokens", "max_concurrency", "sdk_max_retries",
        "max_validation_attempts", "rows_per_table", "junction_rows",
    )
    _missing = [k for k in _arch_keys if not cfg.get("architect", k, fallback="").strip()]
    if _missing:
        raise ConfigError(
            f"Missing key(s) in [architect] section: {', '.join(_missing)}.\n"
            "Every key must be explicitly configured."
        )
    architect_cfg = ArchitectConfig(
        max_tokens=cfg.getint("architect", "max_tokens"),
        max_concurrency=cfg.getint("architect", "max_concurrency"),
        sdk_max_retries=cfg.getint("architect", "sdk_max_retries"),
        max_validation_attempts=cfg.getint("architect", "max_validation_attempts"),
        rows_per_table=cfg.getint("architect", "rows_per_table"),
        junction_rows=cfg.getint("architect", "junction_rows"),
    )

    # Parse [data_source:NAME] sections — each is fully self-contained.
    data_sources: dict[str, DatabaseConfig] = {}
    for section in cfg.sections():
        if section.startswith("data_source:"):
            name = section[len("data_source:"):]
            data_sources[name] = DatabaseConfig(
                dbname=cfg.get(section, "dbname"),
                user=cfg.get(section, "user"),
                password=cfg.get(section, "password"),
                host=cfg.get(section, "host"),
                port=cfg.getint(section, "port"),
            )

    return AppConfig(
        api_key=api_key,
        models=models,
        chat=chat_cfg,
        architect=architect_cfg,
        data_sources=data_sources,
        domains_dir=domains_dir,
    )
