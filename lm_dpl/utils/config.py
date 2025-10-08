"""
Centralized configuration management for LandMapper data pipeline.
Uses pydantic for type-safe environment variable validation and access.
"""

import os
from typing import Optional

from pydantic import field_validator, EmailStr, SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings."""

    DATADIR: str = Field(..., description="Data directory path")
    SENDER_EMAIL: EmailStr = Field(
        ..., description="Sender email address for notifications"
    )
    GMAIL_APP_PASSWORD: SecretStr = Field(
        ..., description="Gmail app password for notifications"
    )
    RECIPIENT_EMAIL: str = Field(
        ..., description="Comma-separated list of recipient email addresses"
    )

    LOG_PATH: Optional[str] = Field(
        None, description="Log file path (optional, auto-generated if not set)"
    )

    # PostgreSQL database configuration
    POSTGRES_HOST: str = Field(..., description="PostgreSQL host address")
    POSTGRES_PORT: int = Field(..., description="PostgreSQL port number")
    POSTGRES_USER: str = Field(..., description="PostgreSQL username")
    POSTGRES_PASSWORD: SecretStr = Field(..., description="PostgreSQL password")
    POSTGRES_DB: str = Field(..., description="PostgreSQL database name")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("RECIPIENT_EMAIL", mode="before")
    def parse_recipient_emails(cls, v):
        """Parse recipient emails from comma-separated string or list."""
        if isinstance(v, list):
            # Convert list back to comma-separated string
            return ",".join(v)
        return v

    @field_validator("POSTGRES_PORT", mode="before")
    def validate_postgres_port(cls, v):
        """Validate PostgreSQL port number."""
        if v is None:
            return 5432

        try:
            port = int(v)
        except (ValueError, TypeError):
            raise ValueError("PostgreSQL port must be a valid integer")

        if not (1 <= port <= 65535):
            raise ValueError("PostgreSQL port must be between 1 and 65535")
        return port

    @property
    def postgres_url(self) -> str:
        """Get PostgreSQL connection URL."""
        password = self.POSTGRES_PASSWORD.get_secret_value()
        return f"postgresql://{self.POSTGRES_USER}:{password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def postgres_url_async(self) -> str:
        """Get PostgreSQL connection URL for async connections."""
        password = self.POSTGRES_PASSWORD.get_secret_value()
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def postgres_dsn_dict(self) -> dict:
        """Get PostgreSQL connection parameters as a dictionary."""
        return {
            "host": self.POSTGRES_HOST,
            "port": self.POSTGRES_PORT,
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD.get_secret_value(),
            "database": self.POSTGRES_DB,
        }


# Singleton instance
_settings_instance: Optional[Settings] = None


def get_config() -> Settings:
    """
    Get the singleton configuration instance.

    Returns:
        Settings: Configured settings instance

    Raises:
        ValueError: If required environment variables are missing or invalid
    """
    global _settings_instance

    if _settings_instance is None:
        try:
            _settings_instance = Settings()
        except Exception as e:
            raise ValueError(f"Failed to load configuration: {e}")

    return _settings_instance


# Don't load during import to avoid issues
# Users should call get_config() explicitly when needed
settings = None
