# adk-session-services

Session service implementations for [Google's Agent Development Kit (ADK)](https://github.com/google/adk-python). Provides persistent session storage backends as drop-in replacements for ADK's `BaseSessionService`.

## Features

- **Redis** — Persistent session storage via Redis with support for app-level and user-level state layering.
- **Firestore** — (Planned) Persistent session storage via Google Cloud Firestore.

## Installation

```bash
pip install adk-session-services
```

## Quick Start

### Redis

```python
from adk_session_services.redis_session import RedisSessionService

service = RedisSessionService("redis://localhost:6379")

session = await service.create_session(
    app_name="my-app",
    user_id="user-123",
)
```

## Redis Key Schema

All keys are prefixed with `adk:sessions:`:

| Key pattern | Type | Purpose |
|---|---|---|
| `{app}:{user}:{session}:meta` | Hash | Session metadata |
| `{app}:{user}:{session}:state` | String (JSON) | Session state dict |
| `{app}:{user}:{session}:events` | List (JSON) | Ordered event log |
| `{app}:{user}:sessions` | Set | Session IDs per user/app |
| `{app}:app_state` | Hash | App-level state (shared across users) |
| `{app}:{user}:user_state` | Hash | User-level state (shared across sessions) |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Install with test dependencies
pip install -e ".[test]"

# Run tests
pytest

# Format
black src/ && isort src/

# Type check
mypy src/
```

## License

MIT
