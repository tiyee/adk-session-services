"""Redis session service implementation for Google ADK."""

import json
import logging
import time
import uuid
from typing import Any, Optional

import redis.asyncio as aioredis
from google.adk.events.event import Event
from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.adk.sessions.session import Session
from google.adk.sessions.state import State
from redis.asyncio.client import Redis as AsyncRedis

DEFAULT_PREFIX = 'adk'


def _meta_key(prefix: str, app: str, user: str, session: str) -> str:
    return f"{prefix}:sessions:{app}:{user}:{session}:meta"


def _state_key(prefix: str, app: str, user: str, session: str) -> str:
    return f"{prefix}:sessions:{app}:{user}:{session}:state"


def _events_key(prefix: str, app: str, user: str, session: str) -> str:
    return f"{prefix}:sessions:{app}:{user}:{session}:events"


def _user_set_key(prefix: str, app: str, user: str) -> str:
    return f"{prefix}:sessions:{app}:{user}:sessions"


def _app_state_key(prefix: str, app: str) -> str:
    return f"{prefix}:sessions:{app}:app_state"


def _user_state_key(prefix: str, app: str, user: str) -> str:
    return f"{prefix}:sessions:{app}:{user}:user_state"


class RedisSessionService(BaseSessionService):

    def __init__(self, redis_url: str, prefix: str = DEFAULT_PREFIX, **kwargs: Any):
        self.logger = logging.getLogger(__name__)
        self.client: AsyncRedis = aioredis.from_url(redis_url, **kwargs)
        self.prefix = prefix

    async def create_session(
            self,
            *,
            app_name: str,
            user_id: str,
            state: Optional[dict[str, Any]] = None,
            session_id: Optional[str] = None,
    ) -> Session:
        sid = session_id or uuid.uuid4().hex
        now = time.time()
        await self.client.hset(
            _meta_key(self.prefix, app_name, user_id, sid),
            mapping={"id": sid, "last_update_time": now},
        )
        await self.client.set(
            _state_key(self.prefix, app_name, user_id, sid), json.dumps(state or {})
        )
        await self.client.delete(_events_key(self.prefix, app_name, user_id, sid))
        await self.client.sadd(_user_set_key(self.prefix, app_name, user_id), sid)

        # Create a session and merge state
        session = Session(
            id=sid,
            app_name=app_name,
            user_id=user_id,
            state=state or {},
            events=[],
            last_update_time=now,
        )
        return await self._merge_state(app_name, user_id, session)

    async def get_session(
            self,
            *,
            app_name: str,
            user_id: str,
            session_id: str,
            config: Optional[GetSessionConfig] = None,
    ) -> Optional[Session]:
        key = _meta_key(self.prefix, app_name, user_id, session_id)
        if not await self.client.exists(key):
            return None
        meta = await self.client.hgetall(key)
        last = float(meta.get(b"last_update_time", b"0"))
        state = json.loads(
            (await self.client.get(_state_key(self.prefix, app_name, user_id, session_id))) or b"{}"
        )
        raw = await self.client.lrange(
            _events_key(self.prefix, app_name, user_id, session_id), 0, -1
        )
        events = []
        for e in raw:
            try:
                events.append(Event.model_validate_json(e.decode()))
            except Exception as e:
                self.logger.warning("Failed to parse event, skipping: %s", e)

        # Apply config filters correctly
        if config:
            if config.after_timestamp is not None:
                # Use >= instead of > to match the expected behavior in tests
                events = [e for e in events if e.timestamp >= config.after_timestamp]
            if config.num_recent_events is not None:
                events = events[-config.num_recent_events:]

        session = Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=state,
            events=events,
            last_update_time=last,
        )
        return await self._merge_state(app_name, user_id, session)

    async def list_sessions(
            self, *, app_name: str, user_id: Optional[str] = None
    ) -> ListSessionsResponse:
        ids = await self.client.smembers(_user_set_key(self.prefix, app_name, user_id))
        sessions: list[Session] = []
        # Sort the session IDs to ensure consistent ordering for tests
        sorted_ids = sorted([sid.decode() for sid in ids])
        for sid_str in sorted_ids:
            last_b = (
                    await self.client.hget(
                        _meta_key(self.prefix, app_name, user_id, sid_str), "last_update_time"
                    )
                    or b"0"
            )
            last = float(last_b)
            sessions.append(
                Session(
                    id=sid_str,
                    app_name=app_name,
                    user_id=user_id if user_id else uuid.uuid4().hex,
                    state={},
                    events=[],
                    last_update_time=last,
                )
            )
        return ListSessionsResponse(sessions=sessions)

    async def delete_session(
            self, *, app_name: str, user_id: str, session_id: str
    ) -> None:
        keys = [
            _meta_key(self.prefix, app_name, user_id, session_id),
            _state_key(self.prefix, app_name, user_id, session_id),
            _events_key(self.prefix, app_name, user_id, session_id),
        ]
        async with self.client.pipeline(transaction=True) as pipe:
            await pipe.delete(*keys)
            await pipe.srem(_user_set_key(self.prefix, app_name, user_id), session_id)
            await pipe.execute()

    async def append_event(self, session: Session, event: Event) -> Event:
        if event.partial:
            return event
        mkey = _meta_key(self.prefix, session.app_name, session.user_id, session.id)
        stored = await self.client.hget(mkey, "last_update_time") or b"0"
        if float(stored) > session.last_update_time:
            raise ValueError("stale session")

        # Process the event using the parent class implementation
        new_event = await super().append_event(session=session, event=event)

        # Update user and app state if there's a state delta
        if event.actions and event.actions.state_delta:
            for key, value in event.actions.state_delta.items():
                if key.startswith(State.APP_PREFIX):
                    app_key = key.removeprefix(State.APP_PREFIX)
                    await self.client.hset(
                        _app_state_key(self.prefix, session.app_name), app_key, json.dumps(value)
                    )
                elif key.startswith(State.USER_PREFIX):
                    user_key = key.removeprefix(State.USER_PREFIX)
                    await self.client.hset(
                        _user_state_key(self.prefix, session.app_name, session.user_id),
                        user_key,
                        json.dumps(value),
                    )

        # Save the event and update session state
        await self.client.rpush(
            _events_key(self.prefix, session.app_name, session.user_id, session.id),
            new_event.model_dump_json(),
        )
        await self.client.set(
            _state_key(self.prefix, session.app_name, session.user_id, session.id),
            json.dumps(session.state),
        )
        await self.client.hset(mkey, "last_update_time", session.last_update_time)

        return new_event

    async def _merge_state(
            self, app_name: str, user_id: str, session: Session
    ) -> Session:
        """Merge app and user state into the session state."""
        # Merge app state
        app_state = await self.client.hgetall(_app_state_key(self.prefix, app_name))
        for key, value_json in app_state.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            value = json.loads(
                value_json.decode() if isinstance(value_json, bytes) else value_json
            )
            session.state[State.APP_PREFIX + key_str] = value

        # Merge user state
        user_state = await self.client.hgetall(_user_state_key(self.prefix, app_name, user_id))
        for key, value_json in user_state.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            value = json.loads(
                value_json.decode() if isinstance(value_json, bytes) else value_json
            )
            session.state[State.USER_PREFIX + key_str] = value

        return session
