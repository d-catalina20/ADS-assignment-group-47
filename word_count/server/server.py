import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data")).resolve()
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
RPC_PORT = int(os.getenv("RPC_PORT", "18861"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
)

cache_executor = ThreadPoolExecutor(max_workers=2)


def normalize_keyword(keyword):
    if not isinstance(keyword, str):
        raise TypeError("keyword must be a string")

    keyword = keyword.strip().lower()
    if not keyword:
        raise ValueError("keyword cannot be empty")

    if re.search(r"\s", keyword):
        raise ValueError("keyword must contain exactly one word")

    return keyword


def normalize_filename(filename):
    """Return a safe path relative to DATA_DIR."""
    if not isinstance(filename, str):
        raise TypeError("filename must be a string")

    filename = filename.strip()
    if not filename:
        raise ValueError("filename cannot be empty")

    requested = Path(filename)

    if requested.suffix.lower() != ".txt":
        raise ValueError("filename must refer to a .txt file")

    candidate = (DATA_DIR / requested).resolve()
    try:
        candidate.relative_to(DATA_DIR)
    except ValueError as exc:
        raise ValueError("filename must refer to a file inside the data directory") from exc

    if not candidate.is_file():
        raise FileNotFoundError(f"file not found: {filename}")

    return candidate, candidate.relative_to(DATA_DIR).as_posix()


class KeywordService:
    """Business-logic layer: count a word in one requested text file."""

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)

    def count(self, keyword, filename):
        path, _ = normalize_filename(filename)
        return self._count_in_file(path, keyword)

    @staticmethod
    def _count_in_file(path, keyword):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"file is not valid UTF-8 text: {path.name}") from exc

        pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"
        return len(re.findall(pattern, text, flags=re.IGNORECASE))


class CacheService:
    """Caching layer backed by Redis."""

    def __init__(self, client, ttl, executor):
        self.client = client
        self.ttl = ttl
        self.executor = executor

    @staticmethod
    def key(keyword, filename):
        safe_filename = filename.replace("/", ":")
        return f"wordcount:{safe_filename}:{keyword}"

    def get(self, keyword, filename):
        key = self.key(keyword, filename)
        try:
            value = self.client.get(key)
        except redis.RedisError as exc:
            raise RuntimeError(f"Redis GET failed: {exc}") from exc
        return None if value is None else int(value)

    def set_async(self, keyword, filename, count):
        key = self.key(keyword, filename)

        def store():
            try:
                self.client.setex(key, self.ttl, count)
            except redis.RedisError as exc:
                print(f"WARNING: Redis SET failed for {key}: {exc}", flush=True)

        self.executor.submit(store)


keyword_service = KeywordService(DATA_DIR)
cache_service = CacheService(redis_client, CACHE_TTL, cache_executor)


class WordCountService(rpyc.Service):
    """RPyC entry point exposed to remote clients."""

    def on_connect(self, conn):
        try:
            redis_client.ping()
        except redis.RedisError as exc:
            raise RuntimeError(f"Redis is unavailable: {exc}") from exc

    def exposed_ping(self):
        return "pong"

    def exposed_count_word(self, keyword, filename):
        keyword = normalize_keyword(keyword)
        _, normalized_filename = normalize_filename(filename)
        cached = cache_service.get(keyword, normalized_filename)
        if cached is not None:
            return {
                "keyword": keyword,
                "filename": normalized_filename,
                "count": cached,
                "cache_hit": True,
            }

        count = keyword_service.count(keyword, normalized_filename)

        cache_service.set_async(keyword, normalized_filename, count)

        return {
            "keyword": keyword,
            "filename": normalized_filename,
            "count": count,
            "cache_hit": False,
        }


if __name__ == "__main__":
    print(f"Starting RPyC server on 0.0.0.0:{RPC_PORT}", flush=True)
    print(f"Reading text files from {DATA_DIR}", flush=True)
    print(f"Using Redis at {REDIS_HOST}:{REDIS_PORT}, TTL={CACHE_TTL}s", flush=True)

    server = ThreadedServer(
        WordCountService,
        port=RPC_PORT,
        protocol_config={"allow_public_attrs": True},
    )
    server.start()
