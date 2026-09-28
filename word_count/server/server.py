import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Final, Literal, TypedDict

import redis
import rpyc
from rpyc.utils.server import ThreadedServer


DATA_DIR: Final[Path] = Path(os.getenv("DATA_DIR", "/app/data")).resolve()
REDIS_HOST: Final[str] = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: Final[int] = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: Final[int] = int(os.getenv("REDIS_DB", "0"))
CACHE_TTL: Final[int] = int(os.getenv("CACHE_TTL", "300"))
RPC_PORT: Final[int] = int(os.getenv("RPC_PORT", "18861"))


# compiled as mildly faster.
WHITESPACE_MATCHING_REGEX_PATTERN: Final[re.Pattern] = re.compile(r"\s")


class ReturnedWordCount(TypedDict):
    keyword: str
    filename: str
    count: int
    cache_hit: bool


class KeywordService:
    """Business-logic layer: count a word in one requested text file."""

    CHUNK_SIZE: Final[int] = 64 * 1024

    @staticmethod
    def count(path: Path, keyword: str) -> int:
        pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"
        compiled_pattern = re.compile(pattern, flags=re.IGNORECASE)
        carry_size = len(keyword) + 2
        carry = ""
        count = 0

        # chunked so large files have a fixed memory footprint, also marginally faster sometimes.
        try:
            with path.open("r", encoding="utf-8") as file:
                while chunk := file.read(KeywordService.CHUNK_SIZE):
                    buffer = carry + chunk
                    safe_end = (
                        len(buffer) - carry_size
                        if len(buffer) > carry_size * 2
                        else 0
                    )
                    for match in compiled_pattern.finditer(buffer):
                        if match.end() <= safe_end:
                            count += 1
                    carry = buffer[safe_end:]
                count += sum(1 for _ in compiled_pattern.finditer(carry))
        except UnicodeDecodeError as exc:
            raise ValueError(f"file is not valid UTF-8 text: {path.name}") from exc

        return count


class CacheService:
    """Caching layer backed by Redis."""

    def __init__(self, client: redis.Redis, ttl: int, executor: ThreadPoolExecutor) -> None:
        self.client: redis.Redis = client
        self.ttl: int = ttl
        self.executor: ThreadPoolExecutor = executor

    @staticmethod
    def key(keyword: str, filename: str) -> str:
        safe_filename = filename.replace("/", ":")
        return f"wordcount:{safe_filename}:{keyword}"

    def get(self, keyword: str, filename: str) -> int | None:
        key = self.key(keyword, filename)
        try:
            value = self.client.get(key)
        except redis.RedisError as exc:
            raise RuntimeError(f"Redis GET failed: {exc}") from exc
        return None if value is None else int(value)

    def set_async(self, keyword: str, filename: str, count: int) -> None:
        key = self.key(keyword, filename)
        self.executor.submit(self.client.setex, key, self.ttl, count)


class WordCountService(rpyc.Service):
    """RPyC entry point exposed to remote clients."""

    def __init__(
        self,
        *args: Any,
        data_dir: Path,
        keyword_service: KeywordService,
        cache_service: CacheService,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._data_dir: Path = data_dir
        self._keyword_service: KeywordService = keyword_service
        self._cache_service: CacheService = cache_service

    @classmethod
    def _normalize_keyword(cls, keyword: str) -> str:
        """ Normalise the keyword and raie relevant ValueError or TypeErrors """
        if not isinstance(keyword, str):
            raise TypeError("keyword must be a string")

        keyword = keyword.strip().lower()
        if len(keyword) == 0:
            raise ValueError("keyword cannot be empty")

        if re.search(WHITESPACE_MATCHING_REGEX_PATTERN, keyword):
            raise ValueError("keyword must contain exactly one word")

        return keyword

    def _normalize_file_path(self, filename: str) -> tuple[Path, str]:
        """Return a safe path relative to self._data_dir."""

        if not isinstance(filename, str):
            raise TypeError("filename must be a string")

        filename = filename.strip()
        if len(filename) == 0:
            raise ValueError("filename cannot be empty")

        requested = Path(filename)

        if requested.suffix.lower() != ".txt":
            raise ValueError("filename must refer to a .txt file")

        candidate = (self._data_dir / requested).resolve()
        try:
            candidate.relative_to(self._data_dir)
        except ValueError as exc:
            raise ValueError("filename must refer to a file inside the data directory") from exc

        if not candidate.is_file():
            raise FileNotFoundError(f"file not found: {filename}")

        return (candidate, candidate.relative_to(self._data_dir).as_posix())

    def on_connect(self, conn) -> None:
        try:
            self._cache_service.client.ping()
        except redis.RedisError as exc:
            raise RuntimeError(f"Redis is unavailable: {exc}") from exc

    def exposed_ping(self) -> Literal["pong"]:
        return "pong"

    def exposed_count_word(self, keyword: str, filename: str) -> ReturnedWordCount:
        keyword = self._normalize_keyword(keyword)
        normalized_file_path, normalized_file_name = self._normalize_file_path(filename)
        cached = self._cache_service.get(keyword, normalized_file_name)
        if cached is not None:
            return ReturnedWordCount(
                keyword=keyword,
                filename=normalized_file_name,
                count=cached,
                cache_hit=True,
            )

        count = self._keyword_service.count(normalized_file_path, keyword)

        self._cache_service.set_async(keyword, normalized_file_name, count)

        return ReturnedWordCount(
            keyword=keyword,
            filename=normalized_file_name,
            count=False,
            cache_hit=True,
        )


if __name__ == "__main__":
    print(f"Starting RPyC server on 0.0.0.0:{RPC_PORT}", flush=True)
    print(f"Reading text files from {DATA_DIR}", flush=True)
    print(f"Using Redis at {REDIS_HOST}:{REDIS_PORT}, TTL={CACHE_TTL}s", flush=True)

    server = ThreadedServer(
        WordCountService(
            data_dir=DATA_DIR,
            keyword_service=KeywordService(),
            cache_service=CacheService(
                ttl=CACHE_TTL,
                executor=ThreadPoolExecutor(max_workers=2),
                client=redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                ),
            ),
        ),
        port=RPC_PORT,
        protocol_config={"allow_public_attrs": True},
    )

    server.start()
