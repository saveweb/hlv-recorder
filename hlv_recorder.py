"""
代码抄了 https://github.com/holstt/stream2podcast (No license)
"""

import asyncio
import logging
from pathlib import Path
import random
import time
from typing import AsyncIterator, Optional, Sequence, Union
from urllib.parse import urljoin
import aiohttp
import m3u8

logger = logging.getLogger(__name__)


# req_retry() 改自 https://github.com/HFrost0/bilix/ (Apache-2.0 license)
async def req_retry(session: aiohttp.ClientSession, url_or_urls: Union[str, Sequence[str]], method='GET',
                    allow_redirects=True, retry=5, **kwargs) -> aiohttp.ClientResponse:
    """Client request with multiple backup urls and retry"""
    pre_exc = None  # predefine to avoid warning
    for times in range(1 + retry):
        url = url_or_urls if type(url_or_urls) is str else random.choice(url_or_urls)
        try:
            logger.debug(f'{method} {url}')
            res = await session.request(method, url, allow_redirects=allow_redirects, **kwargs)
            res.raise_for_status()
        except aiohttp.ClientError as e:
            msg = f'{method} {e.__class__.__name__} url: {url}'
            logger.warning(msg) if times > 0 else logger.debug(msg)
            pre_exc = e
            await asyncio.sleep(.1 * (times + 1))
        except Exception as e:
            logger.warning(f'{method} {e.__class__.__name__} 未知异常 url: {url}')
            raise e
        else:
            return res
    logger.error(f"{method} 超过重复次数 {url_or_urls}")
    raise pre_exc


class AudioStreamException(Exception):
    pass

async def get_m3u8_playlist(session: aiohttp.ClientSession, playlist_url: str) -> m3u8.M3U8:
    try:
        logger.debug(f"Fetching playlist for: {playlist_url}")
        response = await req_retry(session, playlist_url)
        playlist = m3u8.loads(await response.text(), uri=playlist_url)
    except Exception as e:
        raise AudioStreamException(
            f"Unable to fetch playlist: {playlist_url}. Check your stream URL."
        ) from e
    return playlist


# Updates internal segment state and returns new segments
async def get_new_segments(
    session: aiohttp.ClientSession,
    playlist_url: str, old_segments: list[str]
) -> list[str]:
    try:
        # Fetch playlist
        playlist = await get_m3u8_playlist(session, playlist_url)
    except Exception as e:
        raise AudioStreamException(
            f"Unable to fetch stream: {playlist_url}. Check your stream URL."
        ) from e

    # Get all segments not in old
    new_segment_files: list[str] = [segment_file for segment_file in playlist.files if segment_file not in old_segments]  # type: ignore

    # Update state and return new segments
    return new_segment_files

class HttpStreamClient:
    chunk_size: int =  128 * 1024 # 128 KiB
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1788.0",
    }
    session: aiohttp.ClientSession = None

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    # Yields a chunk of bytes from a HTTP stream
    async def get_stream(
        self,
        url: str,
        stream_name: Optional[str] = None,  # Optional. Used for better logging output
    ) -> AsyncIterator[bytes]:
        try:
            logger.debug(f"{stream_name} Fetching stream for: {url}")
            async with self.session.get(url, headers=self.headers, timeout=20) as response:
                response.raise_for_status()
                async for chunk in response.content.iter_chunked(self.chunk_size):
                    yield chunk
        except Exception as e:
            raise AudioStreamException(
                f"Unable to fetch stream: {url}. Check your stream URL."
            ) from e

        

class CountdownTimer:
    _start_time: Optional[int] = None
    duration_total: int = 0
    def __init__(self, duration: int) -> None:
        """
        args:
            duration: Duration of the timer in seconds
        """
        super().__init__()
        self.duration_total = duration

    def start(self) -> None:
        if self._start_time is not None:
            raise RuntimeError("Timer has already been started")
        self._start_time = int(time.time())

    def is_expired(self) -> bool:
        if self._start_time is None:
            raise RuntimeError("Timer has not been started")
        return int(time.time()) - self._start_time >= self.duration_total

class AudioStreamAdapter:
    async def get_audio_data(
        self,
        url: str,
        countdown: CountdownTimer,
        stream_name: Optional[str] = None,
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError()

class HlsAudioStreamAdapter(AudioStreamAdapter):
    session: aiohttp.ClientSession = None
    stream_client: HttpStreamClient = None
    def __init__(
        self,
        session: aiohttp.ClientSession = session,
    ):
        self.session = session
        self.stream_client = HttpStreamClient(self.session)

    # Yields a chunk of bytes from a HLS stream
    async def get_audio_data(
        self,
        url: str,
        countdown: CountdownTimer,
        stream_name: Optional[str] = None,
    ) -> AsyncIterator[bytes]:
        WAIT_TIME_SEC = 10  # Time to wait until checking for new segments
        recorded_segments: list[str] = []

        countdown.start()

        # Get initial segments
        new_segments = await get_new_segments(self.session, url, recorded_segments)
        # Start recording from most recent segment
        recorded_segments = []

        while not countdown.is_expired():
            new_segments = await get_new_segments(self.session, url, recorded_segments)
            logger.info(f"{len(new_segments)} new segment(s) found")

            for segment in new_segments:
                async for chunk in self.stream_client.get_stream(
                    self._to_url(url, segment), stream_name
                ):
                    yield chunk

                # Immediately stop if countdown expires
                if countdown.is_expired():
                    logger.info("Countdown expired")
                    break

            # Update recorded segments
            recorded_segments.extend(new_segments)

            # Wait before fetching new segments
            logger.debug(
                f"{stream_name} Waiting {WAIT_TIME_SEC} seconds before fetching new segments"
            )
            await asyncio.sleep(WAIT_TIME_SEC)
    def _to_url(self, base_url: str, segment_file: str) -> str:
        return urljoin(base_url, segment_file)

class AudioStorageError(Exception):
    pass

class AudioStorageAdapter:  # XXX: FileRepo
    def __init__(self) -> None:
        super().__init__()
        # self.audio_format = audio_format

    async def save(  # XXX: Dto with binary data and domain object? .save(audio_file: AudioFile)
        self,
        audio_data_iterator: AsyncIterator[bytes],
        output_path: Path,
    ):
        # "wb" is write binary
        try:
            with open(output_path, "wb") as f:
                async for chunk in audio_data_iterator:
                    f.write(chunk)

            logger.info(f"Audio file saved: {output_path}")

        except Exception as e:
            raise AudioStorageError(
                f"An error occured while writing audio file: {output_path}"
            ) from e

class RecordAudioService:
    def __init__(
        self,
        audio_stream_adapter: AudioStreamAdapter,
        audio_storage_adapter: AudioStorageAdapter,
        stream_url: str,
    ):
        self.audio_stream_adapter = audio_stream_adapter
        self.audio_storage_adapter = audio_storage_adapter
        self.stream_url = stream_url
    async def start(self, output_path: Path, start_time: int, duration: int) -> None:
        # wait until start time
        logger.info(f"Waiting until start time: {start_time}")
        await asyncio.sleep(start_time - int(time.time()))
        logger.info("Start time reached")
        countdown = CountdownTimer(duration)

        await self.audio_storage_adapter.save(
            self.audio_stream_adapter.get_audio_data(
               self.stream_url, countdown, stream_name=output_path.name
            ),
            output_path
        )

async def main():
    m3u8_url = "https://ngcdn001.cnr.cn/live/zgzs/index.m3u8"
    output_path = Path("output.mp3")
    duration = 60 * 60  # 1 hour
    async with aiohttp.ClientSession() as session:
        recored_audio_service = RecordAudioService(
            HlsAudioStreamAdapter(session=session), AudioStorageAdapter(), m3u8_url
        )
        Cors = []
        for i in range(1):
            Cors.append(
                recored_audio_service.start(
                    output_path.with_name(f"output_{i}.mp3"), int(time.time())+10, duration
                )
            )
        await asyncio.gather(*Cors)

try:
    # set
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler(),
        ],
    )
    logger.info("Starting...")
    asyncio.run(main())
except KeyboardInterrupt:
    pass
