"""Exercise installed optional SDK contracts without browsers, models or TikTok."""

import inspect
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from test_video_research_providers import inputs


def test_installed_tiktok_session_and_comment_contracts():
    api = pytest.importorskip("TikTokApi")
    from TikTokApi.api.video import Video

    parameters = inspect.signature(api.TikTokApi.create_sessions).parameters
    assert {
        "num_sessions",
        "ms_tokens",
        "enable_session_recovery",
        "timeout",
        "browser",
    } <= parameters.keys()
    assert "count" in inspect.signature(Video.comments).parameters
    assert inspect.isasyncgenfunction(Video.comments)


def test_installed_whisper_constructor_never_resolves_remote_model(tmp_path, monkeypatch):
    whisper = pytest.importorskip("faster_whisper")
    from sm_trending_products.video_research import providers

    request, config = inputs(tmp_path, operation="transcribe")
    for field in ("local_files_only", "cpu_threads", "compute_type", "num_workers"):
        assert field in inspect.signature(whisper.WhisperModel).parameters
    # Bind the adapter to the installed SDK signature while replacing only the heavy engine.
    signature = inspect.signature(whisper.WhisperModel)
    calls = []

    def model(*args, **kwargs):
        calls.append(signature.bind(*args, **kwargs))
        raise RuntimeError("controlled stop before model loading")

    model_path = tmp_path / "large-v3"
    model_path.mkdir()
    (model_path / "model.bin").write_bytes(b"controlled")
    import hashlib

    config = config.model_copy(
        update={"model_dir": model_path, "model_sha256": hashlib.sha256(b"controlled").hexdigest()}
    )
    monkeypatch.setattr(whisper, "WhisperModel", model)
    with pytest.raises(RuntimeError, match="controlled stop"):
        providers.transcribe_media(
            {"media_path": str(model_path / "model.bin"), "media_sha256": config.model_sha256},
            config,
        )
    assert calls[0].arguments["local_files_only"] is True
    assert calls[0].arguments["model_size_or_path"] == str(model_path)
    assert request.operation == "transcribe"


def test_installed_ytdlp_downloads_one_selected_file_through_real_http(tmp_path, monkeypatch):
    yt_dlp = pytest.importorskip("yt_dlp")
    from sm_trending_products.video_research import providers

    content = b"controlled-mp4-container-fixture" * 100

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    calls = []

    class ControlledYDL(yt_dlp.YoutubeDL):
        def extract_info(self, url, download=True, **kwargs):
            calls.append(SimpleNamespace(url=url, download=download))
            return self.process_ie_result(
                {
                    "id": "123",
                    "title": "Controlled selected video",
                    "url": f"http://127.0.0.1:{server.server_port}/video.mp4",
                    "ext": "mp4",
                    "height": 360,
                    "duration": 2,
                    "extractor": "controlled",
                    "extractor_key": "Controlled",
                },
                download=download,
            )

    monkeypatch.setattr(yt_dlp, "YoutubeDL", ControlledYDL)
    request, config = inputs(tmp_path, operation="download")
    directory = tmp_path / "download"
    directory.mkdir(mode=0o700)
    try:
        output = providers.download_video(request, config, directory)
        assert output["video_id"] == "123"
        assert (directory / "video.mp4").read_bytes() == content
        assert len(calls) == 1 and calls[0].download is False
        with pytest.raises(providers.VideoResearchDenied, match="already_exists"):
            providers.download_video(request, config, directory)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
