import logging
import os
from typing import Any, Dict, Optional

import httpx
import orjson

logger = logging.getLogger(__name__)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
MODEL_NAME = "gemma3:12b"

async def query_local_ai_json(prompt: str, system_message: str = "") -> Optional[Dict[str, Any]]:
    """
    Sends an async request to Ollama and attempts to parse the response as JSON.
    """
    url = f"{OLLAMA_HOST}/api/generate"

    full_prompt = prompt
    if system_message:
        full_prompt = f"System: {system_message}\nUser: {prompt}"

    payload = {
        "model": MODEL_NAME,
        "prompt": full_prompt,
        "stream": False,
        "format": "json", # Force JSON output (Ollama feature)
        "keep_alive": "60m",
        "options": {
            "temperature": 0.4, # Low temperature for consistent logic
            "num_ctx": 8192,     # Ensure context window is large enough
            "seed": 42
        }
    }

    async with httpx.AsyncClient(timeout=300.0) as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()

            result = response.json()
            ai_text = result.get("response", "")

            return orjson.loads(ai_text)
        except (httpx.RequestError, orjson.JSONDecodeError) as e:
            logger.error(f"AI Request Failed: {e}")
            return None
