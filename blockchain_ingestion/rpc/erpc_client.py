import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any

import aiohttp

from blockchain_ingestion.execution.result_merger import RpcTaskMeta


@dataclass
class RpcErrorResult:
    error: Exception
    meta: RpcTaskMeta


class ErpcClient:
    """Thin eRPC client.

    Retry/circuit-breaker/failover/routing are delegated to eRPC gateway.
    """

    def __init__(self, base_url: str, timeout_sec: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    async def call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": method,
            "params": params,
        }
        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.base_url, json=payload, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json()

        if "error" in data:
            raise RuntimeError(f"eRPC returned error: {data['error']}")
        return data["result"]


class ErpcScheduler:
    def __init__(self, client: ErpcClient, max_inflight: int = 10):
        self.client = client
        self.sem = asyncio.Semaphore(max_inflight)

    async def submit(self, method: str, params: list[Any], meta_extra: dict[str, Any]):
        submit_ts = time.time()
        task_id = id(asyncio.current_task())
        meta = RpcTaskMeta(task_id=task_id, submit_ts=submit_ts, extra=meta_extra)

        async with self.sem:
            try:
                result = await self.client.call(method, params)
                return result, meta
            except Exception as exc:  # runtime error reported in unified form
                return RpcErrorResult(error=exc, meta=meta)
