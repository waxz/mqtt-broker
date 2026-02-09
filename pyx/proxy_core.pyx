# proxy_core.pyx
# cython: language_level=3

import asyncio
from cpython.bytes cimport PyBytes_GET_SIZE

# We declare types for the loop variables to speed up math
async def optimized_ws_to_tcp(object ws, object writer):
    cdef int pending_bytes = 0
    cdef int n = 0
    cdef int FLUSH_THRESHOLD = 131072 # 128KB
    # Increase from 128KB to 512KB for Localhost testing
    # Try Increasing the Flush Threshold for localhost. Since localhost is extremely fast, flushing too often (CPU bound) is worse than flushing less often (Memory bound).
    FLUSH_THRESHOLD = 524288  # 512KB
    cdef bytes data

    try:
        # async for is handled by Cython's generator support
        async for data in ws.iter_bytes():
            if data:
                writer.write(data)
                
                # C-Level Optimization: Get size instantly
                n = PyBytes_GET_SIZE(data)
                pending_bytes += n
                
                # C-Level Comparison (No Python Object overhead)
                if pending_bytes > FLUSH_THRESHOLD or n < 100:
                    await writer.drain()
                    pending_bytes = 0
        
        if pending_bytes > 0:
            await writer.drain()

    except Exception:
        pass

async def optimized_tcp_to_ws(object reader, object ws):
    cdef int BUFFER_SIZE = 65536
    cdef bytes data

    try:
        while True:
            # We cannot optimize the await call itself (it's I/O)
            # But we minimize the overhead AROUND it
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            await ws.send_bytes(data)
    except Exception:
        pass
