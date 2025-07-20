import asyncio
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

# Global thread pool for async operations
_thread_pool = None
_pool_lock = threading.Lock()

def get_thread_pool():
    """Get or create a thread pool for async operations"""
    global _thread_pool
    if _thread_pool is None:
        with _pool_lock:
            if _thread_pool is None:
                _thread_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="async_api")
                logger.info("Created thread pool for async API operations")
    return _thread_pool

def async_route(func):
    """
    Decorator to make Flask-RESTful methods async
    Keeps the same function name but runs synchronous operations in thread pool
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Get or create event loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # If loop is already running, we need to use thread pool
            if loop.is_running():
                executor = get_thread_pool()
                future = executor.submit(asyncio.run, func(*args, **kwargs))
                return future.result()
            else:
                # Run async function in current loop
                return loop.run_until_complete(func(*args, **kwargs))
                
        except Exception as e:
            logger.error(f"Error in async route {func.__name__}: {e}")
            raise
    
    return wrapper

async def run_sync_in_executor(sync_func, *args, **kwargs):
    """
    Run a synchronous function in thread pool executor
    """
    loop = asyncio.get_event_loop()
    executor = get_thread_pool()
    
    # Create a wrapper for the sync function with its arguments
    def sync_wrapper():
        return sync_func(*args, **kwargs)
    
    return await loop.run_in_executor(executor, sync_wrapper)

async def run_controller_method(controller, method_name, *args, **kwargs):
    """
    Run a controller method asynchronously
    """
    method = getattr(controller, method_name)
    return await run_sync_in_executor(method, *args, **kwargs)

def setup_async_environment():
    """Setup the async environment for the Flask app"""
    # Ensure we have an event loop policy that works with threads
    if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    logger.info("Async environment setup complete") 