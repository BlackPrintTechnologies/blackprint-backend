"""
Pre-initialize thread pool to avoid first-request delay
This module should be imported early in the application startup
"""
import logging
from utils.async_utils import get_thread_pool

logger = logging.getLogger(__name__)

def pre_init_thread_pool():
    """Pre-initialize the thread pool during module import"""
    try:
        pool = get_thread_pool()
        logger.info("⚡ Thread pool pre-initialized during import")
        return True
    except Exception as e:
        logger.error(f"❌ Thread pool pre-initialization failed: {e}")
        return False

# Auto-initialize when module is imported
if pre_init_thread_pool():
    logger.debug("✅ Thread pool ready for immediate use") 