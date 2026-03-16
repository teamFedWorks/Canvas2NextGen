import time
import functools
import random
import logging

logger = logging.getLogger(__name__)

def retry(max_attempts=3, base_delay=1, max_delay=10, exceptions=(Exception,)):
    """
    Decorator for exponential backoff with jitter.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        logger.error(f"Function {func.__name__} failed after {attempts} attempts. Error: {str(e)}")
                        raise
                    
                    # Exponential backoff: base_delay * 2^attempts
                    delay = min(max_delay, base_delay * (2 ** attempts))
                    # Add jitter
                    jitter = delay * 0.1 * random.uniform(-1, 1)
                    sleep_time = delay + jitter
                    
                    logger.warning(f"Attempt {attempts} failed for {func.__name__}. Retrying in {sleep_time:.2f}s... Error: {str(e)}")
                    time.sleep(sleep_time)
            return None
        return wrapper
    return decorator
