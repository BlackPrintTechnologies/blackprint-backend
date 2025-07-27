import logging
import hashlib
from module.properties.controller import PropertyController, UserPropertyController
from utils.app_cache import get_from_cache, set_in_cache
from utils.normalization_utils import normalize_fid, normalize_market_id

logger = logging.getLogger(__name__)

def prefetch_fid_response(fid_to_prefetch, user):
    """
    Prefetches and caches the response for /property?fid=...
    """
    norm_fid_prefetch = normalize_fid(fid_to_prefetch)
    prefetch_cache_key_raw = f"user={user}|fid={norm_fid_prefetch}|lat=None|lng=None"
    prefetch_cache_key = hashlib.sha256(prefetch_cache_key_raw.encode()).hexdigest()
    
    if get_from_cache('property', prefetch_cache_key):
        logger.info(f"Prefetch: cache already exists for key: {prefetch_cache_key_raw}")
        return

    logger.info(f"Prefetch: fetching and caching /property?fid={norm_fid_prefetch} for user={user}")
    pc = PropertyController()
    prefetch_response = pc.get_properties(user, fid=norm_fid_prefetch, lat=None, lng=None)
    
    # Only cache successful responses
    if isinstance(prefetch_response, dict) and prefetch_response.get('message', '').lower() == 'success':
        set_in_cache('property', prefetch_cache_key, prefetch_response)
        logger.info(f"Prefetch: saved response for key: {prefetch_cache_key_raw}")
    elif isinstance(prefetch_response, tuple) and len(prefetch_response) == 2:
        response_data, status_code = prefetch_response
        if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
            set_in_cache('property', prefetch_cache_key, prefetch_response)
            logger.info(f"Prefetch: saved response for key: {prefetch_cache_key_raw}")
        else:
            logger.info(f"Prefetch: response not cached for key: {prefetch_cache_key_raw} - not a successful response")
    else:
        logger.info(f"Prefetch: response not cached for key: {prefetch_cache_key_raw} - not a successful response")

def prefetch_userproperty_response(fid_to_prefetch, user):
    """
    Prefetches and caches the response for /property/userproperty
    """
    norm_fid = normalize_fid(fid_to_prefetch)
    prop_status = None  # Prefetch with default prop_status
    cache_key = f"user={user}|fid={norm_fid}|prop_status={prop_status}"
    
    if get_from_cache('user_property', cache_key):
        logger.info(f"UserProperty prefetch: cache already exists for key: {cache_key}")
        return

    logger.info(f"UserProperty prefetch: fetching and caching for key: {cache_key}")
    upc = UserPropertyController()
    response = upc.get_user_properties(user, norm_fid, prop_status)
    
    # Only cache successful responses
    if isinstance(response, dict) and response.get('message', '').lower() == 'success':
        set_in_cache('user_property', cache_key, response)
        logger.info(f"UserProperty prefetch: saved response for key: {cache_key}")
    elif isinstance(response, tuple) and len(response) == 2:
        response_data, status_code = response
        if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
            set_in_cache('user_property', cache_key, response)
            logger.info(f"UserProperty prefetch: saved response for key: {cache_key}")
        else:
            logger.info(f"UserProperty prefetch: response not cached for key: {cache_key} - not a successful response")
    else:
        logger.info(f"UserProperty prefetch: response not cached for key: {cache_key} - not a successful response")

def prefetch_demographic_response(fid_to_prefetch, user):
    """
    Prefetches and caches the response for /property/demographic
    """
    norm_fid = normalize_fid(fid_to_prefetch)
    cache_key = f"user={user}|fid={norm_fid}"
    
    if get_from_cache('demographic', cache_key):
        logger.info(f"Demographic prefetch: cache already exists for key: {cache_key}")
        return
        
    logger.info(f"Demographic prefetch: fetching and caching for key: {cache_key}")
    pc = PropertyController()
    response = pc.get_property_demographic(norm_fid, user)
    
    # Only cache successful responses
    if isinstance(response, dict) and response.get('message', '').lower() == 'success':
        set_in_cache('demographic', cache_key, response)
        logger.info(f"Demographic prefetch: saved response for key: {cache_key}")
    elif isinstance(response, tuple) and len(response) == 2:
        response_data, status_code = response
        if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
            set_in_cache('demographic', cache_key, response)
            logger.info(f"Demographic prefetch: saved response for key: {cache_key}")
        else:
            logger.info(f"Demographic prefetch: response not cached for key: {cache_key} - not a successful response")
    else:
        logger.info(f"Demographic prefetch: response not cached for key: {cache_key} - not a successful response")

def prefetch_marketinfo_response(spot2_id, inmuebles24_id, propiedades_id, user):
    """
    Prefetches and caches the response for /property/marketinfo
    """
    spot2_id = normalize_market_id(spot2_id)
    inmuebles24_id = normalize_market_id(inmuebles24_id)
    propiedades_id = normalize_market_id(propiedades_id)

    cache_key_raw = f"user={user}|spot2_id={spot2_id}|inmuebles24_id={inmuebles24_id}|propiedades_id={propiedades_id}"
    cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
    
    if get_from_cache('market_info', cache_key):
        logger.info(f"Marketinfo prefetch: cache already exists for key: {cache_key_raw}")
        return

    logger.info(f"Marketinfo prefetch: fetching and caching for key: {cache_key_raw}")
    pc = PropertyController()
    response = pc.get_property_market_info(spot2_id, inmuebles24_id, propiedades_id)
    
    # Only cache successful responses
    if isinstance(response, dict) and response.get('message', '').lower() == 'success':
        set_in_cache('market_info', cache_key, response)
        logger.info(f"Marketinfo prefetch: saved response for key: {cache_key_raw}")
    elif isinstance(response, tuple) and len(response) == 2:
        response_data, status_code = response
        if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
            set_in_cache('market_info', cache_key, response)
            logger.info(f"Marketinfo prefetch: saved response for key: {cache_key_raw}")
        else:
            logger.info(f"Marketinfo prefetch: response not cached for key: {cache_key_raw} - not a successful response")
    else:
        logger.info(f"Marketinfo prefetch: response not cached for key: {cache_key_raw} - not a successful response") 