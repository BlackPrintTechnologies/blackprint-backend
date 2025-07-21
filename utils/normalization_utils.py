def normalize_market_id(market_id):
    if market_id is None or str(market_id).strip() == '-1':
        return None
    return str(market_id)

def normalize_fid(fid):
    if fid is None:
        return None
    try:
        f = float(fid)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return str(fid) 