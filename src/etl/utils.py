def pct_to_score(pct: float, pivot: float = 0.0, scale: float = 0.02):
    """
    Map an excess return (e.g., 0.006 = +60bps/yr) into a 0-100 score.
    pivot=0 means market-like goes to ~50.
    """
    import math
    x = (pct - pivot) / scale
    # logistic squash to 0..100 around 50
    score = 100 / (1 + math.exp(-x))  # 0..100
    return max(0, min(100, score))
