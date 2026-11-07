"""Geohash: codificación de (lat, lon) en celdas rectangulares y su geometría GeoJSON.

LifeMap usa precisión 5: celdas de unos 4,9 km x 4,9 km. Con precisión 6 (1,2 km x 0,6 km)
la mayoría de celdas queda con pocos negocios y no alcanza el mínimo de días-negocio.
"""
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_DECODE = {c: i for i, c in enumerate(_BASE32)}


def encode(lat: float, lon: float, precision: int = 5) -> str:
    lat_rng, lon_rng = [-90.0, 90.0], [-180.0, 180.0]
    out, bit, ch, even = [], 0, 0, True
    while len(out) < precision:
        rng, val = (lon_rng, lon) if even else (lat_rng, lat)
        mid = (rng[0] + rng[1]) / 2
        if val >= mid:
            ch |= 1 << (4 - bit)
            rng[0] = mid
        else:
            rng[1] = mid
        even = not even
        bit += 1
        if bit == 5:
            out.append(_BASE32[ch])
            bit, ch = 0, 0
    return "".join(out)


def bbox(gh: str) -> tuple[float, float, float, float]:
    """(lat_min, lat_max, lon_min, lon_max) de la celda."""
    lat_rng, lon_rng, even = [-90.0, 90.0], [-180.0, 180.0], True
    for c in gh:
        v = _DECODE[c]
        for i in range(4, -1, -1):
            rng = lon_rng if even else lat_rng
            mid = (rng[0] + rng[1]) / 2
            if (v >> i) & 1:
                rng[0] = mid
            else:
                rng[1] = mid
            even = not even
    return lat_rng[0], lat_rng[1], lon_rng[0], lon_rng[1]


def polygon(gh: str) -> dict:
    """Polígono GeoJSON de la celda (anillo cerrado, en orden lon, lat)."""
    s, n, w, e = bbox(gh)
    return {"type": "Polygon", "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]}


def center(gh: str) -> tuple[float, float]:
    s, n, w, e = bbox(gh)
    return (s + n) / 2, (w + e) / 2
