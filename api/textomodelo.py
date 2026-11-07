"""Réplica en Python puro del preprocesamiento y de los modelos lineales de Spark MLlib.

La API (/predict) no necesita una JVM: aplica la misma limpieza, el mismo HashingTF
(MurmurHash3 x86_32 con semilla 42, como Spark 3) y los mismos pesos exportados del
modelo entrenado. El trabajo de Spark comprueba que ambas predicciones coinciden.
"""
from __future__ import annotations

import math
import re

RE_NO_LETRAS = re.compile(r"[^a-z ]")


def limpiar(texto: str) -> str:
    """Igual que el SQLTransformer del pipeline: minúsculas y todo lo que no es letra a espacio."""
    return RE_NO_LETRAS.sub(" ", (texto or "").lower())


def tokens(texto: str, stopwords: set[str], min_len: int = 2) -> list[str]:
    return [t for t in limpiar(texto).split() if len(t) >= min_len and t not in stopwords]


def _fmix(h: int) -> int:
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & 0xFFFFFFFF
    h ^= h >> 16
    return h


def murmur3_32(data: bytes, seed: int = 42) -> int:
    """MurmurHash3 x86_32 con signo (como Murmur3_x86_32.hashUnsafeBytes2 de Spark)."""
    c1, c2 = 0xCC9E2D51, 0x1B873593
    h = seed & 0xFFFFFFFF
    n = len(data)
    nblocks = n // 4
    for i in range(nblocks):
        k = int.from_bytes(data[4 * i:4 * i + 4], "little")
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
        h = ((h << 13) | (h >> 19)) & 0xFFFFFFFF
        h = (h * 5 + 0xE6546B64) & 0xFFFFFFFF
    tail = data[4 * nblocks:]
    k = 0
    if len(tail) >= 3:
        k ^= tail[2] << 16
    if len(tail) >= 2:
        k ^= tail[1] << 8
    if len(tail) >= 1:
        k ^= tail[0]
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
    h ^= n
    h = _fmix(h)
    return h - (1 << 32) if h & 0x80000000 else h


def indice(termino: str, num_features: int) -> int:
    """Índice de HashingTF: nonNegativeMod(murmur3(término), numFeatures)."""
    h = murmur3_32(termino.encode("utf-8"), 42)
    return h % num_features  # el % de Python ya es no negativo


def vector_tf(toks: list[str], num_features: int) -> dict[int, float]:
    v: dict[int, float] = {}
    for t in toks:
        j = indice(t, num_features)
        v[j] = v.get(j, 0.0) + 1.0
    return v


def predecir(texto: str, m: dict) -> tuple[int, list[float]]:
    """m: {'num_features', 'stopwords', 'idf', 'tipo', 'W' (clases x features), 'b' (clases)}."""
    tf = vector_tf(tokens(texto, m["stopwords"]), m["num_features"])
    idf, W, b = m["idf"], m["W"], m["b"]
    margenes = []
    for c in range(len(b)):
        fila = W[c]
        margenes.append(b[c] + sum(val * idf[j] * fila[j] for j, val in tf.items()))
    mx = max(margenes)
    ex = [math.exp(x - mx) for x in margenes]
    s = sum(ex)
    prob = [e / s for e in ex]
    return max(range(len(prob)), key=prob.__getitem__), prob
