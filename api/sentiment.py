"""Carga el modelo de sentimiento exportado por el job de Spark y predice sin JVM."""
from __future__ import annotations

import json
import os
import threading

import numpy as np

import textomodelo


class ModeloSentimiento:
    def __init__(self, carpeta: str = "/models", datos: dict | None = None):
        self.carpeta = carpeta
        self._m, self._meta, self._mtime = None, {}, None
        self._lock = threading.Lock()
        if datos is not None:            # para pruebas
            self._m, self._meta = datos, {"modelo": "prueba", "clases": datos["clases"]}

    def _cargar(self) -> None:
        npz = os.path.join(self.carpeta, "sentiment_export.npz")
        js = os.path.join(self.carpeta, "sentiment_export.json")
        if not (os.path.exists(npz) and os.path.exists(js)):
            return
        mtime = os.path.getmtime(npz)
        if self._m is not None and mtime == self._mtime:
            return
        with self._lock:
            with open(js, encoding="utf-8") as f:
                meta = json.load(f)
            arr = np.load(npz)
            self._m = {"num_features": meta["num_features"], "stopwords": set(meta["stopwords"]),
                       "idf": arr["idf"], "W": arr["W"], "b": arr["b"], "clases": meta["clases"]}
            self._meta, self._mtime = meta, mtime

    @property
    def cargado(self) -> bool:
        if self._meta.get("modelo") != "prueba":
            self._cargar()
        return self._m is not None

    def info(self) -> dict:
        return {k: self._meta.get(k) for k in ("modelo", "version", "f1_macro") if k in self._meta}

    def predecir(self, texto: str) -> dict:
        idx, prob = textomodelo.predecir(texto, self._m)
        clases = self._m["clases"]
        return {"sentimiento": clases[idx],
                "probabilidades": {c: round(float(p), 4) for c, p in zip(clases, prob)},
                "modelo": self._meta.get("modelo"), "version": self._meta.get("version")}
