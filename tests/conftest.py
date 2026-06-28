"""
conftest.py — Configuración global de pytest.

En Windows, rutas con caracteres no-ASCII (é, ñ, etc.) corrompen el classpath
que construye spark-class2.cmd, produciendo "Could not find or load main class
org.apache.spark.deploy.SparkSubmit". Pre-configuramos SPARK_HOME con la ruta
corta 8.3 antes de que arranque cualquier SparkSession.
"""
from __future__ import annotations

import os
import subprocess


def _short_path(path: str) -> str:
    ps = f'(New-Object -ComObject Scripting.FileSystemObject).GetFolder("{path}").ShortPath'
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return path


if os.name == "nt" and "SPARK_HOME" not in os.environ:
    try:
        import pyspark as _pyspark
        _home = os.path.dirname(_pyspark.__file__)
        if any(ord(c) > 127 for c in _home):
            os.environ["SPARK_HOME"] = _short_path(_home)
    except Exception:
        pass
