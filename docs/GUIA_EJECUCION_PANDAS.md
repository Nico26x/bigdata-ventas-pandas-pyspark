# Guía de ejecución en Windows — Integrante 1 (pandas)

## Requisitos

- Windows 10/11.
- Python 3.11 (la ejecución documentada empleó Python 3.11.9).
- Dependencias: `pandas`, `numpy`, `psutil` y `pyarrow` según `requirements.txt` del repositorio.
- Espacio suficiente en disco para el CSV elegido y memoria RAM disponible para el escenario de prueba.

## 1. Entorno

Desde PowerShell en la raíz del repositorio:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python --version
```

En caso de bloqueo de ejecución de scripts, para la sesión actual:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Verificar dependencias:

```powershell
python -c "import pandas, numpy, psutil, pyarrow; print('Entorno listo')"
```

## 2. Generación de datos

**Atención:** este comando **reemplaza** `datos/transacciones.csv` y `datos/productos.csv`. No volver a generar el CSV entre pruebas que deban usar el mismo dataset.

```powershell
python generador/generar_datos.py --filas 10000
python generador/validar_datos.py
```

Siguientes tamaños usados: `500000`, `5000000`, `10000000`, `15000000`, `25000000`. Para pruebas iniciales comenzar por 10.000.

## 3. Ejecutar las dos versiones

```powershell
python pandas/procesamiento.py
python pandas/procesamiento_chunks.py --chunksize 100000
python pandas/comparar_resultados.py
```

Las salidas se escriben en `resultados/pandas/` y `resultados/pandas_chunks/`.

**No ejecutar la comparación si una versión se interrumpió:** pueden persistir resultados antiguos y producir diferencias engañosas (o coincidencias engañosas).

## 4. Benchmark

Ejecutar ambas versiones **por separado**, con el mismo CSV y en el mismo equipo. Para una medición exploratoria:

```powershell
python benchmark/medir_rendimiento.py --version tradicional --filas 500000 --repeticiones 1 --timeout 600
python benchmark/medir_rendimiento.py --version chunks --filas 500000 --chunksize 100000 --repeticiones 1 --timeout 600
```

Para las mediciones finales repetir tres veces cada configuración, una vez verificado que ambas terminan correctamente:

```powershell
python benchmark/medir_rendimiento.py --version tradicional --filas 500000 --repeticiones 3 --timeout 600
python benchmark/medir_rendimiento.py --version chunks --filas 500000 --chunksize 100000 --repeticiones 3 --timeout 600
```

El parámetro `--filas` debe coincidir con el archivo; el benchmark cuenta sus líneas antes de ejecutar. Esta comprobación queda **fuera del tiempo medido**.

Los resultados se guardan en `evidencias/metricas/rendimiento.csv` y los registros en `evidencias/logs/`. Dado que el CSV de métricas se **anexa** en cada ejecución, conservar el histórico, separar diagnósticos y seleccionar series comparables para el informe.

## 5. Seguridad de memoria y errores

El medidor incluye una política de protección de RAM: si hay menos de **5 GiB** disponibles al empezar, no inicia la prueba; si la memoria disponible cae bajo **3 GiB**, puede interrumpir la ejecución con estado `RAM_LIMIT`.

`RAM_LIMIT` es **una cancelación preventiva**, no un error nativo `MemoryError` de pandas. El tiempo de esa ejecución es tiempo hasta la interrupción y no debe compararse con el tiempo de procesos completados.

Para revisar memoria disponible:

```powershell
python -c "import psutil; m=psutil.virtual_memory(); print(f'RAM disponible: {m.available/1024**3:.2f} GiB')"
```

No reducir el umbral de seguridad únicamente para provocar fallos. Cerrar aplicaciones de forma normal y no finalizar procesos críticos del sistema.

## 6. Reproducibilidad y notas para el equipo

- Registrar versión de Python y paquetes: `python --version` y `python -m pip freeze`.
- Registrar procesador, RAM, tipo de disco y sistema operativo.
- Medir pandas y PySpark en el mismo computador y controlar los procesos abiertos; documentar cualquier diferencia.
- Conservar identificador del dataset y no regenerarlo entre comparaciones.
- El valor de RAM es el **máximo observado por muestreo** del árbol de procesos; no garantiza el pico físico absoluto.
- El cálculo de montos usa enteros en centavos; Spark deberá reproducir la misma semántica.
- Los datos sintéticos y los CSV masivos normalmente no se suben al repositorio; el generador permite recrearlos.

## 7. Incidencia conocida: 25 millones

Pandas por chunks completó el procesamiento de 25 millones. El tradicional fue detenido por `RAM_LIMIT`. Posteriormente el comparador mostró diferencias porque comparó CSV de distintas ejecuciones. Es un problema de **procedencia de archivos**, no evidencia de cálculos distintos. No usar esos CSV como una comparación válida.
