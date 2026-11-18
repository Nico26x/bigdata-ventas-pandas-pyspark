# Informe técnico — procesamiento de ventas con PySpark

**Proyecto:** Big Data: comparación de pandas y PySpark  
**Componente:** Integrante 2 — implementación, optimización y validación de PySpark  
**Fecha de las pruebas:** 17 de noviembre de 2026  
**Alcance:** ejecución local en Windows, validación funcional y medición experimental

> **Uso de este archivo:** documento complementario para colocar en `spark/INFORME_TECNICO.md`. No reemplaza `spark/README.md` ni el informe comparativo final del grupo. Las rutas corresponden a la estructura utilizada durante las pruebas. Antes de publicar, comprobar que todos los archivos de evidencia mencionados estén efectivamente versionados en el repositorio.

## 1. Objetivo

Implementar un flujo de procesamiento de ventas sintéticas con PySpark, equivalente al desarrollado con pandas, para evaluar su comportamiento con distintos tamaños de datos. Se busca verificar el funcionamiento del procesamiento, documentar las optimizaciones aplicadas y medir tiempo, memoria RAM y CPU en un mismo equipo.

## 2. Entorno de prueba

| Componente | Configuración observada |
|---|---|
| Sistema operativo | Windows 11 |
| Memoria física | Aproximadamente 16 GB (15,77 GiB reportados en el equipo) |
| Núcleos disponibles para Spark local | 12, según Spark UI |
| PySpark | 4.3.0 (según versión instalada y Spark UI) |
| Java | Amazon Corretto 21.0.11 |
| Scala | 2.13.18, según Spark UI |
| Configuración de ejecución | `local[*]` |
| Memoria configurada del driver | `2g` |
| AQE | `spark.sql.adaptive.enabled = true` |
| Herramientas nativas para Windows | `winutils.exe` y librerías de la distribución comunitaria etiquetada como Hadoop 3.5.0 |
| Entorno Python de Spark | `.venv-spark`, separado del entorno usado para pandas |

**Aclaración sobre Hadoop:** la etiqueta 3.5.0 corresponde al paquete de utilidades nativas para Windows descargado de un repositorio comunitario; no se ha establecido aquí que sea la misma versión de todas las bibliotecas Hadoop que distribuye PySpark. Es una dependencia local de reproducibilidad, no un archivo que deba subirse al repositorio. Al ser binarios de terceros, debe verificarse su procedencia e integridad antes de utilizarlos.

## 3. Datos y operaciones implementadas

Se utilizaron transacciones sintéticas y un catálogo de 2.000 productos, generados con la herramienta del repositorio. Las transacciones contienen identificador, fecha, producto, región, cantidad, precio unitario, descuento y método de pago.

La secuencia general del programa `spark/procesamiento.py` es:

1. **Lectura de archivos:** carga `datos/transacciones.csv` y `datos/productos.csv` en DataFrames de Spark.
2. **Limpieza:** descarta registros inválidos conforme a las reglas del proyecto.
3. **Transformación:** prepara fechas, importes y atributos necesarios para el análisis.
4. **Unión:** relaciona transacciones con el catálogo de productos.
5. **Caché:** aplica `ventas.cache()` al DataFrame utilizado por las agrupaciones.
6. **Agregación:** produce ventas por categoría, región y mes.
7. **Exportación:** escribe tres archivos CSV; opcionalmente, detalle en Parquet particionado por año y mes.

Los archivos de salida consolidados son `ventas_categoria.csv`, `ventas_region.csv` y `ventas_mensuales.csv`, bajo `resultados/pyspark/`. La escritura Parquet opcional se realiza en `resultados/pyspark/ventas_detalle.parquet/`.

## 4. Optimizaciones y evidencia

### 4.1 Broadcast Join

El plan físico registrado mediante `--explain` muestra operadores `BroadcastHashJoin` y `BroadcastExchange`, asociados a la unión de transacciones con un catálogo comparativamente pequeño. Esto reduce la necesidad de redistribuir por la red o entre particiones el conjunto grande para efectuar esa unión.

**Evidencia:** `evidencias/spark/plan_spark.txt` y diagramas exportados de Spark UI (`evidencias/spark/dag/`, si están versionados).

### 4.2 Caché de DataFrame

Se aplica `ventas.cache()` antes de obtener las agrupaciones. En la pestaña **Storage** de Spark UI se observaron dos particiones almacenadas, **100 % en caché**, aproximadamente **2,8 MiB** de tamaño en memoria y **0 B** en disco para la ejecución ilustrativa de 100.000 transacciones.

El valor de 2,8 MiB **no representa el consumo total de RAM del programa**; es el tamaño informado por la interfaz para ese dato almacenado en caché.

### 4.3 Adaptive Query Execution (AQE)

En **Environment / Spark Properties** se verificó `spark.sql.adaptive.enabled = true`. El plan físico también incluye `AdaptiveSparkPlan` y operaciones relacionadas con lectura adaptativa de shuffle. Esto confirma que la funcionalidad estaba habilitada y presente en el plan de la ejecución inspeccionada, sin demostrar por sí solo una mejora cuantificada atribuible únicamente a AQE.

### 4.4 Escritura Parquet particionada

Con la opción `--parquet`, el sistema generó el directorio `ventas_detalle.parquet/`, organizado en carpetas `anio=2024/mes=...` y `anio=2025/mes=...`. Se verificó la presencia de archivos físicos con nombres terminados en `.snappy.parquet`.

La partición por año y mes puede beneficiar consultas filtradas por esos campos mediante eliminación de particiones (*partition pruning*); para afirmar una ganancia concreta de tiempo sería necesario medir consultas específicas.

**Evidencias:** capturas de directorios por año/mes y listado de archivos `part-*.snappy.parquet`.

## 5. Metodología experimental

Se probaron tres volúmenes de entrada: **500.000**, **5.000.000** y **10.000.000** transacciones, con **tres repeticiones por volumen**. El archivo fuente fue regenerado para cada tamaño mediante `generador/generar_datos.py`.

Las mediciones definitivas de Spark se realizaron con `benchmark/medir_rendimiento.py`, adaptado para la opción `--version spark`, invocando `spark/procesamiento.py --memoria 2g`.

El instrumento:

- toma el tiempo de pared del proceso completo (incluye inicio y cierre del proceso);
- muestrea RSS de memoria del proceso principal y sus descendientes y conserva el máximo observado;
- estima CPU a partir de variaciones de tiempo de CPU de esos procesos; por agregación de varios núcleos puede superar el 100 %;
- registra estado, código de salida, configuración general y un log;
- contiene control preventivo: no inicia si hay menos de 5 GiB disponibles y puede interrumpir si la memoria libre baja de 3 GiB.

**Limitaciones:** el muestreo puede no capturar picos muy breves; la RAM reportada es una observación de los procesos, no el uso total del sistema ni una medida precisa de memoria asignada a datos. Los procesos pueden dejar de aparecer al finalizar. La CPU es un máximo estimado, no un porcentaje normalizado frente a la capacidad total de doce núcleos. Aunque se emplea el mismo instrumento, las pruebas de pandas y Spark utilizan entornos Python separados y podrían verse afectadas por el estado del sistema, cachés y otros procesos.

Las nueve ejecuciones terminaron con estado `OK` y no alcanzaron los límites de seguridad.

## 6. Resultados medidos de PySpark

### 6.1 Registro de las nueve ejecuciones

| Transacciones | Repetición | Tiempo (s) | RAM máxima observada (MB) | CPU máxima estimada (%) | Estado |
|---:|---:|---:|---:|---:|---|
| 500.000 | 1 | 13,6553 | 1.221,62 | 1.346,08 | OK |
| 500.000 | 2 | 13,0479 | 1.098,11 | 1.188,49 | OK |
| 500.000 | 3 | 13,0955 | 1.176,75 | 1.213,94 | OK |
| 5.000.000 | 1 | 17,5329 | 1.631,12 | 1.587,90 | OK |
| 5.000.000 | 2 | 17,4622 | 1.699,94 | 1.513,77 | OK |
| 5.000.000 | 3 | 17,5274 | 1.795,92 | 1.600,51 | OK |
| 10.000.000 | 1 | 22,2268 | 1.566,54 | 1.601,10 | OK |
| 10.000.000 | 2 | 21,9484 | 1.928,88 | 1.548,69 | OK |
| 10.000.000 | 3 | 21,9557 | 1.905,14 | 1.585,83 | OK |

### 6.2 Promedios de PySpark

| Transacciones | Tiempo promedio (s) | RAM máxima observada promedio (MB) | CPU máxima estimada promedio (%) |
|---:|---:|---:|---:|
| 500.000 | 13,2662 | 1.165,49 | 1.249,50 |
| 5.000.000 | 17,5075 | 1.708,99 | 1.567,39 |
| 10.000.000 | 22,0436 | 1.800,19 | 1.578,54 |

Los promedios de RAM y CPU se calcularon a partir del **máximo observado en cada ejecución**; por tanto no equivalen al consumo medio durante toda la ejecución.

### 6.3 Referencia comparativa con pandas

Promedios previamente medidos con el benchmark del proyecto, redondeados:

| Transacciones | Pandas tradicional: tiempo (s) | Pandas chunks: tiempo (s) | PySpark: tiempo (s) |
|---:|---:|---:|---:|
| 500.000 | 1,46 | 1,73 | 13,27 |
| 5.000.000 | 4,20 | 6,10 | 17,51 |
| 10.000.000 | 7,27 | 10,26 | 22,04 |

| Transacciones | Pandas tradicional: RAM máxima promedio (MB) | Pandas chunks: RAM máxima promedio (MB) | PySpark: RAM máxima promedio (MB) |
|---:|---:|---:|---:|
| 500.000 | 219,64 | 121,43 | 1.165,49 |
| 5.000.000 | 1.486,01 | 130,01 | 1.708,99 |
| 10.000.000 | 2.943,69 | 134,20 | 1.800,19 |

En esta máquina y para estos tamaños, **pandas tradicional tuvo menor tiempo total**. A 10 millones de transacciones, PySpark mostró menor RAM máxima observada promedio que pandas tradicional, aunque mayor que pandas por chunks. Este resultado no permite concluir que Spark sea universalmente superior: se utilizó un único equipo, modo local y una carga de trabajo concreta. Pandas por chunks continuó ejecutándose correctamente y con RAM baja.

Se realizaron además pruebas exploratorias previas con pandas hasta 25 millones de filas. En esa escala, la versión tradicional se interrumpió por la salvaguarda de memoria del benchmark (`RAM_LIMIT`) y la versión chunks terminó. Eso **no equivale a un `MemoryError` de pandas** ni prueba que pandas no pueda procesar ese volumen en otras condiciones.

## 7. Validación de resultados

### 7.1 Conteos consistentes por volumen

| Transacciones de entrada | Transacciones válidas | Coincidencia entre las tres repeticiones de PySpark |
|---:|---:|---|
| 500.000 | 495.106 | Sí |
| 5.000.000 | 4.950.210 | Sí |
| 10.000.000 | 9.899.843 | Sí |

En la prueba de 10 millones, pandas registró **100.157 descartadas** y **9.899.843 válidas**, coincidiendo con PySpark.

### 7.2 Comparación de agregaciones

Se ejecutó `benchmark/comparar_pandas_spark.py` para el mismo dataset de 10 millones, después de generar la salida correspondiente de pandas y conservando la salida de PySpark. Resultado:

| Archivo comparado | Grupos comparados | Resultado |
|---|---:|---|
| `ventas_categoria.csv` | 5 | Iguales |
| `ventas_region.csv` | 8 | Iguales |
| `ventas_mensuales.csv` | 24 | Iguales |

El comparador informó: `TODAS LAS COMPARACIONES FUERON EXITOSAS`. La comprobación demuestra equivalencia de las agregaciones bajo las reglas del script comparador, no igualdad byte a byte de los archivos.

## 8. Incidentes y soluciones en Windows

**Error de escritura Parquet por Hadoop.** Inicialmente, la escritura Parquet falló al requerir utilidades nativas de Windows (`HADOOP_HOME and hadoop.home.dir are unset` y referencias a `winutils.exe`). Se dispuso una carpeta local `C:\hadoop\bin` con las herramientas necesarias y se configuraron `HADOOP_HOME` y `PATH`. Tras ello, `--parquet` terminó correctamente, con carpetas por año/mes y archivos `.snappy.parquet`.

**Versión de Java utilizada por la terminal.** Se fijó `JAVA_HOME` a Amazon Corretto 21 y se antepuso su carpeta `bin` a `PATH`, para que Spark utilizara Java 21 y no otra versión previamente presente en el sistema.

**Avisos de Java al capturar logs.** Un ejecutor preliminar de PowerShell interpretó un aviso de Java por stderr (`WARNING: Using incubator modules: jdk.incubator.vector`) como error debido a la gestión de errores del shell. Se cambió el mecanismo de ejecución para separar stdout, stderr y código de salida. Las ejecuciones posteriores terminaron con código 0.

**Codificación al leer logs.** Algunas salidas de PowerShell presentaron `Transacciones v�lidas`, por lo que una búsqueda exacta del texto acentuado no encontró el conteo. Se verificaron los valores buscando una porción independiente de la tilde (`Transacciones v`). No era evidencia de que los datos faltaran.

## 9. Reproducción de las pruebas

Ejecutar desde la raíz del repositorio en Windows PowerShell, tras crear y activar un entorno con las dependencias indicadas en el README del proyecto.

```powershell
# Ajustar JAVA_HOME a la instalación existente en el equipo
$env:JAVA_HOME = 'C:\Program Files\Amazon Corretto\jdk21.0.11_10'
$env:HADOOP_HOME = 'C:\hadoop'
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"

# Generar un conjunto de datos de ejemplo
.\.venv-spark\Scripts\python.exe generador\generar_datos.py --filas 500000

# Ejecutar PySpark normalmente
.\.venv-spark\Scripts\python.exe spark\procesamiento.py --memoria 2g

# Obtener plan físico y salida Parquet (ejecución funcional, no benchmark)
.\.venv-spark\Scripts\python.exe spark\procesamiento.py --memoria 2g --explain --parquet

# Benchmark formal: tres repeticiones, RAM y CPU
.\.venv-spark\Scripts\python.exe benchmark\medir_rendimiento.py --version spark --filas 500000 --repeticiones 3 --timeout 600
```

Para los otros volúmenes, regenerar el CSV con `--filas 5000000` o `--filas 10000000` y ejecutar el benchmark con la misma cifra. **No se deben ejecutar pandas y Spark sobre datasets diferentes cuando se comparan salidas.** El benchmark verifica la cantidad de filas antes de iniciar. La fuente de métricas definitivas es `evidencias/metricas/rendimiento.csv`; el ejecutor `ejecutar_pruebas_spark.ps1` corresponde a mediciones preliminares del tiempo externo y no reemplaza ese archivo.

**Nota sobre Spark UI:** para capturas se agregó temporalmente una pausa con `input()` antes de `spark.stop()`. Se retiró al medir el rendimiento, porque habría alterado el tiempo de ejecución.

## 10. Evidencias sugeridas para el informe final

| Evidencia | Ubicación en el proyecto | Aporta |
|---|---|---|
| Registro unificado de mediciones | `evidencias/metricas/rendimiento.csv` | Tiempo, RAM, CPU, estado, repetición y tamaño |
| Logs de la medición | `evidencias/logs/` | Salidas de ejecuciones, si están conservadas |
| Plan físico textual | `evidencias/spark/plan_spark.txt` | Broadcast Join y plan Spark |
| Diagramas exportados de Spark UI | `evidencias/spark/dag/` | Operadores físicos y etapas |
| Captura Jobs / Stages | `evidencias/spark/` | Trabajos y tareas completas |
| Captura Storage | `evidencias/spark/` | Caché materializada |
| Captura Environment | `evidencias/spark/` | Java, memoria de driver, AQE y modo local |
| Captura Executors | `evidencias/spark/` | Ejecutores, núcleos, tareas y shuffle |
| Evidencia Parquet | `evidencias/spark/` | Particiones `anio=.../mes=...` y archivos Snappy |
| Comparador funcional | `benchmark/comparar_pandas_spark.py` | Igualdad de las tres agregaciones |

La organización exacta debe coincidir con la estructura real del repositorio: no se debe citar una imagen o log que no se haya guardado efectivamente.

## 11. Conclusiones técnicas

1. PySpark procesó satisfactoriamente los tres volúmenes evaluados en Windows y terminó las nueve ejecuciones medidas sin fallos.
2. Para 10 millones de transacciones, tanto pandas como PySpark produjeron **9.899.843 registros válidos**, y el comparador confirmó igualdad de las tres agregaciones de salida.
3. Las optimizaciones **Broadcast Join, caché y AQE** pudieron identificarse en código, plan físico o Spark UI. La exportación Parquet particionada y comprimida con Snappy quedó verificada por los archivos generados.
4. **Pandas tradicional fue más rápido** en el equipo local para los tamaños medidos. A 10 millones, PySpark presentó menor RAM máxima observada promedio que pandas tradicional, pero no que pandas por chunks.
5. El experimento ilustra diferencias entre estrategias de procesamiento bajo límites concretos de hardware. No constituye una demostración de que pandas sea incapaz de procesar conjuntos grandes ni de que Spark resulte siempre más rápido.

---

**Pendiente únicamente para integración editorial:** seleccionar las capturas efectivamente disponibles, comprobar las rutas en el repositorio y usar estas tablas como insumo para las gráficas y el informe final del integrante 3.
