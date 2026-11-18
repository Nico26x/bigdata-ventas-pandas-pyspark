# Resultados experimentales — Pandas

**Fuente:** capturas de terminal y resultados comunicados durante las pruebas del 17 de noviembre de 2026.  
**Equipo:** Windows, 16 GB de RAM nominal (15,77 GiB informados por `psutil`), unidad C: con 138,24 GB libres en una comprobación anterior. Procesador específico pendiente de registro.  
**Metodología:** muestreo de RSS del árbol de procesos y tiempo de pared del benchmark. Los máximos de RAM no son picos absolutos garantizados.

## 1. Experimentos repetidos (tres ejecuciones por versión)

| Filas | Versión | Tiempo rep. 1 (s) | Rep. 2 | Rep. 3 | Tiempo medio (s) | RAM máxima observada media (MB) | Estado |
|---:|---|---:|---:|---:|---:|---:|---|
| 500.000 | Tradicional | 1,4964 | 1,6726 | 1,2000 | 1,4563 | 219,64 | 3/3 OK |
| 500.000 | Chunks | 1,7636 | 1,8447 | 1,5717 | 1,7267 | 121,43 | 3/3 OK |
| 5.000.000 | Tradicional | 4,6467 | 4,0980 | 3,8540 | 4,1996 | 1.486,01 | 3/3 OK |
| 5.000.000 | Chunks | 5,7535 | 5,6768 | 6,8824 | 6,1042 | 130,01 | 3/3 OK |
| 10.000.000 | Tradicional | 7,2678 | 7,5811 | 6,9646 | 7,2712 | 2.943,69 | 3/3 OK |
| 10.000.000 | Chunks | 10,1484 | 9,9922 | 10,6405 | 10,2604 | 134,20 | 3/3 OK |

Para las tablas de RAM se emplea el promedio de **tres máximos observados**, uno por repetición; no una medida continua de consumo medio.

### Detalle de RAM en las tres repeticiones

| Filas | Tradicional — RAM por repetición (MB) | Chunks — RAM por repetición (MB) |
|---:|---|---|
| 500.000 | 208,89 / 222,13 / 227,91 | 121,52 / 119,85 / 122,92 |
| 5.000.000 | 1.533,86 / 1.429,27 / 1.494,90 | 130,50 / 129,81 / 129,73 |
| 10.000.000 | 2.976,31 / 2.928,07 / 2.926,70 | 132,64 / 134,14 / 135,82 |

## 2. Experimentos exploratorios (una ejecución por versión)

| Filas | Versión | Tiempo (s) | RAM máxima observada (MB) | Estado |
|---:|---|---:|---:|---|
| 15.000.000 | Tradicional | 10,8850 | 4.421,54 | OK |
| 15.000.000 | Chunks | 14,6892 | 135,83 | OK |
| 25.000.000 | Chunks | 23,9443 | 140,11 | OK |
| 25.000.000 | Tradicional | **1,0896 hasta interrupción** | **1.046,34 hasta interrupción** | **RAM_LIMIT** |

El archivo de 15 millones ocupó aproximadamente **0,81 GiB**; el de 25 millones, aproximadamente **1,36 GiB**, según el tamaño informado en terminal. El tamaño en disco no equivale al espacio usado por pandas al convertir los datos a estructuras en memoria.

## 3. Validaciones funcionales

- **10.000 filas:** 122 inválidas, 9.878 válidas; comparación de categoría (5 grupos), región (8) y mes (24) exitosa.
- **500.000 filas:** 4.894 inválidas, 495.106 válidas; comparación de las tres salidas exitosa.
- **5 millones y 10 millones:** ambas versiones finalizaron y los tres resultados coincidieron.
- **15 millones:** ambas versiones finalizaron y los tres resultados coincidieron.
- **25 millones:** chunks terminó; tradicional **no terminó** por la política de RAM, así que **no procede comparar resultados**. El error mostrado por el comparador provino de contrastar resultados recientes de chunks con CSV antiguos de tradicional.

## 4. Interpretación

- A medida que crece el número de filas, aumenta la RAM máxima observada para el procesamiento tradicional. En cambio, chunks conserva un consumo comparativamente estable debido a que acumula agregados parciales y no mantiene todo el archivo en memoria.
- Para 10 millones, la RAM máxima media observada fue **2.943,69 MB** (tradicional) frente a **134,20 MB** (chunks), una reducción aproximada de **95,4 %**.
- Para 10 millones, el tiempo medio fue **7,2712 s** frente a **10,2604 s**. Chunks tardó aproximadamente **41,1 %** más, con el beneficio de una menor huella de memoria.
- La reducción de memoria no implica que chunks sea más rápido: tiempo, RAM y viabilidad deben analizarse por separado.
- En 25 millones, el sistema interrumpió la versión tradicional al quedar menos de **3 GiB de memoria disponible**, de acuerdo con una política preventiva del monitor. No consta un `MemoryError` ni un cierre involuntario de pandas.
- Posteriormente el usuario verificó **15,77 GiB totales y 4,29 GiB disponibles**, que aumentaron hasta **5,07 GiB** al cerrar aplicaciones. Estas cifras ilustran por qué el estado del equipo influye en la prueba.

## 5. Límites de validez

1. El máximo de RAM es **observado por muestreo**, no una medición continua ni el pico absoluto garantizado.
2. Las cifras de CPU son estimaciones en intervalos cortos, muy sensibles al muestreo; deben tratarse como exploratorias.
3. Los resultados con 15 y 25 millones son exploratorios: no tienen tres repeticiones exitosas por configuración.
4. El benchmark no verifica automáticamente que los CSV de resultados pertenezcan a la ejecución que acaba de terminar.
5. No hay mediciones de PySpark en esta tabla. **No es válido afirmar que Spark supera a pandas** sin ejecutar la comparación equivalente.
6. La medición depende de la carga y memoria disponible del sistema. La comparación definitiva debe controlar esas condiciones.

## 6. Evidencias para el informe

Conservar los archivos locales:

- `evidencias/metricas/rendimiento.csv` (contiene varias tandas; seleccionar las ejecuciones indicadas, evitando mezclarlas con diagnósticos previos).
- `evidencias/logs/` y especialmente el log con estado `RAM_LIMIT`.
- Capturas de las terminales de las tres repeticiones, comparación exacta de salidas y disponibilidad de RAM.

**Nota sobre Git:** las evidencias y CSV generados pueden estar excluidos por `.gitignore`. Deben entregarse al responsable del informe por separado o incorporarse como archivos ligeros seleccionados y anonimizados.

## 7. Pendientes del experimento del equipo

Comparar PySpark con pandas sobre datos, operaciones y entorno equivalentes; validar los resultados, interpretar costes adicionales de Spark para volúmenes pequeños y decidir con los datos qué limitaciones se han demostrado realmente.
