# Documentación técnica — Implementación con pandas

**Proyecto:** Análisis masivo de transacciones comerciales con pandas y PySpark  
**Responsabilidad:** Integrante 1 — generación de datos e implementación con pandas  
**Estado:** Implementación y pruebas de pandas documentadas; integración con PySpark pendiente.

## 1. Propósito y alcance

Este proyecto estudia el procesamiento de transacciones comerciales sintéticas de gran volumen, aplicando una versión de pandas que carga todos los datos y otra que procesa el archivo por bloques (`chunks`). El objetivo de esta parte es proporcionar dos implementaciones reproducibles, verificar que produzcan resultados equivalentes y medir sus compromisos de tiempo y memoria. La comparación definitiva con PySpark corresponde a la integración del equipo.

Los datos sintéticos **no corresponden a ventas reales**. Se utilizan para reproducir un escenario de millones de transacciones con estructura estable y reglas de calidad conocidas.

## 2. Archivos y responsabilidades

| Ruta | Función |
|---|---|
| `generador/generar_datos.py` | Produce `transacciones.csv` y `productos.csv` por bloques, con semilla fija. |
| `generador/validar_datos.py` | Verifica dimensiones, nulos, duplicados, transacciones inválidas y correspondencia con productos. |
| `pandas/procesamiento.py` | Implementación en memoria: lectura completa, limpieza, transformación, unión, agregaciones y exportación. |
| `pandas/procesamiento_chunks.py` | Implementación alternativa que procesa bloques y acumula agregados sin conservar todas las transacciones. |
| `pandas/comparar_resultados.py` | Compara los tres CSV de las dos implementaciones de pandas. |
| `benchmark/medir_rendimiento.py` | Ejecuta el procesamiento como proceso externo, registra tiempos y muestrea RAM/CPU de su árbol de procesos. |
| `ESPECIFICACIONES.md` | Contrato compartido de datos y cálculos con PySpark. |

## 3. Datos de entrada

### `datos/transacciones.csv`

| Columna | Significado |
|---|---|
| `id_transaccion` | Identificador de cada transacción. |
| `fecha` | Fecha de venta, `YYYY-MM-DD`. |
| `id_producto` | Clave de producto para unir con el catálogo. |
| `region` | Región de la transacción. |
| `cantidad` | Unidades vendidas. |
| `precio_unitario` | Precio unitario en pesos colombianos (COP). |
| `descuento_pct` | Porcentaje de descuento. |
| `metodo_pago` | Medio de pago. |

### `datos/productos.csv`

| Columna | Significado |
|---|---|
| `id_producto` | Identificador único del catálogo. |
| `nombre_producto` | Nombre sintético del producto. |
| `categoria` | Categoría comercial. |

El generador crea un catálogo de **2.000 productos** y produce transacciones en bloques de **100.000 filas**. Utiliza semilla 42 para transacciones y 43 para productos. Introduce aproximadamente un 1 % de filas inválidas (cantidad cero o precio negativo), que se conservan en el CSV crudo para evaluar la limpieza. La proporción exacta varía por tamaño.

**Nota de reproducibilidad:** generar un tamaño de datos distinto reinicia la secuencia pseudoaleatoria y reemplaza los CSV. No se debe asumir que un archivo menor es necesariamente el prefijo exacto de uno mayor, porque la generación consume números aleatorios por bloques y por columnas. Para comparar implementaciones con un tamaño dado, ejecutar ambas sobre **el mismo archivo** sin regenerarlo entre mediciones.

## 4. Reglas de procesamiento equivalentes

1. Leer las transacciones y el catálogo de productos.
2. Eliminar transacciones con nulos en columnas necesarias y fechas no interpretables.
3. Conservar transacciones con `cantidad > 0`, `precio_unitario > 0` y `0 <= descuento_pct <= 100`.
4. Convertir la fecha, extraer año y mes.
5. Calcular los valores monetarios mediante centavos enteros para evitar errores de redondeo binario en agregaciones.
6. Hacer `INNER JOIN` por `id_producto` con validación `many_to_one` en pandas.
7. Agrupar por categoría, por región y por `(anio, mes)`.
8. Exportar tres CSV, con montos en COP y dos decimales.

### Cálculo monetario

`venta_centavos = cantidad * precio_unitario * (100 - descuento_pct)`

La expresión entrega centavos de COP: si el precio unitario está en COP enteros, multiplicar por `(100 - descuento_pct)` produce el valor neto en centavos. La salida `total_ventas` se convierte a pesos dividiendo entre 100, sin perder precisión en la suma. En esta implementación los tipos enteros de 64 bits admiten los valores generados; para otros conjuntos de datos habría que validar desbordamientos.

**Ejemplo:** 3 unidades × 25.000 COP × (100 − 10) = 6.750.000 centavos = **67.500,00 COP**.

### Resultados

| Archivo | Columnas |
|---|---|
| `ventas_categoria.csv` | `categoria`, `cantidad_vendida`, `numero_transacciones`, `total_ventas` |
| `ventas_region.csv` | `region`, `numero_transacciones`, `total_ventas` |
| `ventas_mensuales.csv` | `anio`, `mes`, `numero_transacciones`, `total_ventas` |

## 5. Diseño de pandas tradicional

`pandas/procesamiento.py` usa `pd.read_csv` para cargar **todas** las transacciones a un DataFrame. Después aplica `dropna`, `pd.to_datetime`, filtros booleanos, transforma columnas, realiza `pd.merge` y ejecuta `groupby(...).agg(...)`. Finalmente exporta resultados.

**Ventaja:** código directo y, en las pruebas realizadas, menor tiempo de ejecución cuando el conjunto cabe cómodamente en RAM.

**Limitación:** varios DataFrames y operaciones intermedias coexisten durante la limpieza, la unión y las agregaciones. El uso máximo de memoria observado crece con el volumen. Una implementación que carga todas las filas es sensible a la memoria disponible del computador.

## 6. Diseño de pandas por bloques

`pandas/procesamiento_chunks.py` usa `pd.read_csv(..., chunksize=100_000, usecols=...)`, carga una sola vez el pequeño catálogo y ejecuta limpieza, transformación, `merge` y agrupación para cada bloque. Los resultados parciales se acumulan en diccionarios por categoría, región y mes. Al terminar, genera los tres CSV.

**Ventaja:** evita conservar las millones de transacciones y sus intermedios en un solo DataFrame. Reduce fuertemente la memoria observada para el esquema y agregaciones elegidos.

**Costo:** repetir las transformaciones y agrupaciones por bloque puede incrementar el tiempo; el resultado depende del tamaño del bloque y del tipo de operación.

**Alcance:** esta estrategia funciona especialmente bien cuando las operaciones se pueden expresar como agregados acumulables (sumas, conteos). Otros problemas, como ordenamientos globales o uniones entre dos tablas masivas, pueden requerir otra estrategia.

## 7. Validación de resultados

`pandas/comparar_resultados.py` ordena cada salida por su clave y usa `pd.testing.assert_frame_equal` para comprobar esquemas y valores. Las salidas de pandas tradicional y chunks coincidieron con los mismos datos para **10.000, 500.000, 5 millones, 10 millones y 15 millones de transacciones** durante las pruebas registradas. En **25 millones** no corresponde afirmar equivalencia, porque la versión tradicional no terminó.

**Precaución:** el comparador actual lee los CSV que existan en las carpetas y **no valida a qué ejecución pertenecen**. Si una versión falla, podrían quedar archivos antiguos. Solo se debe ejecutar después de que **ambas versiones finalicen con estado `OK`** para el mismo dataset, o implementar una validación adicional de procedencia.

## 8. Medición experimental

El benchmark ejecuta cada implementación como proceso separado. Registra tiempo de pared (`time.perf_counter()`), estado final, estimaciones de CPU y un máximo de RAM residente **observado por muestreo** del árbol de procesos. El muestreo no garantiza capturar el pico absoluto; los porcentajes de CPU de intervalos cortos pueden superar 100 % en sistemas multinúcleo y no deben compararse directamente con el uso global de CPU del equipo.

Se registraron tres repeticiones por versión para **500.000, 5 millones y 10 millones** de transacciones. Para **15 y 25 millones** se cuenta con exploraciones de una repetición por versión (a 25 millones, tradicional interrumpida). Ver `RESULTADOS_EXPERIMENTALES.md`.

El límite preventivo establecido detiene el procesamiento si la memoria disponible del sistema cae por debajo de **3 GiB**, y la comprobación de inicio evita comenzar cuando hay menos de **5 GiB** disponibles. Son **límites de seguridad definidos por el equipo**, no un `MemoryError` de pandas ni una prueba concluyente de que el procesamiento sea imposible.

## 9. Interpretación y limitaciones

- El procesamiento tradicional fue más rápido en promedio que chunks en los experimentos repetidos; consumió bastante más memoria.
- Chunks mantuvo un máximo observado cercano a 120–140 MB conforme creció el archivo, para este diseño de agregación.
- A 25 millones, chunks finalizó; el proceso tradicional fue interrumpido por una condición de seguridad del benchmark, cuando el sistema tenía poca memoria disponible. La cifra de tiempo registrada para esa ejecución **no** es tiempo de procesamiento completado.
- La evidencia demuestra una limitación operativa de la estrategia de carga completa bajo el estado de memoria del equipo, pero **no demuestra por sí sola la inviabilidad general de pandas**, especialmente porque chunks sí terminó.
- Aún **no hay resultados de PySpark** en esta documentación: cualquier conclusión comparativa sobre Spark queda pendiente del experimento del integrante 2.
- Los datos son sintéticos, la categoría de productos tiene cardinalidad baja y las agregaciones son acumulables. No deben extrapolarse estos resultados a toda clase de problema Big Data.

## 10. Información pendiente para cerrar el informe

- Marca/modelo de CPU, tipo de disco, sistema operativo y versiones definitivas de librerías para la ficha del entorno.
- Resultados y entorno de PySpark en el **mismo computador**, con los mismos archivos, condiciones y salidas equivalentes.
- Tabla de experimentos final con identificador del dataset, hashes, fecha, repeticiones y evidencia de RAM del sistema.
- Capturas seleccionadas del Administrador de tareas y logs relevantes.
- Referencias bibliográficas en APA o IEEE para el informe general.

