# Versión PySpark

Implementa el mismo procesamiento que la versión pandas
(ver `ESPECIFICACIONES.md`) usando DataFrames de Spark.

## Instalación en Windows 10/11

1. **Python 3.11 o 3.12** (marcar "Add python.exe to PATH").
2. **JDK 17** (por ejemplo Eclipse Temurin 17). Spark 4 solo
   acepta Java 17 o 21; JDK 8 y 11 ya no funcionan.
3. **winutils** (necesario para que Spark escriba archivos en Windows):
   descargar `winutils.exe` y `hadoop.dll` de la versión de Hadoop 3
   y copiarlos en `C:\hadoop\bin`.
4. Variables de entorno (Panel de control → Variables de entorno):

   | Variable       | Valor                                              |
   |----------------|----------------------------------------------------|
   | `JAVA_HOME`    | `C:\Program Files\Eclipse Adoptium\jdk-17...`      |
   | `HADOOP_HOME`  | `C:\hadoop`                                        |
   | `SPARK_HOME`   | `<repo>\.venv\Lib\site-packages\pyspark`           |
   | `PYSPARK_PYTHON` | `python`                                         |
   | `Path`         | agregar `%JAVA_HOME%\bin` y `%HADOOP_HOME%\bin`    |

   El paquete `pyspark` de pip ya trae Spark completo, por eso
   `SPARK_HOME` apunta a la carpeta instalada.

5. Entorno e instalación de dependencias (en PowerShell, desde la raíz del repo):

   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   java -version   # debe mostrar 17
   ```

## Ejecución

```powershell
python generador/generar_datos.py --filas 1000000
python spark/procesamiento.py
```

Opciones:

- `--memoria 6g`: memoria del driver (por defecto `4g`).
- `--parquet`: guarda además el detalle limpio en Parquet
  particionado por `anio`/`mes`.
- `--explain`: imprime el plan físico (DAG) de la consulta.
- `--datos` / `--salida`: carpetas de entrada y salida.

Los resultados quedan en `resultados/pyspark/`:
`ventas_categoria.csv`, `ventas_region.csv` y `ventas_mensuales.csv`.

## Optimizaciones aplicadas

- **Esquema explícito**: evita una lectura extra del CSV para inferir tipos.
- **Selección de columnas**: se descarta `metodo_pago` antes de procesar.
- **Broadcast join**: `productos` (2.000 filas) se copia a cada tarea
  y las transacciones no se redistribuyen (sin shuffle).
- **Cache**: las tres agregaciones reutilizan el DataFrame ya limpio
  y unido; el CSV se lee una sola vez.
- **Adaptive Query Execution**: ajusta las particiones del shuffle.
- **Parquet particionado** (opcional): formato columnar comprimido.
- **Decimales exactos**: `venta_neta` se calcula como entero y se
  divide en `decimal(18,2)`, sin errores de punto flotante.
