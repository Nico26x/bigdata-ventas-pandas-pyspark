"""Procesamiento de transacciones con PySpark.

Implementa el contrato de ESPECIFICACIONES.md: lectura, limpieza,
transformación, join con productos, agregaciones y escritura.

Uso:
    python spark/procesamiento.py
    python spark/procesamiento.py --memoria 6g --explain
    python spark/procesamiento.py --parquet
"""

import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BASE = Path(__file__).resolve().parent.parent

# Esquema explícito: evita que Spark lea el CSV completo
# una vez extra solo para inferir los tipos (inferSchema).
ESQUEMA_TRANSACCIONES = StructType([
    StructField("id_transaccion", LongType()),
    StructField("fecha", DateType()),
    StructField("id_producto", IntegerType()),
    StructField("region", StringType()),
    StructField("cantidad", IntegerType()),
    StructField("precio_unitario", LongType()),
    StructField("descuento_pct", IntegerType()),
    StructField("metodo_pago", StringType()),
])

ESQUEMA_PRODUCTOS = StructType([
    StructField("id_producto", IntegerType()),
    StructField("nombre_producto", StringType()),
    StructField("categoria", StringType()),
])

COLUMNAS_NECESARIAS = [
    "fecha",
    "id_producto",
    "region",
    "cantidad",
    "precio_unitario",
    "descuento_pct",
]


def crear_sesion(memoria):
    spark = (
        SparkSession.builder
        .appName("ventas-pyspark")
        .master("local[*]")
        .config("spark.driver.memory", memoria)
        # AQE ajusta en tiempo de ejecución el número de
        # particiones del shuffle según el tamaño real.
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def cargar(spark, carpeta):
    transacciones = spark.read.csv(
        str(carpeta / "transacciones.csv"),
        header=True,
        schema=ESQUEMA_TRANSACCIONES,
        dateFormat="yyyy-MM-dd",
    )
    productos = spark.read.csv(
        str(carpeta / "productos.csv"),
        header=True,
        schema=ESQUEMA_PRODUCTOS,
    )
    return transacciones, productos


def limpiar(transacciones):
    # La fecha ya llega como DateType por el esquema; una fecha
    # mal formada queda en null y se elimina con el dropna.
    return (
        transacciones
        .select(*COLUMNAS_NECESARIAS)  # metodo_pago no se usa
        .dropna(subset=COLUMNAS_NECESARIAS)
        .filter((F.col("cantidad") > 0) & (F.col("precio_unitario") > 0))
    )


def transformar(transacciones):
    # cantidad * precio * (100 - descuento) es entero exacto;
    # dividir entre 100 en decimal da la venta con 2 decimales
    # sin errores de punto flotante.
    centavos = (
        F.col("cantidad").cast("long")
        * F.col("precio_unitario")
        * (100 - F.col("descuento_pct"))
    )
    return (
        transacciones
        .withColumn(
            "venta_neta",
            (centavos.cast("decimal(20,0)") / 100).cast("decimal(18,2)"),
        )
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
    )


def unir(transacciones, productos):
    # productos es pequeño (2.000 filas): broadcast lo copia a cada
    # executor y evita el shuffle de las transacciones.
    catalogo = productos.select("id_producto", "categoria").dropna()
    return transacciones.join(
        F.broadcast(catalogo), on="id_producto", how="inner"
    )


def agregar(ventas):
    por_categoria = (
        ventas.groupBy("categoria")
        .agg(
            F.sum("venta_neta").alias("total_ventas"),
            F.sum("cantidad").alias("cantidad_vendida"),
            F.count("*").alias("numero_transacciones"),
        )
        .orderBy("categoria")
    )
    por_region = (
        ventas.groupBy("region")
        .agg(
            F.sum("venta_neta").alias("total_ventas"),
            F.count("*").alias("numero_transacciones"),
        )
        .orderBy("region")
    )
    mensuales = (
        ventas.groupBy("anio", "mes")
        .agg(
            F.sum("venta_neta").alias("total_ventas"),
            F.count("*").alias("numero_transacciones"),
        )
        .orderBy("anio", "mes")
    )
    return {
        "ventas_categoria.csv": por_categoria,
        "ventas_region.csv": por_region,
        "ventas_mensuales.csv": mensuales,
    }


def guardar(resultados, salida):
    salida.mkdir(parents=True, exist_ok=True)
    for nombre, df in resultados.items():
        # Los agregados tienen pocas filas: se traen al driver para
        # escribir un único CSV con el nombre exacto del contrato
        # (Spark escribiría una carpeta con archivos part-*).
        df.toPandas().to_csv(salida / nombre, index=False)
        print(f"Guardado: {salida / nombre}")


def main():
    parser = argparse.ArgumentParser(description="Ventas con PySpark")
    parser.add_argument("--datos", type=Path, default=BASE / "datos")
    parser.add_argument(
        "--salida", type=Path, default=BASE / "resultados" / "pyspark"
    )
    parser.add_argument(
        "--memoria", default="4g", help="Memoria del driver, ej. 4g"
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Guardar además el detalle limpio en Parquet por anio/mes",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Mostrar el plan físico (DAG) de la consulta",
    )
    args = parser.parse_args()

    for archivo in ("transacciones.csv", "productos.csv"):
        if not (args.datos / archivo).exists():
            parser.error(f"No existe {args.datos / archivo}")

    inicio = time.perf_counter()
    spark = crear_sesion(args.memoria)
    try:
        transacciones, productos = cargar(spark, args.datos)
        ventas = unir(transformar(limpiar(transacciones)), productos)

        # Las tres agregaciones parten del mismo DataFrame: con cache
        # el CSV se lee y limpia una sola vez en lugar de tres.
        ventas = ventas.cache()

        resultados = agregar(ventas)
        if args.explain:
            resultados["ventas_categoria.csv"].explain(mode="formatted")

        guardar(resultados, args.salida)

        if args.parquet:
            destino = args.salida / "ventas_detalle.parquet"
            (
                ventas.write.mode("overwrite")
                .partitionBy("anio", "mes")
                .parquet(str(destino))
            )
            print(f"Guardado: {destino}")

        print(f"Transacciones válidas: {ventas.count():,}")
    finally:
        spark.stop()

    print(f"Tiempo total: {time.perf_counter() - inicio:.2f} s")


if __name__ == "__main__":
    main()
