
import argparse
import time
from pathlib import Path

import pandas as pd
import psutil


BASE = Path(__file__).resolve().parent.parent
CARPETA_DATOS = BASE / "datos"
CARPETA_RESULTADOS = BASE / "resultados" / "pandas"


def memoria_mb():
    """Memoria RAM del proceso actual en MB."""
    proceso = psutil.Process()
    return proceso.memory_info().rss / (1024 ** 2)


def cargar_datos():
    """Carga los archivos originales sin utilizar Spark."""

    print("\n[1] CARGANDO DATOS")

    transacciones = pd.read_csv(
        CARPETA_DATOS / "transacciones.csv"
    )

    productos = pd.read_csv(
        CARPETA_DATOS / "productos.csv"
    )

    print(f"Transacciones: {len(transacciones):,}")
    print(f"Productos: {len(productos):,}")

    return transacciones, productos


def limpiar_datos(transacciones):
    """Elimina registros que no cumplen las reglas."""

    print("\n[2] LIMPIANDO DATOS")

    total_inicial = len(transacciones)

    columnas = [
        "fecha",
        "id_producto",
        "region",
        "cantidad",
        "precio_unitario",
        "descuento_pct",
    ]

    transacciones = transacciones.dropna(
        subset=columnas
    ).copy()

    transacciones["fecha"] = pd.to_datetime(
        transacciones["fecha"],
        errors="coerce"
    )

    transacciones = transacciones.dropna(
        subset=["fecha"]
    )

    condicion = (
        (transacciones["cantidad"] > 0)
        & (transacciones["precio_unitario"] > 0)
        & (transacciones["descuento_pct"] >= 0)
        & (transacciones["descuento_pct"] <= 100)
    )

    transacciones = transacciones.loc[
        condicion
    ].copy()

    eliminados = total_inicial - len(transacciones)

    print(f"Registros eliminados: {eliminados:,}")
    print(f"Registros validos: {len(transacciones):,}")

    return transacciones


def transformar_datos(transacciones):
    """Calcula ventas y extrae año y mes."""

    print("\n[3] TRANSFORMANDO DATOS")

    # Los precios están expresados en COP enteros.
    # Se calculan centavos exactos para evitar
    # errores acumulados de punto flotante.
    #
    # Ejemplo: 3 * 25000 COP con 10 % descuento
    # equivale a 67500.00 COP.

    transacciones["venta_centavos"] = (
        transacciones["cantidad"].astype("int64")
        * transacciones["precio_unitario"].astype("int64")
        * (
            100
            - transacciones["descuento_pct"].astype("int64")
        )
    )

    transacciones["anio"] = (
        transacciones["fecha"].dt.year
    )

    transacciones["mes"] = (
        transacciones["fecha"].dt.month
    )

    print("Ventas netas calculadas correctamente.")

    return transacciones


def unir_datos(transacciones, productos):
    """Relaciona las ventas con el catálogo."""

    print("\n[4] REALIZANDO JOIN")

    if productos["id_producto"].duplicated().any():
        raise ValueError(
            "El catalogo contiene ID de producto duplicados."
        )

    datos = pd.merge(
        transacciones,
        productos,
        on="id_producto",
        how="inner",
        validate="many_to_one"
    )

    print(f"Registros despues del JOIN: {len(datos):,}")

    return datos


def agregar_datos(datos):
    """Genera los tres análisis solicitados."""

    print("\n[5] REALIZANDO AGRUPACIONES")

    por_categoria = (
        datos.groupby("categoria", as_index=False)
        .agg(
            total_centavos=("venta_centavos", "sum"),
            cantidad_vendida=("cantidad", "sum"),
            numero_transacciones=("id_transaccion", "size")
        )
        .sort_values("categoria")
    )

    por_region = (
        datos.groupby("region", as_index=False)
        .agg(
            total_centavos=("venta_centavos", "sum"),
            numero_transacciones=("id_transaccion", "size")
        )
        .sort_values("region")
    )

    por_mes = (
        datos.groupby(["anio", "mes"], as_index=False)
        .agg(
            total_centavos=("venta_centavos", "sum"),
            numero_transacciones=("id_transaccion", "size")
        )
        .sort_values(["anio", "mes"])
    )

    return por_categoria, por_region, por_mes


def convertir_montos(tabla):
    """Convierte centavos a COP con dos decimales."""

    tabla = tabla.copy()

    tabla["total_ventas"] = tabla[
        "total_centavos"
    ].map(
        lambda x: f"{int(x) // 100}.{int(x) % 100:02d}"
    )

    tabla = tabla.drop(columns=["total_centavos"])

    return tabla


def guardar_resultados(resultados):
    """Escribe archivos CSV con resultados comparables."""

    print("\n[6] GUARDANDO RESULTADOS")

    CARPETA_RESULTADOS.mkdir(
        parents=True, exist_ok=True
    )

    nombres = [
        "ventas_categoria.csv",
        "ventas_region.csv",
        "ventas_mensuales.csv",
    ]

    for nombre, tabla in zip(nombres, resultados):
        tabla = convertir_montos(tabla)

        ruta = CARPETA_RESULTADOS / nombre
        tabla.to_csv(ruta, index=False)

        print(f"Guardado: {ruta}")


def ejecutar():
    print("=" * 50)
    print("PROCESAMIENTO MASIVO CON PANDAS")
    print("=" * 50)

    inicio = time.perf_counter()
    memoria_inicial = memoria_mb()

    transacciones, productos = cargar_datos()

    transacciones = limpiar_datos(transacciones)

    transacciones = transformar_datos(transacciones)

    datos = unir_datos(transacciones, productos)

    resultados = agregar_datos(datos)

    guardar_resultados(resultados)

    tiempo_total = time.perf_counter() - inicio
    memoria_final = memoria_mb()

    print("\n" + "=" * 50)
    print("RESUMEN DE EJECUCION")
    print("=" * 50)

    print(f"Tiempo total: {tiempo_total:.4f} segundos")
    print(f"RAM inicial: {memoria_inicial:.2f} MB")
    print(f"RAM final: {memoria_final:.2f} MB")
    print(f"Registros procesados: {len(datos):,}")

    print("\nPROCESAMIENTO FINALIZADO")


if __name__ == "__main__":
    ejecutar()
