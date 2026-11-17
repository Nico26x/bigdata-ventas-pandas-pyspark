
import argparse
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import psutil


# ==========================================
# 1. CONFIGURACION DEL PROYECTO
# ==========================================

BASE = Path(__file__).resolve().parent.parent
DATOS = BASE / "datos"
SALIDA = BASE / "resultados" / "pandas_chunks"

COLUMNAS = [
    "id_transaccion",
    "fecha",
    "id_producto",
    "region",
    "cantidad",
    "precio_unitario",
    "descuento_pct",
]


def memoria_mb():
    """RAM residente del proceso en MB."""
    return psutil.Process().memory_info().rss / (1024 ** 2)


def acumular(diccionario, clave, ventas, cantidad, conteo):
    """Acumula los resultados de cada bloque."""
    valores = diccionario[clave]
    valores[0] += int(ventas)
    valores[1] += int(cantidad)
    valores[2] += int(conteo)


def procesar_bloque(bloque, productos, acumuladores):
    """Limpia, transforma, une y agrupa un bloque."""

    iniciales = len(bloque)

    # MISMAS REGLAS QUE PANDAS TRADICIONAL

    bloque = bloque.dropna(
        subset=[
            "fecha",
            "id_producto",
            "region",
            "cantidad",
            "precio_unitario",
            "descuento_pct",
        ]
    ).copy()

    bloque["fecha"] = pd.to_datetime(
        bloque["fecha"],
        errors="coerce"
    )

    bloque = bloque.dropna(subset=["fecha"])

    condicion = (
        (bloque["cantidad"] > 0)
        & (bloque["precio_unitario"] > 0)
        & (bloque["descuento_pct"] >= 0)
        & (bloque["descuento_pct"] <= 100)
    )

    bloque = bloque.loc[condicion].copy()

    eliminados = iniciales - len(bloque)

    if bloque.empty:
        return iniciales, eliminados, 0

    # TRANSFORMACIONES

    # El resultado representa centavos de COP.
    # Permite acumular sumas enteras exactas.
    bloque["venta_centavos"] = (
        bloque["cantidad"].astype("int64")
        * bloque["precio_unitario"].astype("int64")
        * (
            100
            - bloque["descuento_pct"].astype("int64")
        )
    )

    bloque["anio"] = bloque["fecha"].dt.year
    bloque["mes"] = bloque["fecha"].dt.month

    # JOIN CON EL CATALOGO

    datos = bloque.merge(
        productos,
        on="id_producto",
        how="inner",
        validate="many_to_one",
    )

    # AGRUPACION POR CATEGORIA

    categorias = datos.groupby(
        "categoria",
        as_index=False
    ).agg(
        ventas=("venta_centavos", "sum"),
        cantidad=("cantidad", "sum"),
        conteo=("id_transaccion", "size"),
    )

    for fila in categorias.itertuples(index=False):
        acumular(
            acumuladores["categoria"],
            fila.categoria,
            fila.ventas,
            fila.cantidad,
            fila.conteo,
        )

    # AGRUPACION POR REGION

    regiones = datos.groupby(
        "region",
        as_index=False
    ).agg(
        ventas=("venta_centavos", "sum"),
        cantidad=("cantidad", "sum"),
        conteo=("id_transaccion", "size"),
    )

    for fila in regiones.itertuples(index=False):
        acumular(
            acumuladores["region"],
            fila.region,
            fila.ventas,
            fila.cantidad,
            fila.conteo,
        )

    # AGRUPACION POR MES

    meses = datos.groupby(
        ["anio", "mes"],
        as_index=False
    ).agg(
        ventas=("venta_centavos", "sum"),
        cantidad=("cantidad", "sum"),
        conteo=("id_transaccion", "size"),
    )

    for fila in meses.itertuples(index=False):
        acumular(
            acumuladores["mensual"],
            (int(fila.anio), int(fila.mes)),
            fila.ventas,
            fila.cantidad,
            fila.conteo,
        )

    return iniciales, eliminados, len(datos)


def convertir_pesos(centavos):
    """Convierte centavos enteros a texto decimal."""
    centavos = int(centavos)
    return f"{centavos // 100}.{centavos % 100:02d}"


def guardar_resultados(acumuladores):
    """Guarda resultados con el mismo esquema tradicional."""

    SALIDA.mkdir(parents=True, exist_ok=True)

    filas_categoria = []

    for categoria, valores in sorted(
        acumuladores["categoria"].items()
    ):
        filas_categoria.append({
            "categoria": categoria,
            "cantidad_vendida": valores[1],
            "numero_transacciones": valores[2],
            "total_ventas": convertir_pesos(valores[0]),
        })

    filas_region = []

    for region, valores in sorted(
        acumuladores["region"].items()
    ):
        filas_region.append({
            "region": region,
            "numero_transacciones": valores[2],
            "total_ventas": convertir_pesos(valores[0]),
        })

    filas_mensuales = []

    for (anio, mes), valores in sorted(
        acumuladores["mensual"].items()
    ):
        filas_mensuales.append({
            "anio": anio,
            "mes": mes,
            "numero_transacciones": valores[2],
            "total_ventas": convertir_pesos(valores[0]),
        })

    archivos = {
        "ventas_categoria.csv": (
            filas_categoria,
            [
                "categoria",
                "cantidad_vendida",
                "numero_transacciones",
                "total_ventas",
            ],
        ),
        "ventas_region.csv": (
            filas_region,
            [
                "region",
                "numero_transacciones",
                "total_ventas",
            ],
        ),
        "ventas_mensuales.csv": (
            filas_mensuales,
            [
                "anio",
                "mes",
                "numero_transacciones",
                "total_ventas",
            ],
        ),
    }

    for nombre, (filas, columnas) in archivos.items():
        tabla = pd.DataFrame(filas, columns=columnas)
        tabla.to_csv(SALIDA / nombre, index=False)
        print(f"Guardado: {SALIDA / nombre}")


def ejecutar(tamano_bloque):
    print("=" * 55)
    print("PROCESAMIENTO PANDAS OPTIMIZADO CON CHUNKS")
    print("=" * 55)

    inicio = time.perf_counter()
    ram_inicial = memoria_mb()
    mayor_ram_observada = ram_inicial

    print("\n[1] CARGANDO CATALOGO")

    productos = pd.read_csv(
        DATOS / "productos.csv"
    )

    if productos["id_producto"].duplicated().any():
        raise ValueError(
            "Existen ID duplicados en el catalogo."
        )

    acumuladores = {
        "categoria": defaultdict(lambda: [0, 0, 0]),
        "region": defaultdict(lambda: [0, 0, 0]),
        "mensual": defaultdict(lambda: [0, 0, 0]),
    }

    registros_leidos = 0
    registros_eliminados = 0
    registros_procesados = 0
    bloques_procesados = 0

    print("\n[2] PROCESANDO TRANSACCIONES POR BLOQUES")
    print(f"Tamano de bloque: {tamano_bloque:,}")

    lector = pd.read_csv(
        DATOS / "transacciones.csv",
        chunksize=tamano_bloque,
        usecols=COLUMNAS,
    )

    for bloque in lector:
        bloques_procesados += 1

        iniciales, eliminados, procesados = procesar_bloque(
            bloque,
            productos,
            acumuladores,
        )

        registros_leidos += iniciales
        registros_eliminados += eliminados
        registros_procesados += procesados

        # Muestra puntual, no pico real de memoria.
        mayor_ram_observada = max(
            mayor_ram_observada,
            memoria_mb()
        )

        if bloques_procesados % 10 == 0:
            print(
                f"Bloques: {bloques_procesados:,} | "
                f"Filas leidas: {registros_leidos:,}"
            )

    print("\n[3] GUARDANDO RESULTADOS")
    guardar_resultados(acumuladores)

    tiempo_total = time.perf_counter() - inicio

    print("\n" + "=" * 55)
    print("RESUMEN DE EJECUCION")
    print("=" * 55)
    print(f"Bloques procesados: {bloques_procesados:,}")
    print(f"Registros leidos: {registros_leidos:,}")
    print(f"Registros eliminados: {registros_eliminados:,}")
    print(f"Registros despues del JOIN: {registros_procesados:,}")
    print(f"Tiempo total: {tiempo_total:.4f} segundos")
    print(f"RAM inicial: {ram_inicial:.2f} MB")
    print(
        "Mayor RAM observada al finalizar bloques: "
        f"{mayor_ram_observada:.2f} MB"
    )
    print("\nPROCESAMIENTO FINALIZADO")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Cantidad de filas por bloque",
    )

    args = parser.parse_args()

    if args.chunksize <= 0:
        parser.error("--chunksize debe ser mayor que cero")

    ejecutar(args.chunksize)
