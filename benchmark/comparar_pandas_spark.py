
from pathlib import Path
from decimal import Decimal, InvalidOperation

import pandas as pd


BASE = Path(__file__).resolve().parent.parent

PANDAS = BASE / "resultados" / "pandas"
SPARK = BASE / "resultados" / "pyspark"

ARCHIVOS = {
    "ventas_categoria.csv": ["categoria"],
    "ventas_region.csv": ["region"],
    "ventas_mensuales.csv": ["anio", "mes"],
}


def normalizar_monto(valor):
    """Compara montos de forma decimal exacta."""
    try:
        return Decimal(str(valor)).quantize(
            Decimal("0.01")
        )
    except (InvalidOperation, ValueError):
        raise ValueError(f"Monto invalido: {valor}")


def comparar(nombre, claves):
    ruta_pandas = PANDAS / nombre
    ruta_spark = SPARK / nombre

    if not ruta_pandas.exists():
        raise FileNotFoundError(ruta_pandas)

    if not ruta_spark.exists():
        raise FileNotFoundError(ruta_spark)

    a = pd.read_csv(
        ruta_pandas,
        dtype={"total_ventas": "string"},
    )

    b = pd.read_csv(
        ruta_spark,
        dtype={"total_ventas": "string"},
    )

    columnas_a = set(a.columns)
    columnas_b = set(b.columns)

    if columnas_a != columnas_b:
        raise AssertionError(
            f"Columnas diferentes en {nombre}:\n"
            f"pandas: {columnas_a}\n"
            f"Spark: {columnas_b}"
        )

    if a.duplicated(subset=claves).any():
        raise AssertionError(
            f"Claves duplicadas en pandas: {nombre}"
        )

    if b.duplicated(subset=claves).any():
        raise AssertionError(
            f"Claves duplicadas en Spark: {nombre}"
        )

    a = a.sort_values(claves).reset_index(drop=True)
    b = b.sort_values(claves).reset_index(drop=True)

    a = a.reindex(columns=sorted(a.columns))
    b = b.reindex(columns=sorted(b.columns))

    a["total_ventas"] = a["total_ventas"].map(
        normalizar_monto
    )

    b["total_ventas"] = b["total_ventas"].map(
        normalizar_monto
    )

    pd.testing.assert_frame_equal(
        a,
        b,
        check_dtype=False,
        check_exact=True,
    )

    print(f"OK: {nombre} - {len(a)} filas iguales")


def main():
    print("=" * 55)
    print("COMPARACION DE PANDAS VS PYSPARK")
    print("=" * 55)

    for nombre, claves in ARCHIVOS.items():
        comparar(nombre, claves)

    print("\nTODAS LAS COMPARACIONES FUERON EXITOSAS")


if __name__ == "__main__":
    main()
