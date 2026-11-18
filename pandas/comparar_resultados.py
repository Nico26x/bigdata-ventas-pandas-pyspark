
from pathlib import Path

import pandas as pd


BASE = Path(__file__).resolve().parent.parent

TRADICIONAL = BASE / "resultados" / "pandas"
CHUNKS = BASE / "resultados" / "pandas_chunks"

ARCHIVOS = {
    "ventas_categoria.csv": ["categoria"],
    "ventas_region.csv": ["region"],
    "ventas_mensuales.csv": ["anio", "mes"],
}


def comparar(nombre, claves):
    print(f"\nComparando {nombre}...")

    ruta_a = TRADICIONAL / nombre
    ruta_b = CHUNKS / nombre

    a = pd.read_csv(
        ruta_a,
        dtype={"total_ventas": "string"},
    )

    b = pd.read_csv(
        ruta_b,
        dtype={"total_ventas": "string"},
    )

    a = a.sort_values(claves).reset_index(drop=True)
    b = b.sort_values(claves).reset_index(drop=True)

    # Se compara también el esquema.
    pd.testing.assert_frame_equal(
        a,
        b,
        check_dtype=True,
    )

    print("OK: resultados exactamente iguales.")
    print(f"Filas comparadas: {len(a)}")


def main():
    print("=" * 50)
    print("COMPARACION PANDAS VS PANDAS CHUNKS")
    print("=" * 50)

    for nombre, claves in ARCHIVOS.items():
        comparar(nombre, claves)

    print("\nTODAS LAS COMPARACIONES FUERON EXITOSAS")


if __name__ == "__main__":
    main()
