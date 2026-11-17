
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


SEMILLA = 42
TAMANO_BLOQUE = 100_000

CATEGORIAS = [
    "Alimentos",
    "Tecnologia",
    "Ropa",
    "Hogar",
    "Cuidado personal",
]

REGIONES = [
    "Quindio",
    "Risaralda",
    "Caldas",
    "Valle del Cauca",
    "Antioquia",
    "Bogota",
    "Tolima",
    "Cundinamarca",
]

METODOS_PAGO = [
    "Efectivo",
    "Tarjeta",
    "Transferencia",
    "Billetera digital",
]


def generar_productos(carpeta):
    rng = np.random.default_rng(SEMILLA + 1)

    cantidad = 2000
    identificadores = np.arange(1, cantidad + 1)

    productos = pd.DataFrame({
        "id_producto": identificadores,
        "nombre_producto": [
            f"Producto_{i:04d}" for i in identificadores
        ],
        "categoria": rng.choice(
            CATEGORIAS, size=cantidad
        ),
    })

    ruta = carpeta / "productos.csv"
    productos.to_csv(ruta, index=False)

    print(f"Catalogo generado: {ruta}")
    return cantidad


def generar_transacciones(carpeta, total):
    rng = np.random.default_rng(SEMILLA)

    ruta = carpeta / "transacciones.csv"
    generados = 0

    # El archivo se escribe en bloques para
    # no almacenar todas las filas en memoria.
    while generados < total:
        n = min(TAMANO_BLOQUE, total - generados)

        fechas = (
            np.datetime64("2024-01-01")
            + rng.integers(0, 731, size=n).astype(
                "timedelta64[D]"
            )
        )

        cantidades = rng.integers(1, 11, size=n)
        precios = rng.integers(1000, 500001, size=n)

        # Introducimos registros invalidos
        # de manera controlada.
        invalidos = rng.random(n) < 0.01
        tipo_error = rng.random(n) < 0.5

        cantidades[invalidos & tipo_error] = 0
        precios[invalidos & ~tipo_error] = -100

        bloque = pd.DataFrame({
            "id_transaccion": np.arange(
                generados + 1,
                generados + n + 1
            ),
            "fecha": fechas,
            "id_producto": rng.integers(
                1, 2001, size=n
            ),
            "region": rng.choice(
                REGIONES, size=n
            ),
            "cantidad": cantidades,
            "precio_unitario": precios,
            "descuento_pct": rng.choice(
                [0, 5, 10, 15, 20], size=n
            ),
            "metodo_pago": rng.choice(
                METODOS_PAGO, size=n
            ),
        })

        bloque.to_csv(
            ruta,
            mode="w" if generados == 0 else "a",
            header=(generados == 0),
            index=False,
            date_format="%Y-%m-%d",
        )

        generados += n
        print(
            f"Generados: {generados:,} / {total:,}",
            end="\r",
        )

    print(f"\nTransacciones generadas: {ruta}")


def main():
    parser = argparse.ArgumentParser(
        description="Generador de datos Big Data"
    )

    parser.add_argument(
        "--filas",
        type=int,
        default=10_000,
        help="Cantidad de transacciones",
    )

    args = parser.parse_args()

    if args.filas <= 0:
        parser.error("--filas debe ser positivo")

    carpeta = Path(__file__).resolve().parent.parent / "datos"
    carpeta.mkdir(parents=True, exist_ok=True)

    print("Iniciando generacion de datos...")

    generar_productos(carpeta)
    generar_transacciones(carpeta, args.filas)

    print("Proceso completado correctamente.")


if __name__ == "__main__":
    main()
