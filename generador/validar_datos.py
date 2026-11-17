
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
DATOS = BASE / "datos"

transacciones = pd.read_csv(
    DATOS / "transacciones.csv"
)

productos = pd.read_csv(
    DATOS / "productos.csv"
)

print("\n=== VALIDACION DEL DATASET ===")

print("\n1. Dimensiones:")
print("Transacciones:", transacciones.shape)
print("Productos:", productos.shape)

print("\n2. Columnas:")
print(transacciones.columns.tolist())
print(productos.columns.tolist())

print("\n3. Valores nulos:")
print(transacciones.isnull().sum())

print("\n4. Registros invalidos:")
invalidos = (
    (transacciones["cantidad"] <= 0)
    | (transacciones["precio_unitario"] <= 0)
)
print("Total invalidos:", invalidos.sum())

print("\n5. Identificadores duplicados:")
print(
    transacciones["id_transaccion"].duplicated().sum()
)

print("\n6. Productos sin correspondencia:")
sin_producto = ~transacciones["id_producto"].isin(
    productos["id_producto"]
)
print("Total sin producto:", sin_producto.sum())

print("\n7. Ejemplo de transacciones:")
print(transacciones.head())

print("\n8. Ejemplo de productos:")
print(productos.head())

print("\n=== FIN DE VALIDACION ===")
