
# Especificaciones del proyecto Big Data

## 1. Objetivo

Comparar el procesamiento de transacciones
comerciales utilizando pandas y PySpark.

Ambas implementaciones deben trabajar con
los mismos datos y producir resultados equivalentes.

## 2. Archivos de entrada

### transacciones.csv

- id_transaccion: identificador único, entero.
- fecha: fecha de venta, formato YYYY-MM-DD.
- id_producto: identificador del producto, entero.
- region: región donde ocurrió la venta.
- cantidad: unidades vendidas, entero.
- precio_unitario: precio unitario en COP, entero.
- descuento_pct: descuento porcentual, entero.
- metodo_pago: método utilizado para pagar.

### productos.csv

- id_producto: identificador único del producto.
- nombre_producto: nombre del producto.
- categoria: categoría comercial.

## 3. Reglas de limpieza

Ambos programas deben:

1. Eliminar transacciones con cantidad <= 0.
2. Eliminar transacciones con precio_unitario <= 0.
3. Eliminar filas con valores nulos en las
   columnas necesarias para el procesamiento.
4. Convertir fecha al tipo fecha.
5. Conservar únicamente transacciones cuyo
   id_producto exista en productos.csv.
6. No eliminar registros válidos arbitrariamente.

## 4. Transformaciones

Calcular la columna:

venta_neta = cantidad * precio_unitario *
             (1 - descuento_pct / 100)

Extraer el año y mes de la fecha.

Para los valores monetarios, ambas implementaciones
deben usar cálculos decimales exactos cuando sea
posible, con dos posiciones decimales.

## 5. Operación JOIN

Realizar un INNER JOIN entre transacciones
y productos mediante id_producto.

## 6. Agregaciones obligatorias

### Ventas por categoría

Agrupar por categoria y calcular:

- total_ventas: suma de venta_neta.
- cantidad_vendida: suma de cantidad.
- numero_transacciones: cantidad de registros.

### Ventas por región

Agrupar por region y calcular:

- total_ventas.
- numero_transacciones.

### Ventas por mes

Agrupar por año y mes y calcular:

- total_ventas.
- numero_transacciones.

## 7. Archivos de salida

Cada implementación debe generar:

- ventas_categoria.csv
- ventas_region.csv
- ventas_mensuales.csv

La versión pandas guardará los resultados
en resultados/pandas/.

La versión PySpark guardará los resultados
en resultados/pyspark/.

Los nombres de columnas deben coincidir
en ambas implementaciones.

## 8. Comparación

Realizar pruebas con tamaños crecientes:

- 1 % del volumen final.
- 10 % del volumen final.
- 50 % del volumen final.
- 100 % del volumen final.

Cada medición se repetirá al menos tres veces.

Medir:
- Tiempo de ejecución.
- Uso de memoria RAM.
- Consumo de CPU.
- Resultado de la ejecución.

Ejecutar las pruebas en el mismo equipo.

## 9. Generación de datos

Generador: generador/generar_datos.py

Semilla fija: 42.

Comando de ejemplo:

python generador/generar_datos.py --filas 10000

La generación reemplaza los CSV existentes.

## 10. Reproducibilidad

Ambos integrantes deben usar los mismos
archivos generados o la misma configuración
del generador.

Los resultados deben coincidir numéricamente,
independientemente del orden de las filas.
