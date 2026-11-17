
import argparse
import csv
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import psutil


BASE = Path(__file__).resolve().parent.parent
METRICAS = BASE / "evidencias" / "metricas"
LOGS = BASE / "evidencias" / "logs"

CAMPOS = [
    "fecha",
    "version",
    "filas",
    "repeticion",
    "chunksize",
    "tiempo_segundos",
    "ram_max_mb",
    "cpu_max_porcentaje",
    "estado",
    "codigo_salida",
    "ram_equipo_gb",
    "procesador",
    "sistema",
    "python",
    "archivo_log",
]


def preparar_carpetas():
    METRICAS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    

def verificar_cantidad_filas(cantidad_esperada):
    """
    Comprueba que el CSV contiene exactamente
    la cantidad de transacciones indicada.
    """

    archivo = BASE / "datos" / "transacciones.csv"

    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontro el archivo: {archivo}"
        )

    print("\nVerificando cantidad de registros...")

    with archivo.open(
        "r",
        encoding="utf-8",
        newline=""
    ) as f:
        cantidad_real = sum(1 for _ in f) - 1

    print(f"Registros encontrados: {cantidad_real:,}")
    print(f"Registros esperados: {cantidad_esperada:,}")

    if cantidad_real != cantidad_esperada:
        raise ValueError(
            "La cantidad de registros no coincide. "
            "Verifica el dataset antes de ejecutar."
        )

    print("Dataset verificado correctamente.\n")


def construir_comando(version, chunksize):
    if version == "tradicional":
        return [
            sys.executable,
            str(BASE / "pandas" / "procesamiento.py"),
        ]

    return [
        sys.executable,
        str(BASE / "pandas" / "procesamiento_chunks.py"),
        "--chunksize",
        str(chunksize),
    ]
    

def obtener_procesos(proceso_principal):
    """
    Obtiene el proceso principal y todos sus
    procesos hijos, incluidos los recursivos.
    """
    try:
        principal = psutil.Process(proceso_principal.pid)
        return [principal] + principal.children(
            recursive=True
        )
    except psutil.NoSuchProcess:
        return []



def ejecutar_prueba(
    version,
    filas,
    repeticion,
    chunksize,
    timeout,
):
    identificador = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    nombre = (
        f"{version}_{filas}_rep{repeticion}_"
        f"{identificador}.log"
    )

    ruta_log = LOGS / nombre
    comando = construir_comando(version, chunksize)

    print(f"\nVersion: {version}")
    print(f"Filas: {filas:,}")
    print(f"Repeticion: {repeticion}")

    ram_maxima = 0.0
    cpu_maxima = 0.0
    estado = "ERROR"
    codigo_salida = None

    # Conserva las observaciones de CPU
    # de cada proceso individual.
    cpu_anterior = {}
    instante_anterior = {}

    inicio = time.perf_counter()

    with ruta_log.open("w", encoding="utf-8") as log:
        proceso = subprocess.Popen(
            comando,
            cwd=BASE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        while True:
            procesos = obtener_procesos(proceso)

            ram_total = 0.0
            cpu_total = 0.0

            for p in procesos:
                try:
                    with p.oneshot():
                        identidad = (
                            p.pid,
                            p.create_time()
                        )

                        memoria = p.memory_info().rss
                        tiempos = p.cpu_times()

                    ram_total += memoria / (1024 ** 2)

                    cpu_acumulada = (
                        tiempos.user + tiempos.system
                    )

                    ahora = time.perf_counter()

                    if identidad in cpu_anterior:
                        diferencia_cpu = (
                            cpu_acumulada
                            - cpu_anterior[identidad]
                        )

                        diferencia_tiempo = (
                            ahora
                            - instante_anterior[identidad]
                        )

                        if diferencia_tiempo > 0:
                            cpu_total += max(
                                0.0,
                                100 * diferencia_cpu
                                / diferencia_tiempo
                            )

                    cpu_anterior[identidad] = cpu_acumulada
                    instante_anterior[identidad] = ahora

                except (
                    psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess,
                ):
                    continue

            ram_maxima = max(
                ram_maxima,
                ram_total
            )

            cpu_maxima = max(
                cpu_maxima,
                cpu_total
            )

            if proceso.poll() is not None:
                break

            if time.perf_counter() - inicio >= timeout:
                estado = "TIMEOUT"

                # Terminar hijos antes del padre.
                for p in reversed(procesos):
                    try:
                        p.kill()
                    except (
                        psutil.NoSuchProcess,
                        psutil.AccessDenied,
                    ):
                        pass

                break

            time.sleep(0.02)

        codigo_salida = proceso.wait()

    tiempo_total = time.perf_counter() - inicio

    if estado != "TIMEOUT":
        estado = (
            "OK" if codigo_salida == 0 else "ERROR"
        )

    resultado = {
        "fecha": datetime.now().isoformat(
            timespec="seconds"
        ),
        "version": version,
        "filas": filas,
        "repeticion": repeticion,
        "chunksize": (
            chunksize if version == "chunks" else ""
        ),
        "tiempo_segundos": round(tiempo_total, 4),
        "ram_max_mb": round(ram_maxima, 2),
        "cpu_max_porcentaje": round(cpu_maxima, 2),
        "estado": estado,
        "codigo_salida": codigo_salida,
        "ram_equipo_gb": round(
            psutil.virtual_memory().total / (1024 ** 3),
            2
        ),
        "procesador": platform.processor(),
        "sistema": platform.platform(),
        "python": platform.python_version(),
        "archivo_log": str(ruta_log.relative_to(BASE)),
    }

    print(f"Estado: {estado}")
    print(f"Tiempo: {tiempo_total:.4f} segundos")

    print(
        "RAM maxima observada (arbol de procesos): "
        f"{ram_maxima:.2f} MB"
    )

    print(
        "CPU maxima estimada (arbol de procesos): "
        f"{cpu_maxima:.2f} %"
    )

    print(f"Log: {ruta_log}")

    return resultado


def guardar_metrica(resultado):
    archivo = METRICAS / "rendimiento.csv"

    existe = archivo.exists()

    with archivo.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as f:
        escritor = csv.DictWriter(
            f,
            fieldnames=CAMPOS,
        )

        if not existe or archivo.stat().st_size == 0:
            escritor.writeheader()

        escritor.writerow(resultado)


def main():
    parser = argparse.ArgumentParser(
        description="Medicion experimental de pandas"
    )

    parser.add_argument(
        "--version",
        choices=["tradicional", "chunks"],
        required=True,
    )

    parser.add_argument(
        "--filas",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--repeticiones",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Tiempo limite por ejecucion en segundos",
    )

    args = parser.parse_args()

    if (
        args.filas <= 0
        or args.repeticiones <= 0
        or args.chunksize <= 0
        or args.timeout <= 0
    ):
        parser.error(
            "Los parametros numericos deben ser positivos."
        )

    preparar_carpetas()
    
    verificar_cantidad_filas(args.filas)

    for repeticion in range(1, args.repeticiones + 1):
        resultado = ejecutar_prueba(
            args.version,
            args.filas,
            repeticion,
            args.chunksize,
            args.timeout,
        )

        guardar_metrica(resultado)

        if resultado["estado"] != "OK":
            print(
                "\nLa ejecucion no termino correctamente."
                " Revisa el archivo log."
            )
            break

    print("\nMediciones guardadas en:")
    print(METRICAS / "rendimiento.csv")


if __name__ == "__main__":
    main()
