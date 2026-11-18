
$ErrorActionPreference = "Stop"

$python = (Resolve-Path ".\.venv-spark\Scripts\python.exe").Path

$volumenes = @(500000, 5000000, 10000000)
$repeticiones = 3

$carpeta = Join-Path (Get-Location).Path "evidencias\spark\benchmarks"
New-Item -ItemType Directory -Force $carpeta | Out-Null

$resultados = @()

foreach ($filas in $volumenes) {

    Write-Host "`n================================"
    Write-Host "GENERANDO $filas TRANSACCIONES"
    Write-Host "================================"

    & $python generador/generar_datos.py --filas $filas

    if ($LASTEXITCODE -ne 0) {
        throw "Error generando datos para $filas registros"
    }

    for ($rep = 1; $rep -le $repeticiones; $rep++) {

        Write-Host "`nPrueba Spark $filas - Repeticion $rep"

        $salida = Join-Path $carpeta "spark_${filas}_rep${rep}_stdout.log"
        $errores = Join-Path $carpeta "spark_${filas}_rep${rep}_stderr.log"

        $inicio = Get-Date

        $proceso = Start-Process `
            -FilePath $python `
            -ArgumentList @(
                "spark/procesamiento.py",
                "--memoria",
                "2g"
            ) `
            -WorkingDirectory (Get-Location).Path `
            -RedirectStandardOutput $salida `
            -RedirectStandardError $errores `
            -Wait `
            -PassThru `
            -NoNewWindow

        $fin = Get-Date

        $codigo = $proceso.ExitCode
        $segundos = ($fin - $inicio).TotalSeconds

        $resultados += [PSCustomObject]@{
            Filas = $filas
            Repeticion = $rep
            TiempoSegundos = [math]::Round($segundos, 3)
            Estado = if ($codigo -eq 0) { "OK" } else { "ERROR" }
            LogSalida = $salida
            LogErrores = $errores
        }

        Write-Host "Tiempo: $([math]::Round($segundos, 3)) segundos"
        Write-Host "Estado: $(if ($codigo -eq 0) { 'OK' } else { 'ERROR' })"

        $resultados |
            Export-Csv (Join-Path $carpeta "resumen_spark.csv") `
                -NoTypeInformation -Encoding UTF8

        if ($codigo -ne 0) {
            Write-Warning "Spark fallo. Revisa el archivo de errores."
            break
        }
    }
}

Write-Host "`n================================"
Write-Host "PRUEBAS FINALIZADAS"
Write-Host "================================"

$resultados | Format-Table -AutoSize
