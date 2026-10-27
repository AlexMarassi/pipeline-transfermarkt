# Idempotencia — mes 2024-03

<<<<<<< Updated upstream
Salida de `scripts/idempotency_check.py 2024-03` después de la primera ejecución y después de re-ejecutar el mes con Clear (las 8 tareas, `try_number` 2). Un `diff` entre ambas no muestra diferencias.
=======
Salida de `scripts/idempotency_check.py 2024-03` después de la primera ejecución y después de re-ejecutar el mes con Clear (las 8 tareas se ejecutan de nuevo). Un `diff` entre ambas no muestra diferencias.
>>>>>>> Stashed changes

## Antes (primera ejecución)
```
Idempotencia del mes 2024-03
tabla                        filas  archivo                                     sha256
silver games                   112  silver/games/2024-03.parquet                adc54d72e29e0888…
silver appearances            3445  silver/appearances/2024-03.parquet          2ece25d41cd9e792…
gold player_appearances       3445  gold/player_appearances/year=2024/month=03/part-0.parquet  ac91fb38fe548c6a…
archivos en la partición Gold: 1
```

## Después (Clear y re-ejecución)
```
Idempotencia del mes 2024-03
tabla                        filas  archivo                                     sha256
silver games                   112  silver/games/2024-03.parquet                adc54d72e29e0888…
silver appearances            3445  silver/appearances/2024-03.parquet          2ece25d41cd9e792…
gold player_appearances       3445  gold/player_appearances/year=2024/month=03/part-0.parquet  ac91fb38fe548c6a…
archivos en la partición Gold: 1
```

**Resultado:** mismas filas (112 partidos, 3.445 apariciones en Silver, 3.445 en Gold), mismos SHA-256 y un único archivo en la partición Gold. Re-ejecutar no duplica ni cambia nada.
