#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Un solo archivo:
# python data_to_csv.py --in /ruta/datos_ES.csv
# Genera /ruta/datos_ES.Last.txt, /ruta/datos_ES.Bid.txt, /ruta/datos_ES.Ask.txt

# carpeta
# python data_to_csv.py --in /ruta/csvs --out /ruta/salida
# Para cada .csv en /ruta/csvs genera 3 archivos en /ruta/salida




from __future__ import annotations
import csv
from pathlib import Path
from typing import Optional, Tuple, Iterator, TextIO
import argparse

# =========================
# Tipos/constantes del feed
# =========================
TYPE_BID: int = 0
TYPE_ASK: int = 1
TYPE_LAST: int = 2

# Estructura de las líneas de entrada (CSV con ';'):
# L1: ts;level;type;price;volume
# L2: ts;level;type;price;volume;action;depth   (se ignora por ser Level=2)


def parse_line_campos(campos: list[str]) -> Optional[Tuple[str, int, int, str, str]]:
    """
    Devuelve (timestamp20, level, type, price_str, volume_str) si la línea es válida (L1 o L2);
    si no, devuelve None. No convierte a float/int para preservar formato y rendimiento.
    """
    # Longitudes válidas: 5 (L1) o 7 (L2)
    if len(campos) not in (5, 7):
        return None
    try:
        timestamp_raw: str = campos[0].strip()
        # Esperamos 'YYYYMMDDhhmmssffffff' → 20 dígitos
        if len(timestamp_raw) != 20 or not timestamp_raw.isdigit():
            return None

        level: int = int(campos[1])
        tipo: int = int(campos[2])

        price_str: str = campos[3].strip()
        volume_str: str = campos[4].strip()

        return (timestamp_raw, level, tipo, price_str, volume_str)
    except Exception:
        return None


def ts20_to_nt_parts(ts20: str) -> Tuple[str, str, str]:
    """
    Convierte 'YYYYMMDDhhmmssffffff' (20 dígitos) a:
        yyyyMMdd, HHmmss, ffffffff (7 dígitos)
    NinjaTrader requiere 7 dígitos en la fracción ('fffffff').
    Tomamos microsegundos (6) y agregamos '0' al final.
    """
    fecha: str = ts20[:8]     # YYYYMMDD
    hora: str = ts20[8:14]    # hhmmss
    micros: str = ts20[14:20] # ffffff (6 dígitos)
    f7: str = micros + "0"    # -> 7 dígitos
    return fecha, hora, f7


def iter_csv_lineas(fp: TextIO) -> Iterator[list[str]]:
    lector = csv.reader(fp, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    for campos in lector:
        if not campos:
            continue
        yield campos


def exportar_tres_archivos(
    ruta_entrada: Path,
    ruta_salida_dir: Optional[Path] = None,
    sufijo_last: str = ".Last.txt",
    sufijo_bid: str = ".Bid.txt",
    sufijo_ask: str = ".Ask.txt",
) -> None:
    """
    Lee un CSV con ticks L1/L2 y genera 3 archivos:
      - <base>.Last.txt   (type=2)
      - <base>.Bid.txt    (type=0)
      - <base>.Ask.txt    (type=1)
    Solo procesa Level == 1. Formato de salida: 'yyyyMMdd HHmmss fffffff;price;volume'
    """
    base_name: str = ruta_entrada.stem
    if ruta_salida_dir is None:
        out_last = ruta_entrada.with_name(f"{base_name}{sufijo_last}")
        out_bid  = ruta_entrada.with_name(f"{base_name}{sufijo_bid}")
        out_ask  = ruta_entrada.with_name(f"{base_name}{sufijo_ask}")
    else:
        ruta_salida_dir.mkdir(parents=True, exist_ok=True)
        out_last = ruta_salida_dir / f"{base_name}{sufijo_last}"
        out_bid  = ruta_salida_dir / f"{base_name}{sufijo_bid}"
        out_ask  = ruta_salida_dir / f"{base_name}{sufijo_ask}"

    # Contadores (diagnóstico opcional)
    total_lineas: int = 0
    lineas_validas: int = 0
    l1_total: int = 0
    l1_bids: int = 0
    l1_asks: int = 0
    l1_trades: int = 0

    with ruta_entrada.open('r', newline='', encoding='utf-8') as fin, \
         out_last.open('w', newline='', encoding='utf-8') as fout_last, \
         out_bid.open('w', newline='', encoding='utf-8') as fout_bid, \
         out_ask.open('w', newline='', encoding='utf-8') as fout_ask:

        escribir_last = fout_last.write
        escribir_bid  = fout_bid.write
        escribir_ask  = fout_ask.write

        for campos in iter_csv_lineas(fin):
            total_lineas += 1

            parsed = parse_line_campos(campos)
            if parsed is None:
                continue  # línea inválida

            lineas_validas += 1
            ts20, level, tipo, price_str, volume_str = parsed

            # Filtrar SOLO Level 1
            if level != 1:
                continue
            l1_total += 1

            # Convertir timestamp a partes NT
            fecha, hora, f7 = ts20_to_nt_parts(ts20)

            # Según el tipo, escribir en el archivo correspondiente
            if tipo == TYPE_LAST:
                # trades
                escribir_last(f"{fecha} {hora} {f7};{price_str};{volume_str}\n")
                l1_trades += 1
                continue

            if tipo == TYPE_BID:
                # mejor bid y su tamaño (volumen del feed)
                escribir_bid(f"{fecha} {hora} {f7};{price_str};{volume_str}\n")
                l1_bids += 1
                continue

            if tipo == TYPE_ASK:
                # mejor ask y su tamaño (volumen del feed)
                escribir_ask(f"{fecha} {hora} {f7};{price_str};{volume_str}\n")
                l1_asks += 1
                continue

            # Otros tipos (DailyVolume, High, Low, etc.) se ignoran
            continue

    # Resumen en consola
    print(
        f"[OK] {ruta_entrada.name} -> "
        f"{out_last.name}, {out_bid.name}, {out_ask.name} | "
        f"líneas={total_lineas}, válidas={lineas_validas}, L1={l1_total}, "
        f"bids={l1_bids}, asks={l1_asks}, trades={l1_trades}"
    )


def convertir_entrada(
    entrada: Path,
    salida: Optional[Path] = None,
) -> None:
    """
    Si 'entrada' es archivo -> genera 3 archivos.
    Si 'entrada' es carpeta -> procesa todos los .csv (no recursivo).
    """
    if entrada.is_file():
        exportar_tres_archivos(entrada, salida)
        return

    if entrada.is_dir():
        if salida is not None:
            salida.mkdir(parents=True, exist_ok=True)
        for csv_path in sorted(entrada.glob("*.csv")):
            exportar_tres_archivos(csv_path, salida)
        return

    raise FileNotFoundError(f"No existe: {entrada}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exportar tres archivos Tick (sub-second) por cada CSV: <base>.Last.txt, <base>.Bid.txt, <base>.Ask.txt (solo Level 1)."
    )
    parser.add_argument("--in", dest="inp", required=True, help="Ruta a archivo CSV o carpeta con CSVs.")
    parser.add_argument("--out", dest="out", default=None, help="Ruta de salida (carpeta).")

    args = parser.parse_args()

    ruta_in: Path = Path(args.inp).expanduser().resolve()
    ruta_out: Optional[Path] = Path(args.out).expanduser().resolve() if args.out else None

    if ruta_out is not None and ruta_in.is_dir():
        ruta_out.mkdir(parents=True, exist_ok=True)

    convertir_entrada(
        entrada=ruta_in,
        salida=ruta_out,
    )


if __name__ == "__main__":
    main()
