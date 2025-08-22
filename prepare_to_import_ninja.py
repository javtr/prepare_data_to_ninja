#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera un único archivo por contrato con extensión ".Last.txt",
cuyo CONTENIDO es el formato Tick Replay (All):
    yyyyMMdd HHmmss ffffffff;last;bid;ask;volume

- Solo emite en eventos L1 de tipo LAST (trades).
- Usa los últimos Bid/Ask conocidos (L1) y fuerza bid <= last <= ask al emitir.
- Nombres de salida por contrato: "<SYMBOL> MM-YY.Last.txt"
- Rollover: segundo viernes de Mar/Jun/Sep/Dic.

Estructura típica de entrada:
  <root>/
    ES_T2_202408/
      20240801.csv
      20240802.csv
    ES_T2_202409/
      20240903.csv
      ...

Cada CSV (un día) contiene líneas separadas por ';' con campos:
  L1: timestamp20;level;type;price;volume
  L2: timestamp20;level;type;price;volume;action;depth  (se ignora por level != 1)
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterator, Optional, TextIO, Tuple, List

# =========================
# Constantes de tipos L1
# =========================
TYPE_BID: int = 0
TYPE_ASK: int = 1
TYPE_LAST: int = 2

# =========================
# Utilidades de parsing
# =========================

def parse_line_fields(campos: List[str]) -> Optional[Tuple[str, int, int, str, str]]:
    """
    Intenta leer (timestamp20, level, type, price_str, volume_str).
    Devuelve None si no cumple con la estructura esperada.
    """
    if len(campos) not in (5, 7):
        return None
    try:
        timestamp_raw: str = campos[0].strip()
        if len(timestamp_raw) != 20 or not timestamp_raw.isdigit():
            return None
        level: int = int(campos[1])
        dato_tipo: int = int(campos[2])
        price_str: str = campos[3].strip()
        volume_str: str = campos[4].strip()
        return (timestamp_raw, level, dato_tipo, price_str, volume_str)
    except Exception:
        return None


def ts20_to_nt_parts(ts20: str) -> Tuple[str, str, str]:
    """
    Convierte 'YYYYMMDDhhmmssffffff' (20 dígitos) a:
        fecha -> 'yyyyMMdd'
        hora  -> 'HHmmss'
        frac7 -> 'fffffff' (7 dígitos: tomamos 6 micros y agregamos '0')
    """
    fecha: str = ts20[:8]
    hora: str = ts20[8:14]
    micros6: str = ts20[14:20]
    frac7: str = micros6 + "0"
    return fecha, hora, frac7


def iter_csv_lineas(fp: TextIO) -> Iterator[List[str]]:
    lector = csv.reader(fp, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    for campos in lector:
        if not campos:
            continue
        yield campos


# =========================
# Rollover / contrato front
# =========================

def second_friday(year: int, month: int) -> date:
    d0 = date(year, month, 1)
    # Monday=0...Friday=4
    offset = (4 - d0.weekday()) % 7
    first_friday = d0 + timedelta(days=offset)
    return first_friday + timedelta(days=7)


@dataclass(frozen=True)
class ContractKey:
    symbol: str
    month: int  # 3, 6, 9, 12
    year: int   # p. ej., 2024

    @property
    def label(self) -> str:
        return f"{self.month:02d}-{self.year % 100:02d}"

    def outfile(self, out_dir: Path) -> Path:
        # Único archivo por contrato: "<SYMBOL> MM-YY.Last.txt"
        return out_dir / f"{self.symbol} {self.label}.Last.txt"


def front_contract_for_date(d: date) -> Tuple[int, int]:
    """
    Dada una fecha 'd', retorna (mes_contrato, año_contrato) del front
    con rollover el segundo viernes de Mar/Jun/Sep/Dic.
    """
    r_mar = second_friday(d.year, 3)
    r_jun = second_friday(d.year, 6)
    r_sep = second_friday(d.year, 9)
    r_dec = second_friday(d.year, 12)
    if d < r_mar:
        return 3, d.year
    elif d < r_jun:
        return 6, d.year
    elif d < r_sep:
        return 9, d.year
    elif d < r_dec:
        return 12, d.year
    else:
        return 3, d.year + 1


# =========================
# Inferencias desde rutas
# =========================

_RE_FOLDER = re.compile(r"^([A-Za-z0-9]+)_T[12]_\d{6}$")   # p.ej. ES_T2_202408
_RE_FILE_DATE = re.compile(r"^(\d{8})\.csv$")              # p.ej. 20240801.csv


def infer_symbol_from_path(csv_path: Path, forced_symbol: Optional[str]) -> str:
    if forced_symbol:
        return forced_symbol
    for p in (csv_path.parent, csv_path.parent.parent):
        m = _RE_FOLDER.match(p.name)
        if m:
            return m.group(1)
    raise ValueError(
        f"No se pudo inferir el símbolo desde '{csv_path}'. Usa --symbol para indicarlo manualmente."
    )


def infer_trade_date_from_filename(csv_path: Path) -> date:
    m = _RE_FILE_DATE.match(csv_path.name)
    if not m:
        raise ValueError(f"Nombre inválido (debe ser 'YYYYMMDD.csv'): '{csv_path.name}'")
    yyyymmdd = m.group(1)
    return date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


# =========================
# Escritor por contrato + estado (bid/ask) para emitir .Last (All-format)
# =========================

class ContractWriters:
    """
    Mantiene abierto (append) un único archivo por contrato (".Last.txt"),
    y estado de último Bid/Ask/Last para emitir en formato Tick Replay (All)
    SOLO cuando llega un tick Last. Se fuerza bid <= last <= ask.
    """
    def __init__(self) -> None:
        self.handles: Dict[ContractKey, TextIO] = {}
        self.state: Dict[ContractKey, Dict[str, Optional[str]]] = {}

    def __enter__(self) -> "ContractWriters":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def get_writer(self, ck: ContractKey, out_dir: Path) -> TextIO:
        if ck not in self.handles:
            out_dir.mkdir(parents=True, exist_ok=True)
            self.handles[ck] = ck.outfile(out_dir).open("a", newline="", encoding="utf-8")
        return self.handles[ck]

    def get_state(self, ck: ContractKey) -> Dict[str, Optional[str]]:
        st = self.state.get(ck)
        if st is None:
            st = {"last": None, "bid": None, "ask": None}
            self.state[ck] = st
        return st

    def close(self) -> None:
        for fp in list(self.handles.values()):
            try:
                fp.flush()
                fp.close()
            except Exception:
                pass
        self.handles.clear()
        self.state.clear()


# =========================
# Helpers para clamp bid/ask
# =========================

def clamp_bbo_with_last(last_str: str, bid_str: str, ask_str: str) -> Tuple[str, str, bool, bool]:
    """
    Garantiza:
      Decimal(bid_out) <= Decimal(last_str) <= Decimal(ask_out)
    Devuelve (bid_out, ask_out, clamp_bid, clamp_ask).
    Si falla parsing decimal, retorna valores originales sin clamp.
    """
    try:
        d_last = Decimal(last_str)
        d_bid  = Decimal(bid_str)
        d_ask  = Decimal(ask_str)
    except (InvalidOperation, ValueError):
        return bid_str, ask_str, False, False

    clamp_bid = False
    clamp_ask = False

    if d_bid > d_last:
        # usar exactamente last_str al clamped
        bid_out = last_str
        clamp_bid = True
    else:
        bid_out = bid_str

    if d_ask < d_last:
        ask_out = last_str
        clamp_ask = True
    else:
        ask_out = ask_str

    return bid_out, ask_out, clamp_bid, clamp_ask


# =========================
# Procesamiento de un CSV
# =========================

def export_csv_day_to_contract_last_allformat(
    csv_path: Path,
    writers: ContractWriters,
    out_dir: Path,
    forced_symbol: Optional[str] = None,
) -> Tuple[int, int, int, int, int]:
    """
    Procesa un CSV (un día) y escribe SOLO en "<SYMBOL> MM-YY.Last.txt" (formato All).
    Retorna contadores: (total_lineas, lineas_validas, l1_total, l1_trades_emitidos, clamps_aplicados)
    """
    symbol: str = infer_symbol_from_path(csv_path, forced_symbol)
    trading_day: date = infer_trade_date_from_filename(csv_path)
    c_month, c_year = front_contract_for_date(trading_day)
    ck = ContractKey(symbol=symbol, month=c_month, year=c_year)

    total_lineas = 0
    lineas_validas = 0
    l1_total = 0
    l1_trades_emitidos = 0
    clamps_aplicados = 0

    fout = writers.get_writer(ck, out_dir)
    escribir = fout.write
    st = writers.get_state(ck)  # {'last':..., 'bid':..., 'ask':...}

    with csv_path.open("r", newline="", encoding="utf-8") as fin:
        for campos in iter_csv_lineas(fin):
            total_lineas += 1

            parsed = parse_line_fields(campos)
            if parsed is None:
                continue

            lineas_validas += 1
            ts20, level, tipo, price_str, volume_str = parsed

            if level != 1:
                continue

            l1_total += 1
            fecha, hora, frac7 = ts20_to_nt_parts(ts20)

            if tipo == TYPE_BID:
                st["bid"] = price_str
                continue

            if tipo == TYPE_ASK:
                st["ask"] = price_str
                continue

            if tipo == TYPE_LAST:
                st["last"] = price_str
                # Emite solo si ya hay bid y ask conocidos
                if st["bid"] is not None and st["ask"] is not None:
                    bid_out, ask_out, did_cb, did_ca = clamp_bbo_with_last(price_str, st["bid"], st["ask"])
                    if did_cb or did_ca:
                        clamps_aplicados += 1
                    escribir(f"{fecha} {hora} {frac7};{price_str};{bid_out};{ask_out};{volume_str}\n")
                    l1_trades_emitidos += 1
                continue

            # Otros tipos (DailyVolume, High, Low, etc.) se ignoran

    print(
        f"[OK] {csv_path.name} -> {ck.symbol} {ck.label} .Last(TickReplay) | "
        f"líneas={total_lineas}, válidas={lineas_validas}, L1={l1_total}, "
        f"trades_emitidos={l1_trades_emitidos}, clamps={clamps_aplicados}"
    )

    return total_lineas, lineas_validas, l1_total, l1_trades_emitidos, clamps_aplicados


# =========================
# Descubrimiento de entradas
# =========================

def discover_csvs(path_in: Path, recursive: bool) -> Iterator[Path]:
    if path_in.is_file():
        if path_in.suffix.lower() == ".csv":
            yield path_in
        return
    if path_in.is_dir():
        pattern = "**/*.csv" if recursive else "*.csv"
        for p in sorted(path_in.glob(pattern)):
            yield p
        return
    raise FileNotFoundError(f"No existe: {path_in}")


# =========================
# CLI / main
# =========================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convierte L1 ticks a un ÚNICO archivo por contrato '<SYMBOL> MM-YY.Last.txt' "
            "cuyo contenido es Tick Replay (last;bid;ask;volume). Emite solo en trades y "
            "fuerza bid<=last<=ask."
        )
    )
    parser.add_argument("--in", dest="inp", required=True,
                        help="Ruta a archivo CSV, carpeta con CSVs, o raíz que contiene subcarpetas.")
    parser.add_argument("--out", dest="out", required=True,
                        help="Carpeta de salida donde se escribirán los archivos por contrato.")
    parser.add_argument("--no-recursive", dest="no_recursive", action="store_true",
                        help="No buscar recursivamente (*.csv). Por defecto busca recursivo.")
    parser.add_argument("--symbol", dest="symbol", default=None,
                        help="Forzar símbolo si no se puede inferir (p. ej., ES).")

    args = parser.parse_args()

    ruta_in: Path = Path(args.inp).expanduser().resolve()
    ruta_out: Path = Path(args.out).expanduser().resolve()
    ruta_out.mkdir(parents=True, exist_ok=True)

    recursive: bool = not args.no_recursive
    forced_symbol: Optional[str] = args.symbol

    total_archivos = 0
    total_lineas = 0
    total_validas = 0
    total_l1 = 0
    total_trades_emitidos = 0
    total_clamps = 0

    csv_paths: List[Path] = list(discover_csvs(ruta_in, recursive=recursive))
    if not csv_paths:
        print("[INFO] No se encontraron CSVs para procesar.")
        return

    with ContractWriters() as writers:
        for csv_path in csv_paths:
            try:
                total_archivos += 1
                c_lineas, c_validas, c_l1, c_emitidos, c_clamps = export_csv_day_to_contract_last_allformat(
                    csv_path=csv_path,
                    writers=writers,
                    out_dir=ruta_out,
                    forced_symbol=forced_symbol,
                )
                total_lineas += c_lineas
                total_validas += c_validas
                total_l1 += c_l1
                total_trades_emitidos += c_emitidos
                total_clamps += c_clamps
            except Exception as e:
                print(f"[WARN] Saltando '{csv_path}': {e}")

    print(
        "\n==== RESUMEN ====\n"
        f"CSV procesados          : {total_archivos}\n"
        f"Líneas totales          : {total_lineas}\n"
        f"Líneas válidas          : {total_validas}\n"
        f"L1 total                : {total_l1}\n"
        f"Trades emitidos (.Last) : {total_trades_emitidos}\n"
        f"Clamps (ajustes BBO)    : {total_clamps}"
    )


if __name__ == "__main__":
    main()
