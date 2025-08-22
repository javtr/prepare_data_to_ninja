#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convierte L1 ticks a formato NinjaTrader Tick (sub-second) y agrega por contrato
(front) según la regla de rollover del segundo viernes de marzo/junio/septiembre/diciembre.

Entradas típicas:
  <root>/
    ES_T2_202408/
      20240801.csv
      ...

Cada CSV (un día) contiene líneas separadas por ';' con campos:
  L1: timestamp20;level;type;price;volume
  L2: timestamp20;level;type;price;volume;action;depth  (se ignora por level != 1)

Salidas por contrato:
  <SYMBOL> MM-YY.Last.txt   -> "yyyyMMdd HHmmss ffffffff;price;volume"
  <SYMBOL> MM-YY.Bid.txt    -> idem
  <SYMBOL> MM-YY.Ask.txt    -> idem
  <SYMBOL> MM-YY.All.txt    -> "yyyyMMdd HHmmss ffffffff;last;bid;ask;volume"
                               Solo emite en trades (Last). En .All se fuerza bid<=last<=ask.
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
        frac7 -> 'fffffff' (7 dígitos, tomamos 6 micros y agregamos '0')
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
    year: int   # p.ej., 2024

    @property
    def label(self) -> str:
        return f"{self.month:02d}-{self.year % 100:02d}"

    def outfile(self, out_dir: Path, typ: str) -> Path:
        # typ ∈ {'Last','Bid','Ask','All'}
        return out_dir / f"{self.symbol} {self.label}.{typ}.txt"


def front_contract_for_date(d: date) -> Tuple[int, int]:
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
# Escritores por contrato + estado para .All
# =========================

class ContractWriters:
    """
    Mantiene abiertos (append) archivos Last/Bid/Ask/All por contrato durante toda la corrida.
    También mantiene estado (últimos Bid/Ask/Last conocidos) para emitir .All SOLO en trades,
    corrigiendo bid/ask para que cumplan bid <= last <= ask.
    """
    def __init__(self) -> None:
        self.handles: Dict[Tuple[ContractKey, str], TextIO] = {}
        self.state: Dict[ContractKey, Dict[str, Optional[str]]] = {}

    def __enter__(self) -> "ContractWriters":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _open_handle(self, ck: ContractKey, typ: str, out_dir: Path) -> TextIO:
        key = (ck, typ)
        if key not in self.handles:
            out_dir.mkdir(parents=True, exist_ok=True)
            self.handles[key] = ck.outfile(out_dir, typ).open(
                "a", newline="", encoding="utf-8"
            )
        return self.handles[key]

    def get_writers(self, ck: ContractKey, out_dir: Path) -> Tuple[TextIO, TextIO, TextIO]:
        return (
            self._open_handle(ck, "Last", out_dir),
            self._open_handle(ck, "Bid", out_dir),
            self._open_handle(ck, "Ask", out_dir),
        )

    def get_all_writer(self, ck: ContractKey, out_dir: Path) -> TextIO:
        return self._open_handle(ck, "All", out_dir)

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
# Helpers para .All
# =========================

def clamp_bbo_with_last(last_str: str, bid_str: str, ask_str: str) -> Tuple[str, str, bool, bool]:
    """
    Devuelve (bid_out, ask_out, clamp_bid, clamp_ask) garantizando:
      Decimal(bid_out) <= Decimal(last_str) <= Decimal(ask_out)
    Si hay error en parsing decimal, devuelve originales sin clamp.
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
        d_bid = d_last
        clamp_bid = True
    if d_ask < d_last:
        d_ask = d_last
        clamp_ask = True

    # Convertimos a string sin alterar el formato innecesariamente:
    # si hubo "clamp", usamos exactamente last_str en el lado ajustado.
    bid_out = last_str if clamp_bid else bid_str
    ask_out = last_str if clamp_ask else ask_str
    return bid_out, ask_out, clamp_bid, clamp_ask


# =========================
# Procesamiento de un CSV
# =========================

def export_csv_into_contract_files(
    csv_path: Path,
    writers: ContractWriters,
    out_dir: Path,
    forced_symbol: Optional[str] = None,
) -> Tuple[int, int, int, int, int, int, int, int, int]:
    """
    Procesa un CSV (un día) y lo escribe en los archivos del contrato correspondiente.
    Retorna contadores:
      (total_lineas, lineas_validas, l1_total, l1_bids, l1_asks, l1_trades,
       all_emits, all_clamp_bid, all_clamp_ask)
    """
    symbol: str = infer_symbol_from_path(csv_path, forced_symbol)
    trading_day: date = infer_trade_date_from_filename(csv_path)
    c_month, c_year = front_contract_for_date(trading_day)
    ck = ContractKey(symbol=symbol, month=c_month, year=c_year)

    total_lineas = lineas_validas = l1_total = 0
    l1_bids = l1_asks = l1_trades = 0
    all_emits = all_clamp_bid = all_clamp_ask = 0

    fout_last, fout_bid, fout_ask = writers.get_writers(ck, out_dir)
    fout_all = writers.get_all_writer(ck, out_dir)

    escribir_last = fout_last.write
    escribir_bid  = fout_bid.write
    escribir_ask  = fout_ask.write
    escribir_all  = fout_all.write

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

            if tipo == TYPE_LAST:
                # Archivos "simples"
                escribir_last(f"{fecha} {hora} {frac7};{price_str};{volume_str}\n")
                l1_trades += 1
                # Estado para .All
                st["last"] = price_str

                # Emitir SOLO en trades y solo si ya tenemos Bid y Ask previos
                if st["bid"] is not None and st["ask"] is not None:
                    bid_out, ask_out, did_clamp_bid, did_clamp_ask = clamp_bbo_with_last(
                        price_str, st["bid"], st["ask"]
                    )
                    if did_clamp_bid:
                        all_clamp_bid += 1
                    if did_clamp_ask:
                        all_clamp_ask += 1

                    escribir_all(f"{fecha} {hora} {frac7};{price_str};{bid_out};{ask_out};{volume_str}\n")
                    all_emits += 1

            elif tipo == TYPE_BID:
                escribir_bid(f"{fecha} {hora} {frac7};{price_str};{volume_str}\n")
                l1_bids += 1
                st["bid"] = price_str

            elif tipo == TYPE_ASK:
                escribir_ask(f"{fecha} {hora} {frac7};{price_str};{volume_str}\n")
                l1_asks += 1
                st["ask"] = price_str

            else:
                # Ignorar otros tipos (DailyVolume, High, Low, etc.)
                continue

    print(
        f"[OK] {csv_path.name} -> {ck.symbol} {ck.label} "
        f"(Last/Bid/Ask/All) | líneas={total_lineas}, válidas={lineas_validas}, "
        f"L1={l1_total}, bids={l1_bids}, asks={l1_asks}, trades={l1_trades}, "
        f"all={all_emits}, clamp_bid={all_clamp_bid}, clamp_ask={all_clamp_ask}"
    )

    return (
        total_lineas, lineas_validas, l1_total, l1_bids, l1_asks, l1_trades,
        all_emits, all_clamp_bid, all_clamp_ask
    )


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
            "Convierte L1 ticks a formato NinjaTrader y agrega por contrato front "
            "(segundo viernes de Mar/Jun/Sep/Dic). Genera 4 archivos por contrato: "
            "'<SYMBOL> MM-YY.Last.txt', '.Bid.txt', '.Ask.txt' y '.All.txt' (Tick Replay, "
            "solo trades; fuerza bid<=last<=ask)."
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

    total_archivos = total_lineas = total_validas = 0
    total_l1 = total_bids = total_asks = total_trades = 0
    total_all = total_all_clamp_bid = total_all_clamp_ask = 0

    csv_paths: List[Path] = list(discover_csvs(ruta_in, recursive=recursive))
    if not csv_paths:
        print("[INFO] No se encontraron CSVs para procesar.")
        return

    with ContractWriters() as writers:
        for csv_path in csv_paths:
            try:
                total_archivos += 1
                (c_lineas, c_validas, c_l1, c_bids, c_asks, c_trades,
                 c_all, c_clamp_bid, c_clamp_ask) = export_csv_into_contract_files(
                    csv_path=csv_path,
                    writers=writers,
                    out_dir=ruta_out,
                    forced_symbol=forced_symbol,
                )
                total_lineas += c_lineas
                total_validas += c_validas
                total_l1 += c_l1
                total_bids += c_bids
                total_asks += c_asks
                total_trades += c_trades
                total_all += c_all
                total_all_clamp_bid += c_clamp_bid
                total_all_clamp_ask += c_clamp_ask
            except Exception as e:
                print(f"[WARN] Saltando '{csv_path}': {e}")

    print(
        "\n==== RESUMEN ====\n"
        f"CSV procesados      : {total_archivos}\n"
        f"Líneas totales      : {total_lineas}\n"
        f"Líneas válidas      : {total_validas}\n"
        f"L1 total            : {total_l1}\n"
        f" - Bids             : {total_bids}\n"
        f" - Asks             : {total_asks}\n"
        f" - Trades           : {total_trades}\n"
        f"All emitidos        : {total_all}  (solo en ticks Last)\n"
        f"All clamp (bid>last): {total_all_clamp_bid}\n"
        f"All clamp (ask<last): {total_all_clamp_ask}"
    )


if __name__ == "__main__":
    main()
