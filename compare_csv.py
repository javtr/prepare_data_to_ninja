#!/usr/bin/env python3
# compare_csv.py
# Uso típico:
#   python compare_csv.py "A.csv" "B.csv" --skip-leading-cols 1 --tolerance 1e-6
#   python compare_csv.py "A.csv" "B.csv" --skip-leading-cols 1 --delimiter ";" --output diffs.csv
#
# Columnas (por defecto, después de saltar la(s) inicial(es)):
#   0=Bid, 1=Ask, 2=Last, 3=Volume
#   Puedes ajustarlas con --bid-col, --ask-col, --last-col, --vol-col

from __future__ import annotations
import argparse
import csv
from itertools import zip_longest
from typing import Optional, Tuple, List

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compara dos CSV línea a línea e informa diferencias. Además clasifica volumen por lado (Bid/Ask) por archivo."
    )
    parser.add_argument("file_a", help="Ruta al primer CSV")
    parser.add_argument("file_b", help="Ruta al segundo CSV")

    parser.add_argument("--delimiter", default=",", help="Delimitador del CSV (por defecto ',')")
    parser.add_argument("--tolerance", type=float, default=0.0,
                        help="Tolerancia para comparar números (|Δ| <= tolerance). 0 = exacto")
    parser.add_argument("--max-print", type=int, default=50,
                        help="Máximo de diferencias a mostrar en consola (por defecto 50)")
    parser.add_argument("--output", default=None,
                        help="Ruta de salida para guardar TODAS las diferencias en CSV")
    parser.add_argument("--ignore-blank-lines", action="store_true",
                        help="Ignorar líneas completamente en blanco en ambos archivos")
    parser.add_argument("--fail-on-diff", action="store_true",
                        help="Devuelve código de salida 1 si hay diferencias")
    parser.add_argument("--skip-leading-cols", type=int, default=0,
                        help="Número de columnas iniciales a ignorar (ej. 1 para ignorar timestamp)")

    # Mapeo de columnas (tras aplicar skip-leading-cols). Predeterminado: Bid, Ask, Last, Volume
    parser.add_argument("--bid-col", type=int, default=0, help="Índice de columna de Bid (relativo a la parte efectiva)")
    parser.add_argument("--ask-col", type=int, default=1, help="Índice de columna de Ask (relativo a la parte efectiva)")
    parser.add_argument("--last-col", type=int, default=2, help="Índice de columna de Last (relativo a la parte efectiva)")
    parser.add_argument("--vol-col", type=int, default=3, help="Índice de columna de Volume (relativo a la parte efectiva)")
    return parser.parse_args()

def is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False

def to_float(value: str) -> Optional[float]:
    try:
        return float(value.strip())
    except Exception:
        return None

def compare_fields(val_a: str, val_b: str, tolerance: float) -> Tuple[bool, Optional[float]]:
    a = val_a.strip()
    b = val_b.strip()
    if a == b:
        return True, 0.0 if (is_float(a) and is_float(b)) else None
    if is_float(a) and is_float(b):
        diff = abs(float(a) - float(b))
        return (diff <= tolerance), diff
    return False, None

def write_diff(
    writer: Optional[csv.writer],
    line_no: int,
    original_col_one_based: Optional[int],
    kind: str,
    value_a: Optional[str],
    value_b: Optional[str],
    abs_diff: Optional[float]
) -> None:
    if writer is None:
        return
    writer.writerow([
        line_no,
        "" if original_col_one_based is None else original_col_one_based,
        kind,
        "" if value_a is None else value_a,
        "" if value_b is None else value_b,
        "" if abs_diff is None else f"{abs_diff:.12g}",
    ])

def classify_row(
    row_eff: List[str],
    bid_col: int,
    ask_col: int,
    last_col: int,
    vol_col: int,
    tolerance: float
) -> Tuple[str, float]:
    """
    Clasifica una fila en 'bid', 'ask', 'both', 'none' y devuelve (clase, volume).
    Usa igualdad con tolerancia para comparar Last con Bid/Ask.
    Si faltan columnas o volumen inválido -> ('none', 0.0)
    """
    # Verificar índices dentro de rango
    needed = [bid_col, ask_col, last_col, vol_col]
    if any(c < 0 or c >= len(row_eff) for c in needed):
        return "none", 0.0

    bid = to_float(row_eff[bid_col])
    ask = to_float(row_eff[ask_col])
    last = to_float(row_eff[last_col])
    vol  = to_float(row_eff[vol_col])

    if bid is None or ask is None or last is None or vol is None:
        return "none", 0.0

    match_bid = abs(last - bid) <= tolerance
    match_ask = abs(last - ask) <= tolerance

    if match_bid and match_ask:
        return "both", vol
    if match_bid:
        return "bid", vol
    if match_ask:
        return "ask", vol
    return "none", vol

def compare_csvs(
    path_a: str,
    path_b: str,
    delimiter: str,
    tolerance: float,
    max_print: int,
    output_path: Optional[str],
    ignore_blank_lines: bool,
    skip_leading_cols: int,
    bid_col: int,
    ask_col: int,
    last_col: int,
    vol_col: int
) -> int:
    total_lines_compared: int = 0
    total_differences: int = 0
    printed_differences: int = 0

    # Acumuladores por archivo
    stats = {
        "A": {"bid_vol": 0.0, "ask_vol": 0.0, "both_vol": 0.0, "none_vol": 0.0,
              "bid_cnt": 0, "ask_cnt": 0, "both_cnt": 0, "none_cnt": 0, "invalid_rows": 0},
        "B": {"bid_vol": 0.0, "ask_vol": 0.0, "both_vol": 0.0, "none_vol": 0.0,
              "bid_cnt": 0, "ask_cnt": 0, "both_cnt": 0, "none_cnt": 0, "invalid_rows": 0},
    }

    output_writer: Optional[csv.writer] = None
    output_file_handle = None
    if output_path:
        output_file_handle = open(output_path, "w", newline="", encoding="utf-8")
        output_writer = csv.writer(output_file_handle)
        output_writer.writerow(["line_number", "column_original_csv", "type", "value_file_a", "value_file_b", "abs_diff"])

    with open(path_a, "r", encoding="utf-8", newline="") as fa, \
         open(path_b, "r", encoding="utf-8", newline="") as fb:

        reader_a = csv.reader(fa, delimiter=delimiter)
        reader_b = csv.reader(fb, delimiter=delimiter)

        for line_no, (row_a, row_b) in enumerate(zip_longest(reader_a, reader_b, fillvalue=None), start=1):

            # ---- Clasificación por archivo (independiente de la comparación) ----
            def process_one(tag: str, row: Optional[List[str]]):
                if row is None:
                    return
                # Ignorar líneas en blanco si se solicita
                if ignore_blank_lines and len(row) == 1 and row[0].strip() == "":
                    return
                eff = row[skip_leading_cols:] if len(row) > skip_leading_cols else []
                cls, vol = classify_row(eff, bid_col, ask_col, last_col, vol_col, tolerance)
                if cls == "bid":
                    stats[tag]["bid_vol"] += vol
                    stats[tag]["bid_cnt"] += 1
                elif cls == "ask":
                    stats[tag]["ask_vol"] += vol
                    stats[tag]["ask_cnt"] += 1
                elif cls == "both":
                    stats[tag]["both_vol"] += vol
                    stats[tag]["both_cnt"] += 1
                elif cls == "none":
                    stats[tag]["none_vol"] += vol
                    stats[tag]["none_cnt"] += 1
                else:
                    stats[tag]["invalid_rows"] += 1

            process_one("A", row_a)
            process_one("B", row_b)

            # ---- Lógica de comparación línea a línea (como antes) ----
            if row_a is None:
                total_differences += 1
                if printed_differences < max_print:
                    print(f"[Línea {line_no}] Fila faltante en A; en B hay datos.")
                    printed_differences += 1
                write_diff(output_writer, line_no, None, "missing_in_A", None, ",".join(row_b) if row_b else "", None)
                continue

            if row_b is None:
                total_differences += 1
                if printed_differences < max_print:
                    print(f"[Línea {line_no}] Fila faltante en B; en A hay datos.")
                    printed_differences += 1
                write_diff(output_writer, line_no, None, "missing_in_B", ",".join(row_a) if row_a else "", None, None)
                continue

            # Ignorar líneas en blanco si se solicita
            if ignore_blank_lines:
                if len(row_a) == 1 and row_a[0].strip() == "":
                    row_a = []
                if len(row_b) == 1 and row_b[0].strip() == "":
                    row_b = []
                if row_a == [] and row_b == []:
                    continue

            total_lines_compared += 1

            eff_a: List[str] = row_a[skip_leading_cols:] if len(row_a) > skip_leading_cols else []
            eff_b: List[str] = row_b[skip_leading_cols:] if len(row_b) > skip_leading_cols else []

            len_a_eff: int = len(eff_a)
            len_b_eff: int = len(eff_b)
            max_len_eff: int = max(len_a_eff, len_b_eff)

            if len_a_eff != len_b_eff:
                total_differences += 1
                if printed_differences < max_print:
                    print(f"[Línea {line_no}] Diferente número de columnas tras ignorar {skip_leading_cols}: A={len_a_eff}, B={len_b_eff}")
                    printed_differences += 1
                write_diff(output_writer, line_no, None, "column_count_mismatch_after_skip",
                           ",".join(eff_a), ",".join(eff_b), None)

            for col_idx_eff in range(max_len_eff):
                val_a = eff_a[col_idx_eff] if col_idx_eff < len_a_eff else ""
                val_b = eff_b[col_idx_eff] if col_idx_eff < len_b_eff else ""
                equal, abs_diff = compare_fields(val_a, val_b, tolerance)
                if not equal:
                    total_differences += 1
                    original_col_one_based: int = skip_leading_cols + col_idx_eff + 1
                    if printed_differences < max_print:
                        if abs_diff is not None:
                            print(f"[Línea {line_no}, Col {original_col_one_based}] Diferencia numérica: A='{val_a}' B='{val_b}' | |Δ|={abs_diff:.12g}")
                        else:
                            print(f"[Línea {line_no}, Col {original_col_one_based}] Diferencia texto: A='{val_a}' B='{val_b}'")
                        printed_differences += 1
                    write_diff(output_writer, line_no, original_col_one_based, "value_mismatch", val_a, val_b, abs_diff)

    if output_file_handle:
        output_file_handle.close()

    # ---- Resumen comparativo + resumen por lados ----
    print("\n==================== RESUMEN COMPARACIÓN ====================")
    print(f" Líneas comparadas       : {total_lines_compared}")
    print(f" Diferencias encontradas : {total_differences}")
    if output_path:
        print(f" Reporte completo guardado en: {output_path}")

    def print_stats(tag: str):
        s = stats[tag]
        print(f"\n---- Archivo {tag} ----")
        print(f"  Ticks clasificados (bid/ask/both/none): {s['bid_cnt']}/{s['ask_cnt']}/{s['both_cnt']}/{s['none_cnt']}")
        print(f"  Volumen Bid (puro) : {s['bid_vol']}")
        print(f"  Volumen Ask (puro) : {s['ask_vol']}")
        print(f"  Volumen BOTH       : {s['both_vol']}  (cuando Last≈Bid≈Ask)")
        print(f"  Volumen NONE       : {s['none_vol']}  (cuando Last no coincide con Bid ni Ask)")
        print(f"  Totales por lado (incl. BOTH):  Bid={s['bid_vol'] + s['both_vol']} | Ask={s['ask_vol'] + s['both_vol']}")
        if s['invalid_rows'] > 0:
            print(f"  Filas inválidas (parse/índices): {s['invalid_rows']}")

    print_stats("A")
    print_stats("B")

    # Código de salida: 0 si sin diferencias; 1 si hubo diferencias
    return 0 if total_differences == 0 else 1

def main() -> None:
    args = parse_args()
    exit_code: int = compare_csvs(
        path_a=args.file_a,
        path_b=args.file_b,
        delimiter=args.delimiter,
        tolerance=args.tolerance,
        max_print=args.max_print,
        output_path=args.output,
        ignore_blank_lines=args.ignore_blank_lines,
        skip_leading_cols=args.skip_leading_cols,
        bid_col=args.bid_col,
        ask_col=args.ask_col,
        last_col=args.last_col,
        vol_col=args.vol_col
    )
    if args.fail_on_diff and exit_code != 0:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
