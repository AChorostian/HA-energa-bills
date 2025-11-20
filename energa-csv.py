import os
import json
import unicodedata
from datetime import datetime

import pdfplumber
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CSV_OUTPUT_FILE = "energa_output.csv"


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def safe_ascii(text: str) -> str:
    if not isinstance(text, str):
        return text
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def extract_pdf_lines(filename: str) -> list[str]:
    start_marker = "WINNO BYC" if "_KES_" in filename else "DANE ODCZYTOWE"
    end_marker = "ROZLICZENIE VAT"

    result = []
    inside_block = False

    with pdfplumber.open(filename) as pdf:
        for page in pdf.pages:
            for entry in page.extract_text_lines():
                line = safe_ascii(entry["text"])

                if start_marker in line.upper():
                    inside_block = True

                if end_marker in line.upper():
                    inside_block = False

                if inside_block:
                    result.append(line)

    return result


def parse_float_token(line: str, index: int) -> float:
    return float(line.split()[index].replace(",", "."))


def add_fee_value(row: dict, line: str, keyword: str, name: str):
    if keyword in line:
        row[name] = round(row[name] + parse_float_token(line, -2), 2)


# ---------------------------------------------------------------------------
# PDF Parsing
# ---------------------------------------------------------------------------

def init_row(filename: str) -> dict:
    parts = filename.split("_")
    faktura_name = f"{parts[2]}_{parts[3][:-4]}"

    return {
        "Faktura": faktura_name,
        "Data od": "",
        "Data do": "",
        "pobranie od": 0.0,
        "pobranie do": 0.0,
        "pobranie kWh": 0.0,
        "pobranie saldo": 0,
        "oddanie od": 0.0,
        "oddanie to": 0.0,
        "oddanie kWh": 0.0,
        "oddanie saldo": 0,
        "sprzedaz": 0.0,
        "sprzedaz_akcyza": 0.0,
        "dystrybucja_abonamentowa": 0.0,
        "dystrybucja_sieciowa_stala": 0.0,
        "dystrybucja_przejsciowa": 0.0,
        "dystrybucja_mocowa": 0.0,
        "dystrybucja_sieciowa_zmienna": 0.0,
        "dystrybucja_jakosciowa": 0.0,
        "dystrybucja_OZE": 0.0,
        "dystrybucja_kogeneracyjna": 0.0,
        "depozyt_wprowadzono": 0.0,
        "depozyt_wprowadzenie_cena": 0.0,
        "depozyt_pobrano": 0.0,
        "depozyt_razem": 0.0,
    }


def parse_sections(lines: list[str], row: dict) -> dict:
    sections = {"Odczyt": [], "Sprzedaz": [], "Dystrybucja": [], "Saldo": []}
    current = "Odczyt"
    saldo_flag = False

    for line in lines:
        if "ROZLICZENIE SPRZEDAZY" in line:
            current = "Sprzedaz"
        elif "ROZLICZENIE DYSTRYBUCJI" in line:
            current = "Dystrybucja"
        elif "ROZLICZENIE ENERGII" in line:
            current = "Saldo"

        if current == "Sprzedaz" and "Energia czynna" in line:
            value = parse_float_token(line, -2)
            if "akcyza" in line:
                row["sprzedaz_akcyza"] += value
            else:
                row["sprzedaz"] += value

        if current == "Dystrybucja":
            add_fee_value(row, line, "Opata abonamentowa", "dystrybucja_abonamentowa")
            add_fee_value(row, line, "Opata sieciowa staa", "dystrybucja_sieciowa_stala")
            add_fee_value(row, line, "Opata przejsciowa", "dystrybucja_przejsciowa")
            add_fee_value(row, line, "Opata mocowa", "dystrybucja_mocowa")
            add_fee_value(row, line, "Opata sieciowa zmienna", "dystrybucja_sieciowa_zmienna")
            add_fee_value(row, line, "Opata jakosciowa", "dystrybucja_jakosciowa")
            add_fee_value(row, line, "Opata OZE", "dystrybucja_OZE")
            add_fee_value(row, line, "Opata kogeneracyjna", "dystrybucja_kogeneracyjna")

        if current == "Saldo":
            if "Suma godzinowych sald ujemnych" in line:
                row["depozyt_wprowadzono"] += parse_float_token(line, -1)

            if "ROZLICZENIE SPRZEDAZY ENERGII POBRANEJ brutto" in line:
                row["depozyt_pobrano"] += parse_float_token(line, -2)

            if "Depozyt energii po rozliczeniu" in line:
                saldo_flag = True

            if "Razem" in line and saldo_flag:
                row["depozyt_razem"] += parse_float_token(line, -2)
                saldo_flag = False

        sections[current].append(line)

    return sections


def parse_odczyt_section(lines: list[str], row: dict):
    read_type = "pobranie "

    for line in lines:
        parts = line.split()

        if "30928304" in line:
            row["Data od"] = parts[3]
            row["Data do"] = parts[4]

            idx = 5
            for suffix in ["od", "do", "kWh"]:
                val = parts[idx]
                if "," in val:
                    row[read_type + suffix] = float(val.replace(",", "."))
                    idx += 1
                else:
                    value = float(val.replace(",", ".")) * 1000 + float(parts[idx + 1].replace(",", "."))
                    row[read_type + suffix] = round(value, 3)
                    idx += 2

        if "oddanie" in line:
            read_type = "oddanie "

        if "Suma godzinowych sald dodatnich" in line:
            row["pobranie saldo"] = int(parts[6]) * 1000 + int(parts[7]) if len(parts) > 7 else int(parts[6])

        if "Suma godzinowych sald ujemnych" in line:
            row["oddanie saldo"] = int(parts[6]) * 1000 + int(parts[7]) if len(parts) > 7 else int(parts[6])

    if row["oddanie saldo"] > 0:
        row["depozyt_wprowadzenie_cena"] = round(row["depozyt_wprowadzono"] / row["oddanie saldo"], 2)

    if row["pobranie saldo"] == 0:
        row["pobranie saldo"] = round(row["pobranie kWh"])

    return row


def format_data(lines: list[str], filename: str) -> dict:
    row = init_row(filename)
    sections = parse_sections(lines, row)
    parse_odczyt_section(sections["Odczyt"], row)
    return row


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def delete_duplicates(rows: list[dict]) -> list[dict]:
    to_remove = []
    for kes in rows:
        if "KES" in kes["Faktura"]:
            for fes in rows:
                if "FES" in fes["Faktura"]:
                    if kes["Data od"] == fes["Data od"] and kes["Data do"] == fes["Data do"]:
                        to_remove.append(fes)

    for x in to_remove:
        if x in rows:
            rows.remove(x)

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_all_pdfs():
    rows = []

    for filename in os.listdir():
        if filename.lower().endswith(".pdf"):
            print(f"Processing: {filename}")
            lines = extract_pdf_lines(filename)
            row = format_data(lines, filename)
            rows.append(row)

    rows = delete_duplicates(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values(by="Data od", key=lambda x: pd.to_datetime(x, dayfirst=True))
    df.columns = df.columns.str.replace(" ", "_")

    return df


if __name__ == "__main__":
    print("Reading PDFs...")
    df = process_all_pdfs()

    print(f"Saving CSV to {CSV_OUTPUT_FILE}...")
    df.to_csv(CSV_OUTPUT_FILE, index=False)

    print("Done!")
