import os
import json
import unicodedata
from datetime import datetime

import pdfplumber
import pandas as pd
import paho.mqtt.publish as publish

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_BROKER = "---ip address of HA---"
MQTT_TOPIC = "home/energa/odczyty"
MQTT_PORT = 1883
MQTT_USER = "---login for mqtt---"
MQTT_PASSWORD = "---password for mqtt---"


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def safe_ascii(text: str) -> str:
    """Convert a string to ASCII, removing accents and unsupported characters."""
    if not isinstance(text, str):
        return text
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def extract_pdf_lines(filename: str) -> list[str]:
    """Extract lines between markers inside the PDF."""
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
    """Safe float parsing with comma support."""
    return float(line.split()[index].replace(",", "."))


def add_fee_value(row: dict, line: str, keyword: str, name: str):
    """Generic handler for fee values."""
    if keyword in line:
        row[name] = round(row[name] + parse_float_token(line, -2), 2)


# ---------------------------------------------------------------------------
# Main PDF Parsing
# ---------------------------------------------------------------------------

def init_row(filename: str) -> dict:
    """Create base output structure for one invoice."""
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
        "oddanie do": 0.0,
        "oddanie kWh": 0.0,
        "oddanie saldo": 0,
        "sprzedaz": 0.0,
        "sprzedaz_akcyza": 0.0,

        # Dystrybucja
        "dystrybucja_abonamentowa": 0.0,
        "dystrybucja_sieciowa_stala": 0.0,
        "dystrybucja_przejsciowa": 0.0,
        "dystrybucja_mocowa": 0.0,
        "dystrybucja_sieciowa_zmienna": 0.0,
        "dystrybucja_jakosciowa": 0.0,
        "dystrybucja_OZE": 0.0,
        "dystrybucja_kogeneracyjna": 0.0,

        # Depozyt
        "depozyt_wprowadzono": 0.0,
        "depozyt_wprowadzenie_cena": 0.0,
        "depozyt_pobrano": 0.0,
        "depozyt_razem": 0.0,
    }


def parse_sections(lines: list[str], row: dict) -> dict:
    """Parse all PDF sections: Odczyt, Sprzedaż, Dystrybucja, Saldo."""
    sections = {"Odczyt": [], "Sprzedaz": [], "Dystrybucja": [], "Saldo": []}
    current = "Odczyt"
    saldo_flag = False

    for line in lines:
        # Detect section switches
        if "ROZLICZENIE SPRZEDAZY" in line:
            current = "Sprzedaz"
        elif "ROZLICZENIE DYSTRYBUCJI" in line:
            current = "Dystrybucja"
        elif "ROZLICZENIE ENERGII" in line:
            current = "Saldo"

        # --- SPRZEDAŻ ---
        if current == "Sprzedaz" and "Energia czynna" in line:
            value = parse_float_token(line, -2)
            if "akcyza" in line:
                row["sprzedaz_akcyza"] += value
            else:
                row["sprzedaz"] += value

        # --- DYSTRYBUCJA ---
        if current == "Dystrybucja":
            add_fee_value(row, line, "Opata abonamentowa", "dystrybucja_abonamentowa")
            add_fee_value(row, line, "Opata sieciowa staa", "dystrybucja_sieciowa_stala")
            add_fee_value(row, line, "Opata przejsciowa", "dystrybucja_przejsciowa")
            add_fee_value(row, line, "Opata mocowa", "dystrybucja_mocowa")
            add_fee_value(row, line, "Opata sieciowa zmienna", "dystrybucja_sieciowa_zmienna")
            add_fee_value(row, line, "Opata jakosciowa", "dystrybucja_jakosciowa")
            add_fee_value(row, line, "Opata OZE", "dystrybucja_OZE")
            add_fee_value(row, line, "Opata kogeneracyjna", "dystrybucja_kogeneracyjna")

        # --- SALDO ---
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
    """Parse Odczyt section for energy meter values."""
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
            items = parts
            row["pobranie saldo"] = int(items[6]) * 1000 + int(items[7]) if len(items) > 7 else int(items[6])

        if "Suma godzinowych sald ujemnych" in line:
            items = parts
            row["oddanie saldo"] = int(items[6]) * 1000 + int(items[7]) if len(items) > 7 else int(items[6])

    if row["oddanie saldo"] > 0:
        row["depozyt_wprowadzenie_cena"] = round(row["depozyt_wprowadzono"] / row["oddanie saldo"], 2)

    if row["pobranie saldo"] == 0:
        row["pobranie saldo"] = round(row["pobranie kWh"])

    return row


def format_data(lines: list[str], filename: str) -> dict:
    """Top-level parser for one PDF file."""
    row = init_row(filename)
    sections = parse_sections(lines, row)
    parse_odczyt_section(sections["Odczyt"], row)
    return row


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def delete_duplicates(rows: list[dict]) -> list[dict]:
    """Remove duplicate FES when matching KES exists."""
    to_remove = []

    for kes in rows:
        if "KES" in kes["Faktura"]:
            for fes in rows:
                if "FES" in fes["Faktura"]:
                    if kes["Data od"] == fes["Data od"] and kes["Data do"] == fes["Data do"]:
                        to_remove.append(fes)

    for item in to_remove:
        if item in rows:
            rows.remove(item)

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def send_mqtt(payload: str):
    publish.single(
        topic=MQTT_TOPIC,
        payload=payload,
        hostname=MQTT_BROKER,
        port=MQTT_PORT,
        auth={"username": MQTT_USER, "password": MQTT_PASSWORD},
        retain=True,
    )


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

    payload = json.dumps({"data": df.to_dict(orient="records")}, separators=(",", ":"))
    return payload


if __name__ == "__main__":
    print("Reading PDFs...")
    payload = process_all_pdfs()

    print("Uploading to MQTT...")
    send_mqtt(payload)
    print("Done!")
