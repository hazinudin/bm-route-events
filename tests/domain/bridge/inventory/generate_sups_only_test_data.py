"""
Script to generate test data for superstructure-only update feature.
Fetches real data from the database and creates partial payloads.
"""

import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import create_engine
from route_events.bridge.inventory import BridgeInventory
from dotenv import load_dotenv
import polars as pl
import json


# Custom JSON encoder to handle Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# Load test environment variables (contains Oracle DB credentials)
load_dotenv("tests/dev.env")

# Database connection - use GDB_HOST from dev.env
HOST = os.getenv("GDB_HOST")
USER = os.getenv("MISC_USER")
PWD = os.getenv("MISC_PWD")

if not HOST:
    print("ERROR: GDB_HOST not found in tests/dev.env")
    print("Please ensure tests/dev.env contains the Oracle database host")
    exit(1)

if not USER or not PWD:
    print("ERROR: MISC_USER or MISC_PWD not found in tests/dev.env")
    exit(1)

print(f"Connecting to Oracle database at {HOST}:1521/geodbbm")
print(f"User: {USER}")

# Create engine
engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

# Test bridge IDs to fetch - these bridges have complete inventory data in production
test_bridge_ids = [
    "0100005",  # 2 spans, year 2025
    "0100006",  # 1 span, year 2025
    "0100012",  # 1 span, year 2025
    "0100017",  # 3 spans, year 2025 (good for testing multi-span)
    "0100019",  # 1 span, year 2025
]


def generate_sups_only_payload(profile_df, sups_df) -> dict:
    """
    Generate a superstructure-only update payload from database data.
    This creates a partial payload with only the 5 superstructure columns + 4 profile fields.
    """
    # Extract profile fields from DataFrame
    profile_row = profile_df.row(0, named=True)

    profile_data = {
        "id_jbt": profile_row["BRIDGE_ID"],
        "pjg_total": float(profile_row["BRIDGE_LENGTH"])
        if profile_row["BRIDGE_LENGTH"]
        else 0.0,
        "cons_year": int(profile_row["CONS_YEAR"])
        if profile_row.get("CONS_YEAR")
        else 2025,
        "tipe_ba_utama": profile_row["MAIN_SPAN_TYPE"] or "",
    }

    # Extract superstructure data (only the 5 columns from partial payload)
    bangunan_atas = []
    for span_row in sups_df.iter_rows(named=True):
        span_data = {
            "no_btg": int(span_row["SPAN_NUMBER"]),
            "tipe_btg": span_row["SPAN_TYPE"],
            "seq_btg": int(span_row["SPAN_SEQ"]),
            "struktur_ba": span_row["SUPERSTRUCTURE"],
            "pjg_btg": float(span_row["SPAN_LENGTH"])
            if span_row["SPAN_LENGTH"]
            else 0.0,
        }
        bangunan_atas.append(span_data)

    payload = {
        "id_jbt": profile_data["id_jbt"],
        "pjg_total": profile_data["pjg_total"],
        "cons_year": profile_data["cons_year"],
        "tipe_ba_utama": profile_data["tipe_ba_utama"],
        "bangunan_atas": bangunan_atas,
    }

    return payload


def generate_test_data_for_bridge(
    bridge_id: str, inv_year: int, output_dir: str = "tests/domain/bridge/inventory"
):
    """
    Fetch inventory data for a bridge and generate test data files.
    """
    print(f"\n{'=' * 60}")
    print(f"Processing bridge: {bridge_id} (year {inv_year})")
    print("=" * 60)

    _where = f"where BRIDGE_ID = '{bridge_id}' and INV_YEAR = {inv_year}"

    # Query profile
    print(f"  Querying NAT_BRIDGE_PROFILE...")
    profile_query = f"select * from NAT_BRIDGE_PROFILE {_where}"
    profile_df = pl.read_database(profile_query, connection=engine)

    if len(profile_df) == 0:
        print(f"  No profile data found")
        return None

    print(f"  ✓ Found profile data")

    # Query spans
    print(f"  Querying NAT_BRIDGE_SPAN...")
    sups_query = f"select * from NAT_BRIDGE_SPAN {_where}"
    sups_df = pl.read_database(sups_query, connection=engine)

    if len(sups_df) == 0:
        print(f"  No span data found")
        return None

    print(f"  ✓ Found {len(sups_df)} span(s)")

    # Generate sups-only payload
    sups_only_payload = generate_sups_only_payload(profile_df, sups_df)

    # Save to files
    os.makedirs(output_dir, exist_ok=True)

    # Save sups-only payload
    sups_only_file = f"{output_dir}/sups_only_payload_{bridge_id}_{inv_year}.json"
    with open(sups_only_file, "w") as f:
        json.dump(sups_only_payload, f, indent=2)
    print(f"  ✓ Saved sups-only payload: {sups_only_file}")

    # Save full inventory data (for reference/comparison)
    full_data_file = f"{output_dir}/full_inventory_{bridge_id}_{inv_year}.json"

    # Convert to dict format similar to invij
    profile_row = profile_df.row(0, named=True)
    full_data = {
        "mode": "update",
        "val_history": [],
        "id_jbt": profile_row["BRIDGE_ID"],
        "tanggal_inv": profile_row["INV_DATE"].strftime("%d/%m/%Y")
        if profile_row.get("INV_DATE")
        else "01/01/2025",
        "tahun_inv": str(inv_year),
        "linkid": profile_row.get("LINKID", ""),
        "longitude": float(profile_row["LONGITUDE"])
        if profile_row.get("LONGITUDE")
        else 0.0,
        "latitude": float(profile_row["LATITUDE"])
        if profile_row.get("LATITUDE")
        else 0.0,
        "pjg_total": float(profile_row["BRIDGE_LENGTH"])
        if profile_row["BRIDGE_LENGTH"]
        else 0.0,
        "lbr_jembatan": float(profile_row["BRIDGE_WIDTH"])
        if profile_row.get("BRIDGE_WIDTH")
        else 0.0,
        "btg_terpanjang": max(sups_df["SPAN_LENGTH"].to_list())
        if len(sups_df) > 0
        else 0.0,
        "tipe_lintasan": profile_row.get("CROSS_TYPE", "S"),
        "tipe_struktur": "Jembatan",
        "tipe_ba_utama": profile_row.get("MAIN_SPAN_TYPE", ""),
        "lokasi_dari": "",
        "lokasi_km": 0,
        "tahun_bangun": int(profile_row["CONS_YEAR"])
        if profile_row.get("CONS_YEAR")
        else inv_year,
        "jml_bentang": len(sups_df),
        "no_jbt": profile_row.get("BRIDGE_NUM", ""),
        "bangunan_atas": [],
        "bangunan_bawah": [],
    }

    # Add superstructure data with all columns
    for span_row in sups_df.iter_rows(named=True):
        span_data = {
            "no_btg": int(span_row["SPAN_NUMBER"]),
            "tipe_btg": span_row["SPAN_TYPE"].lower(),
            "seq_btg": int(span_row["SPAN_SEQ"]),
            "pjg_btg": float(span_row["SPAN_LENGTH"])
            if span_row["SPAN_LENGTH"]
            else 0.0,
            "jml_gelagar": int(span_row.get("NUM_GIRDERS", 4))
            if span_row.get("NUM_GIRDERS")
            else 4,
            "radius": float(span_row.get("RADIUS", 0))
            if span_row.get("RADIUS")
            else 0.0,
            "skew": float(span_row.get("SKEW", 0)) if span_row.get("SKEW") else 0.0,
            "lbr_lantai_kend": float(span_row.get("FLOOR_WIDTH", 0))
            if span_row.get("FLOOR_WIDTH")
            else 0.0,
            "lbr_trotoar_kiri": float(span_row.get("SIDEWALK_WIDTH_LEFT", 0))
            if span_row.get("SIDEWALK_WIDTH_LEFT")
            else 0.0,
            "lbr_trotoar_kanan": float(span_row.get("SIDEWALK_WIDTH_RIGHT", 0))
            if span_row.get("SIDEWALK_WIDTH_RIGHT")
            else 0.0,
            "lbr_drainase_kiri": 0.0,
            "lbr_drainase_kanan": 0.0,
            "lbr_bahu_kanan": 0.0,
            "lbr_bahu_kiri": 0.0,
            "lbr_median": 0.0,
            "tinggi_ruang_bebas": 0.0,
            "struktur_ba": span_row["SUPERSTRUCTURE"],
            "elemen": [],
        }
        full_data["bangunan_atas"].append(span_data)

    with open(full_data_file, "w") as f:
        json.dump(full_data, f, indent=2, cls=DecimalEncoder)
    print(f"  ✓ Saved full inventory data: {full_data_file}")

    return {
        "bridge_id": bridge_id,
        "year": inv_year,
        "sups_only_file": sups_only_file,
        "full_data_file": full_data_file,
        "num_spans": len(sups_df),
    }


if __name__ == "__main__":
    print("Generating test data for superstructure-only update feature")
    print("=" * 60)

    results = []
    for bridge_id in test_bridge_ids:
        try:
            # Get available years for this bridge
            years_query = f"select distinct INV_YEAR from NAT_BRIDGE_PROFILE where BRIDGE_ID = '{bridge_id}' order by INV_YEAR desc"
            years_df = pl.read_database(years_query, connection=engine)
            years = years_df["INV_YEAR"].to_list()

            if not years:
                print(f"\n  No available years found for bridge {bridge_id}")
                continue

            # Get the latest year
            latest_year = max(years)

            result = generate_test_data_for_bridge(bridge_id, latest_year)
            if result:
                results.append(result)
        except Exception as e:
            print(f"  Error processing {bridge_id}: {str(e)}")
            import traceback

            traceback.print_exc()

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Successfully generated test data for {len(results)} bridge(s):")
    for r in results:
        print(f"  - Bridge {r['bridge_id']} (year {r['year']}): {r['num_spans']} spans")

    print("\nGenerated files can be found in: tests/domain/bridge/inventory/")
