"""
Validate generated test data for superstructure-only update feature.
This script checks that all test case files are valid JSON and follow the expected format.
"""

import json
import os
from pathlib import Path


def validate_test_file(file_path):
    """Validate a single test file."""
    print(f"\nValidating: {file_path}")
    print("-" * 60)

    try:
        # Load JSON
        with open(file_path, "r") as f:
            data = json.load(f)
        print(f"✓ Valid JSON")

        # Check required profile fields
        required_profile_fields = ["id_jbt", "pjg_total", "cons_year", "tipe_ba_utama"]
        missing_profile = [f for f in required_profile_fields if f not in data]

        if missing_profile:
            print(f"✗ Missing profile fields: {missing_profile}")
            return False
        else:
            print(f"✓ All required profile fields present")

        # Check profile field types
        if not isinstance(data["id_jbt"], str):
            print(f"✗ id_jbt should be string, got {type(data['id_jbt'])}")
            return False

        if not isinstance(data["pjg_total"], (int, float)):
            print(f"✗ pjg_total should be number, got {type(data['pjg_total'])}")
            return False

        if not isinstance(data["cons_year"], int):
            print(f"✗ cons_year should be integer, got {type(data['cons_year'])}")
            return False

        if not isinstance(data["tipe_ba_utama"], str):
            print(
                f"✗ tipe_ba_utama should be string, got {type(data['tipe_ba_utama'])}"
            )
            return False

        print(f"✓ Profile field types are correct")

        # Check bangunan_atas
        if "bangunan_atas" not in data:
            print(f"✗ Missing bangunan_atas")
            return False

        if not isinstance(data["bangunan_atas"], list):
            print(f"✗ bangunan_atas should be list, got {type(data['bangunan_atas'])}")
            return False

        if len(data["bangunan_atas"]) == 0:
            print(f"✗ bangunan_atas cannot be empty")
            return False

        print(f"✓ bangunan_atas present with {len(data['bangunan_atas'])} span(s)")

        # Check span fields
        required_span_fields = [
            "no_btg",
            "tipe_btg",
            "seq_btg",
            "struktur_ba",
            "pjg_btg",
        ]

        for i, span in enumerate(data["bangunan_atas"]):
            missing_span = [f for f in required_span_fields if f not in span]

            if missing_span:
                print(f"✗ Span {i + 1} missing fields: {missing_span}")
                return False

            # Validate span field types
            if not isinstance(span["no_btg"], int):
                print(f"✗ Span {i + 1} no_btg should be integer")
                return False

            if not isinstance(span["tipe_btg"], str):
                print(f"✗ Span {i + 1} tipe_btg should be string")
                return False

            if span["tipe_btg"] not in ["UTAMA", "KANAN", "KIRI"]:
                print(
                    f"✗ Span {i + 1} tipe_btg '{span['tipe_btg']}' not in domain [UTAMA, KANAN, KIRI]"
                )
                return False

            if not isinstance(span["seq_btg"], int):
                print(f"✗ Span {i + 1} seq_btg should be integer")
                return False

            if not isinstance(span["struktur_ba"], str):
                print(f"✗ Span {i + 1} struktur_ba should be string")
                return False

            if not isinstance(span["pjg_btg"], (int, float)):
                print(f"✗ Span {i + 1} pjg_btg should be number")
                return False

            if span["pjg_btg"] <= 0:
                print(f"✗ Span {i + 1} pjg_btg must be positive")
                return False

        print(f"✓ All span fields are valid")

        # Summary
        print(f"\n✓ VALID TEST CASE")
        print(f"  Bridge ID: {data['id_jbt']}")
        print(f"  Length: {data['pjg_total']}m")
        print(f"  Construction Year: {data['cons_year']}")
        print(f"  Main Span Type: {data['tipe_ba_utama']}")
        print(f"  Number of Spans: {len(data['bangunan_atas'])}")

        for i, span in enumerate(data["bangunan_atas"]):
            print(
                f"    Span {i + 1}: {span['tipe_btg']} #{span['no_btg']} - "
                f"{span['struktur_ba']} ({span['pjg_btg']}m)"
            )

        return True

    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    test_dir = Path("tests/domain/bridge/inventory")
    test_files = sorted(test_dir.glob("sups_only_test_case_*.json"))

    print("=" * 60)
    print("SUPERSTRUCTURE-ONLY UPDATE TEST DATA VALIDATION")
    print("=" * 60)
    print(f"\nFound {len(test_files)} test case files")

    results = []
    for test_file in test_files:
        result = validate_test_file(str(test_file))
        results.append((test_file.name, result))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    valid_count = sum(1 for _, r in results if r)
    invalid_count = len(results) - valid_count

    print(f"\nTotal: {len(results)} test cases")
    print(f"✓ Valid: {valid_count}")
    print(f"✗ Invalid: {invalid_count}")

    if invalid_count > 0:
        print("\nInvalid files:")
        for name, result in results:
            if not result:
                print(f"  - {name}")

    # Check for expected test cases
    expected_cases = [
        "sups_only_test_case_1_valid.json",
        "sups_only_test_case_2_length_change.json",
        "sups_only_test_case_3_year_change.json",
        "sups_only_test_case_4_type_change.json",
        "sups_only_test_case_5_structure_change.json",
        "sups_only_test_case_6_span_length_change.json",
        "sups_only_test_case_7_multiple_changes.json",
        "sups_only_test_case_8_invalid_missing_id.json",
        "sups_only_test_case_9_invalid_missing_length.json",
        "sups_only_test_case_10_invalid_span_mismatch.json",
        "sups_only_test_case_11_invalid_extra_span.json",
        "sups_only_test_case_12_invalid_negative_length.json",
        "sups_only_test_case_13_invalid_span_type.json",
    ]

    missing_cases = [c for c in expected_cases if c not in [f.name for f in test_files]]

    if missing_cases:
        print(f"\n⚠ Missing expected test cases:")
        for case in missing_cases:
            print(f"  - {case}")
    else:
        print(f"\n✓ All expected test cases present")

    return invalid_count == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
