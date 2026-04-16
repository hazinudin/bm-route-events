"""
Generate test data for superstructure-only update feature from existing test data.
This creates partial payloads based on the full inventory data.
"""

import json
import os


def load_full_inventory(file_path):
    """Load full inventory data from JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def generate_sups_only_payload(full_data):
    """
    Generate a superstructure-only update payload from full inventory data.
    Extracts only the 5 superstructure columns + 4 profile fields.
    """
    # Extract profile fields (only the 4 that can be updated)
    profile_data = {
        "id_jbt": full_data["id_jbt"],
        "pjg_total": full_data["pjg_total"],
        "cons_year": full_data["tahun_bangun"],
        "tipe_ba_utama": full_data["tipe_ba_utama"],
    }

    # Extract superstructure data (only the 5 columns from partial payload)
    bangunan_atas = []
    for span in full_data["bangunan_atas"]:
        span_data = {
            "no_btg": span["no_btg"],
            "tipe_btg": span["tipe_btg"].upper(),  # Convert to uppercase for validation
            "seq_btg": span["seq_btg"],
            "struktur_ba": span["struktur_ba"],
            "pjg_btg": span["pjg_btg"],
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


def generate_modified_sups_payload(full_data, modifications):
    """
    Generate a modified superstructure-only payload for testing updates.

    Args:
        full_data: Full inventory data
        modifications: Dict with optional keys:
            - 'pjg_total': New bridge length
            - 'cons_year': New construction year
            - 'tipe_ba_utama': New main span type
            - 'spans': List of span modifications (index-based)
    """
    base_payload = generate_sups_only_payload(full_data)

    # Apply profile modifications
    if "pjg_total" in modifications:
        base_payload["pjg_total"] = modifications["pjg_total"]
    if "cons_year" in modifications:
        base_payload["cons_year"] = modifications["cons_year"]
    if "tipe_ba_utama" in modifications:
        base_payload["tipe_ba_utama"] = modifications["tipe_ba_utama"]

    # Apply span modifications
    if "spans" in modifications:
        for mod in modifications["spans"]:
            idx = mod.get("index", 0)
            if idx < len(base_payload["bangunan_atas"]):
                if "struktur_ba" in mod:
                    base_payload["bangunan_atas"][idx]["struktur_ba"] = mod[
                        "struktur_ba"
                    ]
                if "pjg_btg" in mod:
                    base_payload["bangunan_atas"][idx]["pjg_btg"] = mod["pjg_btg"]

    return base_payload


def generate_test_cases():
    """Generate various test cases for sups-only update."""
    test_dir = "tests/domain/bridge/inventory"
    os.makedirs(test_dir, exist_ok=True)

    # Load existing test data
    existing_test_file = "tests/domain/bridge/inventory/test_inventory_invij.json"

    if os.path.exists(existing_test_file):
        full_data = load_full_inventory(existing_test_file)

        # Test Case 1: Valid sups-only payload (no changes)
        print("Generating test case 1: Valid sups-only payload (no changes)")
        payload_1 = generate_sups_only_payload(full_data)
        with open(f"{test_dir}/sups_only_test_case_1_valid.json", "w") as f:
            json.dump(payload_1, f, indent=2)

        # Test Case 2: Changed bridge length
        print("Generating test case 2: Changed bridge length")
        payload_2 = generate_modified_sups_payload(
            full_data,
            {
                "pjg_total": full_data["pjg_total"] + 1.0  # Increase by 1m
            },
        )
        with open(f"{test_dir}/sups_only_test_case_2_length_change.json", "w") as f:
            json.dump(payload_2, f, indent=2)

        # Test Case 3: Changed construction year
        print("Generating test case 3: Changed construction year")
        payload_3 = generate_modified_sups_payload(
            full_data,
            {
                "cons_year": full_data["tahun_bangun"] + 5  # Increase by 5 years
            },
        )
        with open(f"{test_dir}/sups_only_test_case_3_year_change.json", "w") as f:
            json.dump(payload_3, f, indent=2)

        # Test Case 4: Changed main span type
        print("Generating test case 4: Changed main span type")
        payload_4 = generate_modified_sups_payload(
            full_data,
            {"tipe_ba_utama": "BET" if full_data["tipe_ba_utama"] != "BET" else "GTP"},
        )
        with open(f"{test_dir}/sups_only_test_case_4_type_change.json", "w") as f:
            json.dump(payload_4, f, indent=2)

        # Test Case 5: Changed superstructure type for first span
        print("Generating test case 5: Changed superstructure type")
        first_span = full_data["bangunan_atas"][0]
        payload_5 = generate_modified_sups_payload(
            full_data,
            {
                "spans": [
                    {
                        "index": 0,
                        "struktur_ba": "ABP"
                        if first_span["struktur_ba"] != "ABP"
                        else "GTP",
                    }
                ]
            },
        )
        with open(f"{test_dir}/sups_only_test_case_5_structure_change.json", "w") as f:
            json.dump(payload_5, f, indent=2)

        # Test Case 6: Changed span length
        print("Generating test case 6: Changed span length")
        payload_6 = generate_modified_sups_payload(
            full_data,
            {
                "spans": [
                    {
                        "index": 0,
                        "pjg_btg": first_span["pjg_btg"] + 0.5,  # Increase by 0.5m
                    }
                ]
            },
        )
        with open(
            f"{test_dir}/sups_only_test_case_6_span_length_change.json", "w"
        ) as f:
            json.dump(payload_6, f, indent=2)

        # Test Case 7: Multiple changes
        print("Generating test case 7: Multiple changes")
        payload_7 = generate_modified_sups_payload(
            full_data,
            {
                "pjg_total": full_data["pjg_total"] + 2.0,
                "cons_year": full_data["tahun_bangun"] + 10,
                "tipe_ba_utama": "BET"
                if full_data["tipe_ba_utama"] != "BET"
                else "GTP",
                "spans": [
                    {
                        "index": 0,
                        "struktur_ba": "ABP",
                        "pjg_btg": first_span["pjg_btg"] + 1.0,
                    }
                ],
            },
        )
        with open(f"{test_dir}/sups_only_test_case_7_multiple_changes.json", "w") as f:
            json.dump(payload_7, f, indent=2)

        # Test Case 8: Invalid - missing required field (id_jbt)
        print("Generating test case 8: Invalid - missing id_jbt")
        payload_8 = generate_sups_only_payload(full_data)
        del payload_8["id_jbt"]
        with open(
            f"{test_dir}/sups_only_test_case_8_invalid_missing_id.json", "w"
        ) as f:
            json.dump(payload_8, f, indent=2)

        # Test Case 9: Invalid - missing required field (pjg_total)
        print("Generating test case 9: Invalid - missing pjg_total")
        payload_9 = generate_sups_only_payload(full_data)
        del payload_9["pjg_total"]
        with open(
            f"{test_dir}/sups_only_test_case_9_invalid_missing_length.json", "w"
        ) as f:
            json.dump(payload_9, f, indent=2)

        # Test Case 10: Invalid - span config mismatch (remove one span)
        print("Generating test case 10: Invalid - span mismatch (missing span)")
        payload_10 = generate_sups_only_payload(full_data)
        if len(payload_10["bangunan_atas"]) > 1:
            payload_10["bangunan_atas"] = payload_10["bangunan_atas"][
                :-1
            ]  # Remove last span
        with open(
            f"{test_dir}/sups_only_test_case_10_invalid_span_mismatch.json", "w"
        ) as f:
            json.dump(payload_10, f, indent=2)

        # Test Case 11: Invalid - span config mismatch (add extra span)
        print("Generating test case 11: Invalid - span mismatch (extra span)")
        payload_11 = generate_sups_only_payload(full_data)
        if len(payload_11["bangunan_atas"]) > 0:
            extra_span = payload_11["bangunan_atas"][0].copy()
            extra_span["no_btg"] = 999  # Invalid span number
            payload_11["bangunan_atas"].append(extra_span)
        with open(
            f"{test_dir}/sups_only_test_case_11_invalid_extra_span.json", "w"
        ) as f:
            json.dump(payload_11, f, indent=2)

        # Test Case 12: Invalid - negative span length
        print("Generating test case 12: Invalid - negative span length")
        payload_12 = generate_modified_sups_payload(
            full_data,
            {
                "spans": [
                    {
                        "index": 0,
                        "pjg_btg": -5.0,  # Invalid negative length
                    }
                ]
            },
        )
        with open(
            f"{test_dir}/sups_only_test_case_12_invalid_negative_length.json", "w"
        ) as f:
            json.dump(payload_12, f, indent=2)

        # Test Case 13: Invalid - invalid span type
        print("Generating test case 13: Invalid - invalid span type")
        payload_13 = generate_sups_only_payload(full_data)
        if len(payload_13["bangunan_atas"]) > 0:
            payload_13["bangunan_atas"][0]["tipe_btg"] = "INVALID_TYPE"
        with open(
            f"{test_dir}/sups_only_test_case_13_invalid_span_type.json", "w"
        ) as f:
            json.dump(payload_13, f, indent=2)

        print(f"\nGenerated 13 test cases in {test_dir}/")

    else:
        print(f"Error: Existing test file not found: {existing_test_file}")


if __name__ == "__main__":
    print("Generating test data for superstructure-only update feature")
    print("=" * 60)
    generate_test_cases()
    print("\nTest data generation complete!")
