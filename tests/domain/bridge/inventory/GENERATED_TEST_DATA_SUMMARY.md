# Test Data Generation from Database

## Summary

Successfully generated test data for the superstructure-only update feature by fetching real data from the production Oracle database.

## Generated Files

### Sups-Only Payloads (5 files)
Partial payloads containing only the 4 profile fields + 5 superstructure columns:

1. `sups_only_payload_0100005_2025.json` - Bridge with 2 spans
2. `sups_only_payload_0100006_2025.json` - Bridge with 1 span
3. `sups_only_payload_0100012_2025.json` - Bridge with 1 span
4. `sups_only_payload_0100017_2025.json` - Bridge with 3 spans (best for multi-span testing)
5. `sups_only_payload_0100019_2025.json` - Bridge with 1 span

### Full Inventory Data (5 files)
Complete inventory data for reference and comparison:

1. `full_inventory_0100005_2025.json`
2. `full_inventory_0100006_2025.json`
3. `full_inventory_0100012_2025.json`
4. `full_inventory_0100017_2025.json`
5. `full_inventory_0100019_2025.json`

## Database Connection

- **Host**: `db.binamarga.pu.go.id` (from `GDB_HOST` in `tests/dev.env`)
- **Port**: 1521
- **Service**: `geodbbm`
- **User**: `MISC`
- **Tables**: Production tables (`NAT_BRIDGE_PROFILE`, `NAT_BRIDGE_SPAN`, etc.)

## Example Payload

```json
{
  "id_jbt": "0100017",
  "pjg_total": 72.5,
  "cons_year": 2025,
  "tipe_ba_utama": "RBB",
  "bangunan_atas": [
    {
      "no_btg": 1,
      "tipe_btg": "UTAMA",
      "seq_btg": 1,
      "struktur_ba": "RBB",
      "pjg_btg": 36.2
    },
    {
      "no_btg": 2,
      "tipe_btg": "UTAMA",
      "seq_btg": 1,
      "struktur_ba": "RBB",
      "pjg_btg": 18.2
    },
    {
      "no_btg": 3,
      "tipe_btg": "UTAMA",
      "seq_btg": 1,
      "struktur_ba": "RBB",
      "pjg_btg": 18.1
    }
  ]
}
```

## Script Usage

```bash
source .venv/bin/activate
python scripts/generate_sups_only_test_data.py
```

The script:
1. Reads database credentials from `tests/dev.env`
2. Connects to Oracle database at `GDB_HOST`
3. Queries production tables for bridges with complete inventory data
4. Generates partial sups-only payloads and full inventory files
5. Saves files to `tests/domain/bridge/inventory/`

## Bridge Data Summary

| Bridge ID | Year | Spans | Length (m) | Main Span Type |
|-----------|------|-------|------------|----------------|
| 0100005   | 2025 | 2     | 82.0       | RBB            |
| 0100006   | 2025 | 1     | 26.0       | RBB            |
| 0100012   | 2025 | 1     | 20.0       | RBB            |
| 0100017   | 2025 | 3     | 72.5       | RBB            |
| 0100019   | 2025 | 1     | 16.0       | RBB            |

## Next Steps

1. Use these real payloads for integration testing
2. Generate modified versions (changed lengths, types, etc.) for testing various scenarios
3. Test the validation flow with `BridgeInventoryValidation` using `sups_only=True`
4. Verify that span configuration matching works correctly

## Related Files

- Script: `scripts/generate_sups_only_test_data.py`
- Implementation Plan: `docs/sups-only-update-implementation-plan.md`
- Test Data README: `tests/domain/bridge/inventory/README_SUPS_ONLY_TEST_DATA.md`
