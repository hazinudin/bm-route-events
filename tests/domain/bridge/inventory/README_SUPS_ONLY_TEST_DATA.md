# Superstructure-Only Update Test Data

This directory contains test data for the superstructure-only update feature as specified in the implementation plan (`docs/sups-only-update-implementation-plan.md`).

## Overview

The superstructure-only update feature allows updating bridge superstructure data using a **partial payload** that contains only:
- 4 profile fields: `id_jbt`, `pjg_total`, `cons_year`, `tipe_ba_utama`
- 5 superstructure columns per span: `no_btg`, `tipe_btg`, `seq_btg`, `struktur_ba`, `pjg_btg`

Missing fields are automatically filled from the existing inventory record in the database.

## Payload Format

```json
{
  "id_jbt": "<ID jembatan>",
  "pjg_total": "<panjang jembatan dalam m>",
  "cons_year": "<tahun bangun>",
  "tipe_ba_utama": "<tipe bangunan atas utama>",
  "bangunan_atas": [
    {
      "no_btg": "<nomor bentang>",
      "tipe_btg": "<tipe bentang (UTAMA, KANAN, KIRI)>",
      "seq_btg": "<urutan bentang pelebaran>",
      "struktur_ba": "<struktur bangunan atas>",
      "pjg_btg": "<panjang bentang>"
    }
  ]
}
```

## Test Cases

### Valid Test Cases

#### 1. `sups_only_test_case_1_valid.json`
**Description**: Valid payload with no changes from existing data  
**Purpose**: Baseline test - should pass validation with no review warnings  
**Expected Result**: ✅ Verified (no changes detected)

#### 2. `sups_only_test_case_2_length_change.json`
**Description**: Changed bridge length (`pjg_total`)  
**Purpose**: Test profile field update - bridge length increased by 1m  
**Expected Result**: ✅ Verified (master data merge required)

#### 3. `sups_only_test_case_3_year_change.json`
**Description**: Changed construction year (`cons_year`)  
**Purpose**: Test profile field update - construction year increased by 5 years  
**Expected Result**: ✅ Verified

#### 4. `sups_only_test_case_4_type_change.json`
**Description**: Changed main span type (`tipe_ba_utama`)  
**Purpose**: Test profile field update - alternate main span type  
**Expected Result**: ✅ Verified

#### 5. `sups_only_test_case_5_structure_change.json`
**Description**: Changed superstructure type for first span (`struktur_ba`)  
**Purpose**: Test superstructure column update - GTP → ABP  
**Expected Result**: ⚠️ Review (superstructure type changed)

#### 6. `sups_only_test_case_6_span_length_change.json`
**Description**: Changed span length for first span (`pjg_btg`)  
**Purpose**: Test superstructure column update - span length increased by 0.5m  
**Expected Result**: ⚠️ Review (span length changed)

#### 7. `sups_only_test_case_7_multiple_changes.json`
**Description**: Multiple changes across profile and superstructure  
**Purpose**: Integration test - all fields changed simultaneously  
**Expected Result**: ⚠️ Review (multiple changes)

### Invalid Test Cases

#### 8. `sups_only_test_case_8_invalid_missing_id.json`
**Description**: Missing required `id_jbt` field  
**Purpose**: Test schema validation - missing bridge ID  
**Expected Result**: ❌ Rejected (schema validation error)

#### 9. `sups_only_test_case_9_invalid_missing_length.json`
**Description**: Missing required `pjg_total` field  
**Purpose**: Test schema validation - missing bridge length  
**Expected Result**: ❌ Rejected (schema validation error)

#### 10. `sups_only_test_case_10_invalid_span_mismatch.json`
**Description**: Missing span in configuration  
**Purpose**: Test span configuration match - removed last span  
**Expected Result**: ❌ Rejected (span configuration mismatch)

#### 11. `sups_only_test_case_11_invalid_extra_span.json`
**Description**: Extra span in configuration  
**Purpose**: Test span configuration match - added invalid span #999  
**Expected Result**: ❌ Rejected (span configuration mismatch)

#### 12. `sups_only_test_case_12_invalid_negative_length.json`
**Description**: Negative span length  
**Purpose**: Test range validation - span length = -5.0  
**Expected Result**: ❌ Rejected (range validation error)

#### 13. `sups_only_test_case_13_invalid_span_type.json`
**Description**: Invalid span type  
**Purpose**: Test domain validation - tipe_btg = 'INVALID_TYPE'  
**Expected Result**: ❌ Rejected (domain validation error)

## Generation Script

Test data was generated using:
```bash
python scripts/generate_sups_only_test_data_local.py
```

This script reads the existing test inventory data from `test_inventory_invij.json` and generates:
- 7 valid test cases with various field modifications
- 6 invalid test cases for validation testing

## Validation Rules

### Schema Validation
- `id_jbt`: String, required
- `pjg_total`: Double, range [0, 4000], required
- `cons_year`: Integer, required
- `tipe_ba_utama`: String, required
- `no_btg`: Integer, range [1, ∞), required
- `tipe_btg`: String, domain [UTAMA, KANAN, KIRI], required
- `seq_btg`: Integer, range [1, ∞), required
- `struktur_ba`: String, required
- `pjg_btg`: Double, range (0, ∞), required

### Business Rules
1. **Span Configuration Match**: Payload spans must exactly match existing inventory spans by (SPAN_TYPE, SPAN_SEQ, SPAN_NUMBER)
2. **Bridge Existence**: Bridge ID must exist in master data and have existing inventory
3. **Superstructure Presence**: Must have at least one superstructure span
4. **Structure Type Validation**: Main span structure type must be valid
5. **Span Number Uniqueness**: Span numbers must be unique within each span type
6. **Span Sequence**: Span sequences must be valid and continuous

## Merge Strategy

When a valid sups-only payload is processed:

### Profile Merge
- **Override**: `PJG_TOTAL`, `CONS_YEAR`, `TIPE_BA_UTAMA` from payload
- **Keep**: All other profile fields (NO_JBT, LINKID, LONGITUDE, etc.) from existing inventory

### Superstructure Merge
For each span (matched by SPAN_TYPE, SPAN_SEQ, SPAN_NUMBER):
- **Override**: `STRUKTUR_BA`, `SPAN_LENGTH` from payload
- **Keep**: All other columns (FLOOR_WIDTH, SKEW, NUM_GIRDERS, etc.) from existing inventory

### Substructure
- **Keep**: All substructure data unchanged from existing inventory

### Elements
- **Keep**: All L3/L4 elements unchanged from existing inventory

## Database Tables Updated

When `put_sups_only_data()` is called:
- ✅ `NAT_BRIDGE_PROFILE` - Updated (3 fields: BRIDGE_LENGTH, CONS_YEAR, MAIN_SPAN_TYPE)
- ✅ `NAT_BRIDGE_SPAN` - Updated (all rows for this bridge)
- ✅ `NAT_BRIDGE_SPAN_L3L4` - Updated (if elements exist)
- ❌ `NAT_BRIDGE_ABT` - Not touched
- ❌ `NAT_BRIDGE_ABT_L3L4` - Not touched

## Usage Example

```python
from src.route_events.bridge.inventory import BridgeInventory, BridgeInventoryRepo
from worker.handler import BridgeSupsOnlyValidation

# Load existing inventory
repo = BridgeInventoryRepo(sql_engine=engine)
existing_inv = repo.get_by_bridge_id('2201152', 2025)

# Load partial payload
with open('tests/domain/bridge/inventory/sups_only_test_case_1_valid.json') as f:
    payload_data = json.load(f)

# Create merged inventory
merged_inv = BridgeInventory.from_sups_only_update(payload_data, existing_inv)

# Validate
validation = BridgeInventoryValidation(
    data=payload_data,
    validation_mode='UPDATE',
    lrs_grpc_host=LRS_HOST,
    sql_engine=MISC_ENGINE,
    sups_only=True
)

validation.sups_only_update_check()

if validation.get_status() == 'verified':
    validation.put_sups_only_data()
```

## Related Files

- Implementation Plan: `docs/sups-only-update-implementation-plan.md`
- Schema: `src/route_events/bridge/inventory/profile/sups_only_schema.json`
- Schema: `src/route_events/bridge/inventory/structure/superstructure/sups_only_schema.json`
- Validation: `src/service/bridge/inventory_validation.py`
- Handler: `worker/handler.py` (BridgeSupsOnlyValidation class)

## Notes

- All test cases are based on bridge ID `2201152` from existing test data
- Test data assumes the bridge exists in the database with inventory year 2025
- For database testing, ensure the bridge inventory exists before running update tests
- Invalid test cases should be tested with unit tests, not integration tests
