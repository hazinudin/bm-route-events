# Superstructure-Only Update Feature — Implementation Plan

## Overview

Add a feature to update bridge superstructure data using a partial payload that contains only superstructure attributes (`no_btg`, `tipe_btg`, `seq_btg`, `struktur_ba`, `pjg_btg`) and a minimal set of profile fields (`id_jbt`, `pjg_total`, `cons_year`, `tipe_ba_utama`). Missing columns are filled from the existing inventory record in the database. The payload span configuration must exactly match the existing data — any mismatch results in rejection.

---

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

---

## Architecture Constraints & Decisions

| Constraint | Decision |
|---|---|
| Missing profile fields (NO_JBT, LINKID, LONGITUDE, etc.) | Auto-fetch from the latest existing inventory record in DB |
| Missing superstructure columns (FLOOR_WIDTH, SKEW, etc.) | Fill from existing inventory's superstructure data |
| Payload span config doesn't match existing | Reject the update |
| Validation class | Extend `BridgeInventoryValidation` with `sups_only=True` flag |
| Validation mode | `UPDATE` only — no INSERT or RETIRE |
| Merge strategy | Match payload spans to existing spans by `(SPAN_TYPE, SPAN_SEQ, SPAN_NUMBER)`, override only `STRUKTUR_BA` and `PJG_BTG` |
| Profile merge | Override only `PJG_TOTAL`, `CONS_YEAR`, `TIPE_BA_UTAMA` from payload; keep everything else from existing record |
| DB write | Only update `NAT_BRIDGE_PROFILE` (3 fields), `NAT_BRIDGE_SPAN` (all rows for this bridge), and `NAT_BRIDGE_SPAN_L3L4` if elements exist. Leave substructure tables untouched |

---

## Files to Create

### 1. `src/route_events/bridge/inventory/structure/superstructure/sups_only_schema.json`

Minimal Pydantic schema for the 5 superstructure columns in the partial payload + `BRIDGE_ID`/`INV_YEAR`.

```json
{
    "column_details": {
        "BRIDGE_ID": {
            "dtype": "string"
        },
        "INV_YEAR": {
            "dtype": "integer"
        },
        "NO_BTG": {
            "dtype": "integer",
            "db_col": "SPAN_NUMBER",
            "range": {
                "lower": 1,
                "upper": null,
                "eq_lower": true,
                "eq_upper": false
            }
        },
        "TIPE_BTG": {
            "dtype": "string",
            "db_col": "SPAN_TYPE",
            "domain": [
                "UTAMA",
                "KANAN",
                "KIRI"
            ]
        },
        "SEQ_BTG": {
            "dtype": "integer",
            "db_col": "SPAN_SEQ",
            "range": {
                "lower": 1,
                "upper": null,
                "eq_upper": true,
                "eq_lower": true
            }
        },
        "STRUKTUR_BA": {
            "dtype": "string",
            "db_col": "SUPERSTRUCTURE"
        },
        "PJG_BTG": {
            "dtype": "double",
            "db_col": "SPAN_LENGTH",
            "range": {
                "lower": 0,
                "upper": null,
                "eq_upper": false,
                "eq_lower": false
            }
        }
    }
}
```

**Rationale**: This mirrors `sups_schema.json` but only includes the 5 fields present in the partial payload plus the 2 key fields (`BRIDGE_ID`, `INV_YEAR`) that are injected at runtime. The `BRIDGE_ID` and `INV_YEAR` use `db_col` equal to column name (no mapping needed since they're injected, not from user input). The same domain/range validation rules apply from the full schema.

---

### 2. `src/route_events/bridge/inventory/structure/superstructure/sups_only_schema.py`

```python
from .....schema import RouteEventsSchema
import os


class SuperstructureOnlySchema(RouteEventsSchema):
    def __init__(self, ignore_review_err=False):
        schema_config = os.path.dirname(__file__) + '/sups_only_schema.json'
        RouteEventsSchema.__init__(
            self,
            file_path=schema_config,
            ignore_review_err=ignore_review_err
        )
```

**Rationale**: Follows the exact same pattern as `sups_schema.py` and `SuperstructureSchema`, just pointing to the new minimal schema file.

---

## Files to Modify

### 3. `src/route_events/bridge/inventory/structure/superstructure/__init__.py`

**Current**:
```python
from .superstructure import Superstructure
from .sups_schema import SuperstructureSchema
```

**Change**: Add export for `SuperstructureOnlySchema`:
```python
from .superstructure import Superstructure
from .sups_schema import SuperstructureSchema
from .sups_only_schema import SuperstructureOnlySchema
```

---

### 4. `src/route_events/bridge/inventory/structure/superstructure/superstructure.py`

Add a new `from_invij_sups_only()` class method to `Superstructure`.

#### Method: `Superstructure.from_invij_sups_only()`

```
@classmethod
def from_invij_sups_only(cls, bridge_id, inv_year, data, validate=True):
```

**Logic**:
1. If `validate=True`, validate each span dict in `data` using `SuperstructureOnlySchema` model with `BRIDGE_ID` and `INV_YEAR` overrides (same pattern as `from_invij`).
2. Convert validated data to list of dicts via `model_dump(by_alias=True)`.
3. Create `Superstructure` object from the data (`cls(spans_df.to_arrow(), validate=False)`).
4. Return the `Superstructure` object.

**Key difference from `from_invij()`**: No `ELEMEN` (element) handling — the partial payload doesn't include element data.

**Implementation detail**: After validation, the resulting dicts will have column names with db_col aliases (e.g., `SPAN_NUMBER` not `NO_BTG`). Since only 5 columns + 2 key columns are present, the resulting `Superstructure` object will only have those columns in its `artable`. The missing columns (e.g., `FLOOR_WIDTH`, `SKEW`, etc.) will need to be merged in later (in `BridgeInventory.from_sups_only_update()`).

#### Add `merge_columns()` method

```
def merge_columns(self, other_sups, columns: list[str]) -> Superstructure:
```

**Logic**:
1. Join `self.pl_df` with `other_sups.pl_df` on `(SPAN_TYPE, SPAN_SEQ, SPAN_NUMBER)`.
2. For each column in `columns`, replace `self`'s column with `other_sups`'s corresponding column.
3. Drop the `_right` suffixed columns from the join.
4. Create a new `Superstructure` from the merged DataFrame.
5. Return the new `Superstructure`.

This is needed because the partial payload `Superstructure` only has 7 columns, but the DB expects all columns. We merge in the missing columns from the existing inventory's superstructure.

**Alternative considered**: Build the merge logic entirely in `BridgeInventory.from_sups_only_update()`. Decided to put it on `Superstructure` for better encapsulation and reusability.

---

### 5. `src/route_events/bridge/inventory/structure/__init__.py`

**Current**:
```python
from .substructure import Substructure, SubstructureSchema
from .superstructure import Superstructure, SuperstructureSchema
```

**Change**: Add `SuperstructureOnlySchema`:
```python
from .substructure import Substructure, SubstructureSchema
from .superstructure import Superstructure, SuperstructureSchema, SuperstructureOnlySchema
```

---

### 6. `src/route_events/bridge/inventory/__init__.py`

**Current**:
```python
from .structure import (
    Superstructure,
    Substructure,
    SubstructureSchema,
    SuperstructureSchema
)

from .structure.element import(
    ElementSchema,
    StructureElement
)

from .profile.model import BridgeInventory
from .repo import BridgeInventoryRepo
```

**Change**: Add `SuperstructureOnlySchema` to the import:
```python
from .structure import (
    Superstructure,
    Substructure,
    SubstructureSchema,
    SuperstructureSchema,
    SuperstructureOnlySchema
)

from .structure.element import(
    ElementSchema,
    StructureElement
)

from .profile.model import BridgeInventory
from .repo import BridgeInventoryRepo
```

---

### 7. `src/route_events/bridge/inventory/profile/model.py`

Add a new `from_sups_only_update()` class method and a `sups_only_schema.json` for profile.

#### 7a. Create `src/route_events/bridge/inventory/profile/sups_only_schema.json`

Minimal profile schema for the 4 fields in the partial payload:

```json
{
    "column_details": {
        "ID_JBT": {
            "dtype": "string",
            "db_col": "BRIDGE_ID"
        },
        "PJG_TOTAL": {
            "dtype": "double",
            "range": {
                "lower": 0,
                "upper": 4000,
                "eq_upper": true,
                "eq_lower": true,
                "review": true
            },
            "db_col": "BRIDGE_LENGTH"
        },
        "CONS_YEAR": {
            "dtype": "integer",
            "db_col": "CONS_YEAR"
        },
        "TIPE_BA_UTAMA": {
            "dtype": "string",
            "db_col": "MAIN_SPAN_TYPE"
        }
    }
}
```

#### 7b. Create `src/route_events/bridge/inventory/profile/sups_only_profile_schema.py`

```python
from ....schema import RouteEventsSchema
import os


class SupsOnlyProfileSchema(RouteEventsSchema):
    def __init__(self, ignore_review_err=False):
        schema_config = os.path.dirname(__file__) + '/sups_only_schema.json'
        RouteEventsSchema.__init__(
            self,
            file_path=schema_config,
            ignore_review_err=ignore_review_err
        )
```

#### 7c. Add `from_sups_only_update()` to `BridgeInventory`

```python
@classmethod
def from_sups_only_update(cls, data: dict, existing_inv: 'BridgeInventory', ignore_review_err=False):
    """
    Load partial superstructure-only update data into a BridgeInventory object.
    Missing fields are filled from existing_inv.
    
    Args:
        data: Partial payload dict with keys: id_jbt, pjg_total, cons_year, 
              tipe_ba_utama, bangunan_atas
        existing_inv: BridgeInventory object loaded from DB with full data
        ignore_review_err: Whether to ignore review-level validation errors
    """
```

**Logic**:

1. **Validate partial profile**: Use `SupsOnlyProfileSchema` to validate the 4 profile fields in `data`. Extract `id_jbt` (→ `BRIDGE_ID`), `pjg_total` (→ `BRIDGE_LENGTH`), `cons_year` (→ `CONS_YEAR`), `tipe_ba_utama` (→ `MAIN_SPAN_TYPE`).

2. **Validate partial superstructure**: Use `SuperstructureOnlySchema` to validate `bangunan_atas` list. Inject `BRIDGE_ID` and `INV_YEAR` from `existing_inv`.

3. **Create partial Superstructure**: Call `Superstructure.from_invij_sups_only()` with the validated superstructure data. This results in a Superstructure with only 7 columns: `BRIDGE_ID`, `INV_YEAR`, `SPAN_NUMBER`, `SPAN_TYPE`, `SPAN_SEQ`, `SUPERSTRUCTURE`, `SPAN_LENGTH`.

4. **Merge superstructure columns from existing**: Determine which columns exist in the existing `Superstructure` but NOT in the partial one. These are the columns to merge. Call `partial_sups.merge_columns(existing_inv.sups, columns=missing_columns)` to fill in `FLOOR_WIDTH`, `SIDEWALK_WIDTH`, `SKEW`, etc. from existing data.

5. **Build profile DataFrame**: Start from `existing_inv.artable` (full profile). Override `BRIDGE_LENGTH` with payload's `pjg_total`, `CONS_YEAR` with payload's `cons_year`, `MAIN_SPAN_TYPE` with payload's `tipe_ba_utama`.

6. **Construct BridgeInventory**: Create `cls(merged_profile.to_arrow(), state=existing_inv.inventory_state)`.

7. **Attach merged superstructure**: `inv.add_superstructure(merged_sups, replace=True)`.

8. **Attach existing substructure** (if present): `inv.add_substructure(existing_inv.subs)`.

9. **Attach existing elements** (if present): `inv.sups.add_l3_l4_elements(existing_inv.sups.elements)` and `inv.subs.add_l3_l4_elements(existing_inv.subs.elements)`.

10. Return the fully-populated `BridgeInventory`.

**Error handling**: If the validated `id_jbt` doesn't match `existing_inv.id`, raise `ValueError`.

---

### 8. `src/service/bridge/inventory_validation.py`

This is the main validation service file. Changes are significant.

#### 8a. Modify `__init__()`

Add a `sups_only` parameter:

```python
def __init__(
        self,
        data: dict,
        validation_mode: Literal['UPDATE', 'INSERT', 'RETIRE'],
        lrs_grpc_host: str,
        sql_engine: Engine,
        ignore_review: bool = False,
        ignore_force: bool = False,
        sups_only: bool = False,     # <-- NEW
        **kwargs
):
```

**When `sups_only=True`**:

1. Extract `id_jbt` from `data` (after uppercasing).
2. Fetch existing inventory: `self._current_inv = self._repo.get_by_bridge_id(id_jbt, max(self._repo.get_available_years(id_jbt)))`.
3. Validate `id_jbt` existence in master data: `self._bm = self._bm_repo.get_by_bridge_id(id_jbt)` — reject if None.
4. Validate the partial profile fields using `SupsOnlyProfileSchema`.
5. Validate the partial superstructure using `SuperstructureOnlySchema` with `BRIDGE_ID` and `INV_YEAR` from existing inventory.
6. Parse any `ValidationError` and add messages to `self._result`.
7. Call `BridgeInventory.from_sups_only_update(data, self._current_inv, ignore_review_err=...)` to build the full `BridgeInventory`.
8. Set `self._lrs` from existing inventory's `linkid` (skip LRS validation since we don't validate location).
9. If validation fails at this point, return early with status.

**When `sups_only=False`**: Current behavior unchanged.

#### 8b. Add `span_config_match_check()` method

New validation method that ensures the payload spans exactly match existing spans:

```python
def span_config_match_check(self):
    """
    Check if the superstructure-only update payload span configuration 
    exactly matches the existing inventory span configuration.
    Every span in the payload must exist in the existing data with 
    matching SPAN_TYPE, SPAN_SEQ, and SPAN_NUMBER, and vice versa.
    """
```

**Logic**:
1. Get the set of `(SPAN_TYPE, SPAN_SEQ, SPAN_NUMBER)` from `self._inv.sups`.
2. Get the same set from `self._current_inv.sups`.
3. If the sets differ:
   - Spans in payload but not in existing: `self._result.add_message("Bentang ... tidak terdapat pada data inventori yang sudah ada.", 'error')`
   - Spans in existing but not in payload: `self._result.add_message("Bentang ... pada data inventori tidak terdapat pada data yang diinput.", 'error')`

#### 8c. Add `sups_only_update_check()` method

```python
def sups_only_update_check(self):
    """
    Validation checks for superstructure-only update.
    """
    self.previous_data_exists_check(should_exists=True)
    
    if self.get_status() != 'rejected':
        self.span_config_match_check()
    
    if self.get_status() != 'error':
        self.base_check(validate_length=True, validate_width=False)
    
    if self.get_status() != 'error':
        self.superstructure_no_changes()
        self.compare_main_span_length()
```

**Note**: `base_check()` calls `has_sups_check()` and runs structure checks. We skip `has_subs_check()` since this is a superstructure-only update. The `validate_width=False` flag skips width-related checks (since we don't have width data in the payload). However, the current `base_check` always runs `has_sups_check()`, `main_span_structure_type_check()`, `span_num_unique_check()`, etc. which are all applicable.

**Revisit `base_check()`**: Currently `base_check()` runs:
- `has_sups_check()` — OK, we have superstructure
- `has_subs_check()` for DETAILED_STATE — **should be SKIPPED for sups_only** (we don't touch subs)
- `compare_total_span_length_to_inv_length_check()` — OK, relevant
- `main_span_structure_type_check()` — OK
- `main_span_num_check()` — OK
- `span_num_unique_check()` — OK
- `other_span_num_exist_in_main_span_check()` — OK
- `span_seq_check()` — OK
- `master_data_bridge_number_comparison()` — **should be SKIPPED for sups_only** (we may not have correct bridge number in partial payload; use existing inventory's number)

Proposed approach: Instead of modifying `base_check()`, the `sups_only_update_check()` will call individual check methods directly rather than `base_check()`, giving full control:

```python
def sups_only_update_check(self):
    self.previous_data_exists_check(should_exists=True)
    
    if self.get_status() != 'rejected':
        self.span_config_match_check()
    
    if self.get_status() != 'error':
        self.has_sups_check()
        self.main_span_structure_type_check()
        self.main_span_num_check()
        self.span_num_unique_check()
        self.other_span_num_exist_in_main_span_check()
        self.span_seq_check()
        self.compare_total_span_length_to_inv_length_check()
        self.superstructure_no_changes()
        self.compare_main_span_length()
```

#### 8d. Add `put_sups_only_data()` method

```python
def put_sups_only_data(self):
    """
    Write only superstructure data to database.
    Updates profile fields (PJG_TOTAL, CONS_YEAR, TIPE_BA_UTAMA) 
    and superstructure tables.
    """
    self._repo.put_sups(self._inv)
    return self
```

#### 8e. Modify imports

Add imports at top:
```python
from route_events.bridge.inventory import SuperstructureOnlySchema
```

(Also import `SupsOnlyProfileSchema` from the profile module once created.)

---

### 9. `src/route_events/bridge/inventory/repo.py`

Add `put_sups()` method for partial updates.

#### Method: `BridgeInventoryRepo.put_sups()`

```python
def put_sups(self, obj: BridgeInventory):
    """
    Update only superstructure and profile fields for an existing bridge inventory.
    Does not modify substructure or substructure elements.
    """
    with self._engine.connect() as conn, conn.execution_options(isolation_level="READ COMMITTED"):
        try:
            # 1. Delete + re-insert superstructure data
            self._delete_sups_only(obj, conn=conn, commit=False)
            self._insert_sups_only(obj, conn=conn, commit=False)
            
            # 2. Update profile fields (PJG_TOTAL, CONS_YEAR, TIPE_BA_UTAMA)
            self._update_profile_sups_only(obj, conn=conn, commit=False)
        except Exception as e:
            conn.rollback()
            raise e
        
        conn.commit()
    
    return
```

**Helper methods**:

```python
def _delete_sups_only(self, obj: BridgeInventory, conn, commit=True):
    """Delete only superstructure and superstructure element rows."""
    _where = f"where {self.bridge_id_col} = '{obj.id}' and {self.inv_year_col} = {obj.inv_year}"
    
    for table in [self.sups_table_name, self.sups_el_table_name]:
        if not self._table_exists(table):
            continue
        try:
            conn.execute(text(f"DELETE FROM {table} {_where}"))
        except Exception as e:
            conn.rollback()
            raise e
    
    if commit:
        conn.commit()
    return


def _insert_sups_only(self, obj: BridgeInventory, conn, commit=True):
    """Insert only superstructure and superstructure element data."""
    sups_df = obj.sups.pl_df
    
    if obj.sups.elements is not None:
        sups_el_df = obj.sups.elements.pl_df
    else:
        sups_el_df = None
    
    table_mapping = {
        self.sups_table_name: sups_df,
    }
    if sups_el_df is not None:
        table_mapping[self.sups_el_table_name] = sups_el_df
    
    for table, df in table_mapping.items():
        args = []
        if self._table_exists(table):
            if has_objectid(table, self._engine):
                oids = generate_objectid(
                    schema=self._db_schema,
                    table=table,
                    sql_engine=self._engine,
                    oid_count=df.select(pl.len()).rows()[0][0]
                )
                args = [pl.Series('OBJECTID', oids)]
        
        df_ = df.with_columns(
            pl.lit(datetime.now()).dt.datetime().alias('UPDATE_DATE'),
            *args
        )
        
        try:
            df_.write_database(table, connection=conn, if_table_exists='append')
        except Exception as e:
            conn.rollback()
            raise e
    
    if commit:
        conn.commit()
    return


def _update_profile_sups_only(self, obj: BridgeInventory, conn, commit=True):
    """Update only the 3 profile fields that can change in a superstructure-only update."""
    _where = f"where {self.bridge_id_col} = '{obj.id}' and {self.inv_year_col} = {obj.inv_year}"
    
    update_query = text(
        f"UPDATE {self.inv_table_name} "
        f"SET BRIDGE_LENGTH = :bridge_length, "
        f"CONS_YEAR = :cons_year, "
        f"MAIN_SPAN_TYPE = :main_span_type, "
        f"UPDATE_DATE = :update_date "
        f"{_where}"
    )
    
    try:
        conn.execute(
            update_query,
            parameters={
                'bridge_length': obj.length,
                'cons_year': obj.artable['CONS_YEAR'][0].as_py(),
                'main_span_type': obj.span_type,
                'update_date': datetime.now()
            }
        )
    except Exception as e:
        conn.rollback()
        raise e
    
    if commit:
        conn.commit()
    return
```

**AWS note**: The profile update uses parameterized SQL to avoid injection. The `CONS_YEAR` field is accessed from `obj.artable` since there's no dedicated property for it on `BridgeInventory` — we may need to add one, or access it directly via `obj.artable.column('CONS_YEAR')[0].as_py()`.

**Alternative approach**: Instead of `_update_profile_sups_only` doing a targeted SQL UPDATE, we could do a full delete+re-insert of the profile row (like the existing `_delete` and `_insert` do). This is simpler but requires the full profile DataFrame. Since we have a fully-merged `BridgeInventory` object (with all columns filled from existing data), the full delete+re-insert approach would also work:

**Recommended approach**: Use the simpler full replace approach for profile (delete existing profile row, insert merged profile row), which matches the existing pattern and avoids having to worry about column name differences:

```python
def put_sups(self, obj: BridgeInventory):
    """
    Update only superstructure and profile data for an existing bridge inventory.
    Does not modify substructure or substructure elements.
    """
    with self._engine.connect() as conn, conn.execution_options(isolation_level="READ COMMITTED"):
        try:
            # Delete superstructure, superstructure elements, and profile
            _where = f"where {self.bridge_id_col} = '{obj.id}' and {self.inv_year_col} = {obj.inv_year}"
            for table in [self.sups_table_name, self.sups_el_table_name, self.inv_table_name]:
                if self._table_exists(table):
                    conn.execute(text(f"DELETE FROM {table} {_where}"))
            
            # Re-insert superstructure, superstructure elements (if any), and profile
            self._insert_sups_only(obj, conn=conn, commit=False)
            self._insert_profile_only(obj, conn=conn, commit=False)
        except Exception as e:
            conn.rollback()
            raise e
        
        conn.commit()
```

This is cleaner — we delete and re-insert the profile and superstructure tables, but **never touch** `NAT_BRIDGE_ABT` or `NAT_BRIDGE_ABT_L3L4`.

---

### 10. `worker/handler.py`

Add a new handler class for superstructure-only updates.

#### 10a. New payload format class

```python
class BridgeSupsOnlyPayloadFormat(BaseModel):
    model_config = ConfigDict(extra="allow")
    validation_params: Optional[BridgeValidationParams] = Field(
        default=BridgeValidationParams(), exclude=True
    )
```

Or simply reuse `BridgeValidationPayloadFormat` since it already has `extra="allow"`.

#### 10b. New handler class

```python
class BridgeSupsOnlyValidation(BridgeInventoryValidation_):
    """
    Handler for superstructure-only update validation.
    Only supports UPDATE mode.
    """
    def __init__(
        self,
        payload: BridgeValidationPayloadFormat,
        job_id: str,
        validate: bool = True,
    ):
        BridgeInventoryValidation_.__init__(
            self, payload, job_id, validate, popup=False
        )
        self._sups_only = True

    def validate(self) -> str:
        with tracer.start_as_current_span(
            "bridge.sups-only-validation-process"
        ) as span:
            span.set_attribute("validation.mode", "UPDATE")
            span.set_attribute("validation.type", "sups_only")

            check = BridgeInventoryValidation(
                data=self.payload.model_dump(),
                validation_mode="UPDATE",
                lrs_grpc_host=LRS_HOST,
                sql_engine=MISC_ENGINE,
                dev=False,
                popup=False,
                ignore_review=self.ignore_review,
                ignore_force=self.force_write,
                sups_only=True,  # <-- NEW PARAMETER
            )

            if self.validate:
                if check.get_status() == "rejected":
                    span.set_attribute("validation.final_status", check.get_status())
                    return check._result.to_job_event(self.job_id)

                check.sups_only_update_check()

            if (
                (check.get_status() == "verified")
                and WRITE_VERIFIED_DATA
            ):
                check.merge_master_data()
                check.update_master_data()

            if check.get_status() == "verified":
                check.put_sups_only_data()

            span.set_attribute("validation.result.status", check.get_status())
            span.set_status(StatusCode.OK)

        return check._result.to_job_event(self.job_id)
```

**Key differences from `BridgeInventoryValidation_`**:
- Calls `sups_only_update_check()` instead of `update_check()`
- Calls `put_sups_only_data()` instead of `put_data()`
- Always `validation_mode="UPDATE"` and `popup=False`
- Passes `sups_only=True`

---

## Detailed Validation Logic in `sups_only_update_check()`

```python
def sups_only_update_check(self):
    """
    Validation checks for superstructure-only update.
    """
    # 1. Must have existing inventory data
    self.previous_data_exists_check(should_exists=True)
    
    if self.get_status() == 'rejected':
        return
    
    # 2. Span configuration must match exactly
    self.span_config_match_check()
    
    if self.get_status() == 'error':
        return
    
    # 3. Superstructure presence check
    self.has_sups_check()
    
    # 4. Structure/type checks (all applicable to superstructure-only)
    self.main_span_structure_type_check()
    self.main_span_num_check()
    self.span_num_unique_check()
    self.other_span_num_exist_in_main_span_check()
    self.span_seq_check()
    
    # 5. Length checks
    self.compare_total_span_length_to_inv_length_check()
    self.compare_main_span_length()
    
    # 6. Review if superstructure type or span length changed
    self.superstructure_no_changes()
```

### `span_config_match_check()` implementation detail

```python
def span_config_match_check(self):
    """
    Check that the payload span configuration exactly matches existing inventory.
    """
    current_spans = set(
        (row[self._inv.sups._span_type_col], 
         row[self._inv.sups._span_seq_col], 
         row[self._inv.sups._span_num_col])
        for row in self._current_inv.sups.pl_df.iter_rows(named=True)
    )
    
    new_spans = set(
        (row[self._inv.sups._span_type_col], 
         row[self._inv.sups._span_seq_col], 
         row[self._inv.sups._span_num_col])
        for row in self._inv.sups.pl_df.iter_rows(named=True)
    )
    
    extra_in_new = new_spans - current_spans
    missing_in_new = current_spans - new_spans
    
    for span_type, span_seq, span_num in extra_in_new:
        msg = f"Bentang {span_type}/{span_seq} nomor {span_num} tidak terdapat pada data inventori yang sudah ada."
        self._result.add_message(msg, 'error')
    
    for span_type, span_seq, span_num in missing_in_new:
        msg = f"Bentang {span_type}/{span_seq} nomor {span_num} pada data inventori tidak terdapat pada data yang diinput."
        self._result.add_message(msg, 'error')
```

---

## Data Merge Flow (in `BridgeInventory.from_sups_only_update()`)

```
┌─────────────────────────┐     ┌──────────────────────────┐
│   Partial Payload       │     │   Existing Inventory (DB) │
│                         │     │                            │
│ Profile:                │     │ Profile:                   │
│   id_jbt                │     │   id_jbt, no_jbt, linkid, │
│   pjg_total  ─────override─►│   longitude, latitude, ... │
│   cons_year  ─────override─►│   pjg_total, cons_year,    │
│   tipe_ba_utama ─override─►│   tipe_ba_utama, ...        │
│                         │     │                            │
│ Superstructure:          │     │ Superstructure:            │
│   no_btg, tipe_btg,    │     │   no_btg, tipe_btg,       │
│   seq_btg, struktur_ba │     │   seq_btg, struktur_ba,    │
│   pjg_btg               │     │   pjg_btg, lbr_lantai_   │
│                         │     │   kend, lbr_trotoar_kanan,│
│                         │     │   skew, jml_gelagar, ...   │
│                         │     │                            │
│                         │     │ Substructure:              │
│                         │     │   (kept as-is)             │
│                         │     │                            │
│                         │     │ Elements:                  │
│                         │     │   (kept as-is)              │
└─────────────────────────┘     └──────────────────────────┘
                │                           │
                │         MERGE             │
                └───────────┬───────────────┘
                            │
                            ▼
              ┌──────────────────────────┐
              │   Merged BridgeInventory │
              │                          │
              │ Profile:                 │
              │   Existing profile with │
              │   3 fields overridden    │
              │                          │
              │ Superstructure:          │
              │   Payload spans with     │
              │   5 payload fields +     │
              │   remaining columns from │
              │   existing (per span)    │
              │                          │
              │ Substructure:            │
              │   Copied from existing   │
              │                          │
              │ Elements:                │
              │   Copied from existing   │
              └──────────────────────────┘
```

### Merge Algorithm for Superstructure

For each span in the payload (matched by `SPAN_TYPE + SPAN_SEQ + SPAN_NUMBER`):

1. Start with the existing span row from the DB (e.g., `{'BRIDGE_ID': 'X', 'SPAN_NUMBER': 1, 'SPAN_TYPE': 'UTAMA', 'SPAN_SEQ': 1, 'SUPERSTRUCTURE': 'ABC', 'SPAN_LENGTH': 10.0, 'FLOOR_WIDTH': 7.0, 'SKEW': 0.0, ...}`).
2. Override `SUPERSTRUCTURE` with value from payload's `struktur_ba`.
3. Override `SPAN_LENGTH` with value from payload's `pjg_btg`.
4. Keep all other columns (`FLOOR_WIDTH`, `SKEW`, etc.) from the existing data.

This means the merge happens at the **row level**: we match each payload span to an existing span, and only override the 2 superstructure columns that are present in the payload.

---

## Profile Merge in `__init__` of `BridgeInventoryValidation`

When `sups_only=True`:

```python
# After validating the partial payload fields...
# Build the merged profile by starting from existing inventory's profile
existing_profile_df = self._current_inv.pl_df

# Override the 3 fields from payload
profile_data = existing_profile_df.with_columns([
    pl.lit(validated_pjg_total).alias('BRIDGE_LENGTH'),
    pl.lit(validated_cons_year).alias('CONS_YEAR'),
    pl.lit(validated_tipe_ba_utama).alias('MAIN_SPAN_TYPE'),
])

# Create merged BridgeInventory
merged_inv = BridgeInventory(profile_data.to_arrow(), state=self._current_inv.inventory_state)
```

---

## Property Addition: `BridgeInventory.cons_year`

The `BridgeInventory` class currently has `id`, `length`, `span_type`, `width`, `number`, `linkid`, `inv_year`, `latitude`, `longitude` properties. It does **not** have a `cons_year` property. We need to add one for the profile update.

In `model.py`:

```python
@property
def cons_year(self) -> int:
    """
    Return the construction year.
    """
    return self.artable['CONS_YEAR'][0].as_py()
```

---

## What to Skip in `sups_only` Mode

| Check/Action | Full UPDATE | Sups-Only UPDATE | Reason |
|---|---|---|---|
| `previous_data_exists_check` | Yes | Yes | Must have existing data |
| `has_sups_check` | Yes | Yes | Payload has superstructure |
| `has_subs_check` | Yes (DETAIL) | **No** | Not touching substructure |
| `master_data_distance_check` | Yes | **No** | Not changing location |
| `lrs_distance_check` | Yes | **No** | Not changing location |
| `compare_length_to_master_data_check` | Yes | **No** | Not comparing location |
| `master_data_bridge_number_comparison` | Yes | **No** | Using existing bridge number |
| `span_width_check` | Yes | **No** | No width data in payload |
| `floor_width_no_changes` | Yes | **No** | No width data in payload |
| `sidewalk_width_no_changes` | Yes | **No** | No width data in payload |
| `subs_num_unique_check` | Yes | **No** | Not touching substructure |
| `span_subs_count_check` | Yes | **No** | Not touching substructure |
| LRS validation in `__init__` | Yes | **No** | Not changing location |
| `span_config_match_check` | No | **Yes** | NEW: ensure match |
| `main_span_structure_type_check` | Yes | Yes | Relevant |
| `main_span_num_check` | Yes | Yes | Relevant |
| `span_num_unique_check` | Yes | Yes | Relevant |
| `other_span_num_exist_in_main_span_check` | Yes | Yes | Relevant |
| `span_seq_check` | Yes | Yes | Relevant |
| `compare_total_span_length_to_inv_length_check` | Yes | Yes | Relevant (pjg_total changed) |
| `compare_main_span_length` | Yes | Yes | Relevant |
| `superstructure_no_changes` | Yes | Yes | Relevant |
| `put_data()` (all tables) | Yes | **No** | Use `put_sups_only_data()` |
| `put_sups_only_data()` (partial) | No | **Yes** | NEW: only superstructure tables |
| `merge_master_data()` | Yes | Yes | Length may have changed |
| `update_master_data()` | Yes | Yes | Length may have changed |

---

## `__init__` Flow for `sups_only=True`

```
1. Extract id_jbt from data
2. Fetch existing inventory from DB
3. Fetch master data from DB
4. If no existing inventory → reject ("Jembatan tidak tersedia untuk diupdate")
5. If no master data → reject ("Jembatan belum memiliki data pokok jembatan")
6. Validate partial profile fields (id_jbt, pjg_total, cons_year, tipe_ba_utama)
7. Validate partial superstructure (5 columns per span)
8. Merge payload with existing data:
   a. Build full profile from existing + 3 overrides
   b. Build full superstructure from existing + 2 column overrides per span
   c. Copy substructure from existing
   d. Copy elements from existing
9. Create BridgeInventory from merged data
10. Skip LRS validation
11. Add any review messages from validation
12. Store result
```

---

## Testing Considerations

### Unit Tests to Add

1. **`test_sups_only_schema_validation.py`**: Validate that `SuperstructureOnlySchema` and `SupsOnlyProfileSchema` correctly validate the partial payload and reject invalid data.

2. **`test_from_sups_only_update.py`**: Test `BridgeInventory.from_sups_only_update()`:
   - Happy path: partial payload merges correctly with existing inventory
   - Profile fields are overridden (`PJG_TOTAL`, `CONS_YEAR`, `TIPE_BA_UTAMA`)
   - Superstructure columns are merged (`STRUKTUR_BA`, `PJG_BTG` from payload; rest from existing)
   - Substructure and elements are carried over
   - Error when `id_jbt` doesn't match existing inventory's `id`

3. **`test_span_config_match_check.py`**: Test the new validation method:
   - Matching span config passes
   - Missing span in payload fails
   - Extra span in payload fails
   - Different `SPAN_TYPE`/`SPAN_SEQ`/`SPAN_NUMBER` fails

4. **`test_sups_only_validation.py`**: Integration test for `BridgeInventoryValidation` with `sups_only=True`:
   - All applicable checks run
   - Skipped checks don't run
   - `put_sups_only_data()` only modifies superstructure and profile tables

5. **`test_from_invij_sups_only.py`**: Test `Superstructure.from_invij_sups_only()`:
   - Valid partial data creates correct `Superstructure` object
   - Invalid data raises `ValidationError`

---

## Execution Order

1. Create `sups_only_schema.json` and `sups_only_schema.py` for superstructure
2. Create `sups_only_schema.json` and `sups_only_profile_schema.py` for profile
3. Add `from_invij_sups_only()` and `merge_columns()` to `Superstructure`
4. Add `from_sups_only_update()` and `cons_year` property to `BridgeInventory`
5. Add `SuperstructureOnlySchema` and `SupsOnlyProfileSchema` to `__init__.py` exports
6. Add `sups_only` parameter and `sups_only` flow to `BridgeInventoryValidation.__init__()`
7. Add `span_config_match_check()` method to `BridgeInventoryValidation`
8. Add `sups_only_update_check()` method to `BridgeInventoryValidation`
9. Add `put_sups_only_data()` method to `BridgeInventoryValidation`
10. Add `put_sups()` method to `BridgeInventoryRepo`
11. Add `BridgeSupsOnlyValidation` handler to `worker/handler.py`
12. Write unit tests