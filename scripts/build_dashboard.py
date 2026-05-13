"""
build_dashboard.py
Fetches RX_Data sheet via Google Service Account, applies the locked classification logic,
regenerates patients[] and SC_APPTS blocks in mh_dashboard.html, injects timestamp.

Runs in GitHub Actions on schedule (10:00 + 15:00 IST).
Reads service account JSON from env var GCP_SA_KEY.
"""

import os
import re
import json
import csv
import io
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIG ===
SHEET_ID = "1C1ZmeEmBjfXS9E81GqZFNLatzh3py4eNN0imkF-_2Zs"  # Mental Health Category Launch
RX_DATA_GID = 0  # RX_Data tab gid — UPDATE if different
DASHBOARD_PATH = "index.html"
ALLOWED_DOCTORS = {"Dr. Sandhiya", "Dr. Shraddha", "Dr. Adithya", "Dr. Basava Chetan"}
DATE_CUTOFF = "2026-04-13"  # CSV-derived data starts here; pre-cutoff is manual

# Doctor-specific cutoffs: doctors who joined the pilot later only count from their start date.
# Sandhiya was in the pilot from day 1 (Mar 14). The Bangalore doctors went live Apr 23.
DOCTOR_START_DATE = {
    "Dr. Sandhiya": "2026-03-14",       # full history
    "Dr. Shraddha": "2026-04-23",       # Bangalore launch
    "Dr. Adithya": "2026-04-23",
    "Dr. Basava Chetan": "2026-04-23",
}

# === COLUMN MAP (resolved DYNAMICALLY from header row) ===
# Internal field name -> list of acceptable header names (priority order, first match wins)
# This makes the script resilient to column insertions/reorderings in the sheet.
HEADER_ALIASES = {
    "patient_id":              ["patient_id"],
    "appointment_id":          ["appointment_id"],
    "app_date":                ["app_date"],
    "clinic":                  ["clinic"],
    "provider_name":           ["provider_name"],
    "city_type":               ["city_name", "city_type"],
    "patient_gender":          ["gender", "patient_gender"],
    "patient_age":             ["age", "patient_age"],
    "encounter_diagnoses":     ["diagnosis", "encounter_diagnoses"],
    "treamentplan_eligiblity": [
        "is_the_patient_eligible_for_allo_s_treatment_plan",
        "treamentplan_eligiblity"
    ],
    "diagnosis_severity": [
        "considering_your_total_experience_with_this_particular_disorder_how_ill_is_the_patient_at_this_time",
        "diagnosis_severity"
    ],
    "ismhorsh":     ["is_mh_or_sh", "ismhorsh"],
    "moqlq":        ["what_was_the_patient_s_primary_reason_for_the_consultation_today", "moqlq"],
    "mh_diagnosis": ["working_diagnosis_based_on_icd_11", "mh_diagnosis"],
}

def resolve_columns(header_row):
    """Build COL dict by finding header names in the first row."""
    col = {}
    missing = []
    for field, aliases in HEADER_ALIASES.items():
        idx = None
        for a in aliases:
            if a in header_row:
                idx = header_row.index(a)
                break
        if idx is None:
            missing.append(field)
        else:
            col[field] = idx
    # Drug columns — any header matching drug_N_name
    col["drug_cols"] = [i for i, h in enumerate(header_row) if "drug" in h and h.endswith("_name")]
    if missing:
        sys.exit(f"ERROR: missing required columns in CSV header: {missing}\nHeader was: {header_row[:20]}...")
    return col

# COL is populated at runtime once header is read (see build_blocks)
COL = {}

STI_KW = ["STI", "GUI", "PEP", "Genito Urinary", "Post-Exposure", "Post Exposure",
          "HIV", "Genital", "Anogenital"]


def fetch_sheet_as_csv():
    """Authenticate via service account and read RX_Data tab via Sheets API.
    The default Drive export grabs the first tab only — we need the specific RX_Data tab."""
    sa_json = os.environ.get("GCP_SA_KEY")
    if not sa_json:
        sys.exit("ERROR: GCP_SA_KEY environment variable not set")

    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    # Read the entire RX_Data tab as a 2D array
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range="RX_Data",  # tab name; Google fetches all populated cells
        valueRenderOption="FORMATTED_VALUE",
    ).execute()

    rows = result.get("values", [])
    if not rows:
        sys.exit("ERROR: RX_Data tab is empty or not found")

    # Pad short rows so every row has the same column count as the header
    ncols = len(rows[0])
    padded = [r + [""] * (ncols - len(r)) for r in rows]

    # Convert to CSV string for the rest of the script's pipeline
    buf = io.StringIO()
    csv.writer(buf).writerows(padded)
    return buf.getvalue()


def is_sti(dx):
    if not dx:
        return False
    u = dx.upper()
    return any(kw.upper() in u for kw in STI_KW)


def map_doctor(p):
    pl = p.lower()
    if "sandhiya" in pl: return "Dr. Sandhiya"
    if "adithya" in pl: return "Dr. Adithya"
    if "shraddha" in pl: return "Dr. Shraddha"
    if "chetan" in pl: return "Dr. Basava Chetan"
    return p


# Clinic → City mapping (clinic column is source of truth, doctor can sit at multiple clinics)
CLINIC_TO_CITY = {
    "bharathi nagar": "Coimbatore",
    "indiranagar": "Bangalore",
    "kr puram": "Bangalore",
    # Add new clinics here as they come online
}

# Canonical city aliases — handles spelling variants (Bengaluru → Bangalore, etc.)
CITY_ALIASES = {
    "bengaluru": "Bangalore",
    "bangalore": "Bangalore",
    "bangaluru": "Bangalore",
    "chennai": "Chennai",
    "madras": "Chennai",
    "coimbatore": "Coimbatore",
    "kovai": "Coimbatore",
    "mumbai": "Mumbai",
    "bombay": "Mumbai",
}


def map_city(clinic):
    """Map clinic name to city. Falls back to clinic name if mapping unknown.
    Also normalizes spelling variants (Bengaluru → Bangalore, etc.)."""
    if not clinic: return "Unknown"
    key = clinic.strip().lower()
    # First try clinic mapping
    if key in CLINIC_TO_CITY:
        return CLINIC_TO_CITY[key]
    # Then try city alias normalization
    if key in CITY_ALIASES:
        return CITY_ALIASES[key]
    return clinic.strip().title()


def map_l1(mhdx):
    s = mhdx.lower()
    if "depress" in s: return "Depression"
    if "bipolar" in s: return "Bipolar / Psychotic Spectrum"
    if "psychos" in s or "schizo" in s: return "Bipolar / Psychotic Spectrum"
    if "ptsd" in s or "trauma" in s: return "Trauma & Stress-Related"
    if "adhd" in s or "autism" in s: return "Neurodevelopmental"
    if "ocd" in s or "obsess" in s: return "OCD Spectrum"
    if "panic" in s: return "Anxiety Disorders"
    if "hypochondri" in s or "health anxiety" in s: return "Anxiety Disorders"
    if "anxiety" in s or "gad" in s: return "Anxiety Disorders"
    if "adjust" in s or "grief" in s: return "Adjustment / Grief Spectrum"
    if any(k in s for k in ["alcohol", "cannabis", "nicotine", "substance", "addict"]):
        return "Addiction / Substance"
    if "dement" in s: return "Neurocognitive Disorders"
    return "Other MH"


def map_l2(mhdx, sev):
    s = mhdx.lower(); sl = (sev or "").lower()
    if "depress" in s:
        if "severe" in sl or "marked" in sl: return "Moderate–Severe*"
        if "mild" in sl or "border" in sl: return "Mild*"
        return "Moderate–Severe*"
    if "bipolar" in s: return "Bipolar I/II"
    if "psychos" in s: return "Psychosis"
    if "ptsd" in s and "complex" in s: return "Complex PTSD"
    if "ptsd" in s: return "PTSD"
    if "adhd" in s: return "ADHD — Combined"
    if "panic" in s: return "Panic Disorder"
    if "hypochondri" in s or "health anxiety" in s: return "Health Anxiety"
    if "gad" in s or ("gener" in s and "anx" in s):
        if "severe" in sl or "marked" in sl: return "GAD — Severe"
        if "mild" in sl or "border" in sl: return "GAD — Mild"
        return "GAD — Moderate"
    if "ocd" in s or "obsess" in s:
        return "OCD — Severe" if ("severe" in sl or "marked" in sl) else "OCD — Moderate"
    if "adjust" in s: return "Adjustment Disorder"
    if "grief" in s: return "Grief Disorder"
    if "alcohol" in s and "harm" in s: return "Alcohol Harmful Use"
    if "alcohol" in s: return "Alcohol Dependence"
    if "cannabis" in s: return "Cannabis Use Disorder"
    if "nicotine" in s or "tobacco" in s: return "Nicotine Use Disorder"
    if "dement" in s: return "Dementia"
    return "Other MH"


def map_meds(r):
    seen = []
    for c in COL["drug_cols"]:
        d = r[c].strip() if c < len(r) else ""
        if d and d not in seen:
            seen.append(d)
    return ", ".join(seen)


def js_esc(s):
    if s is None: return ""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("|", ";").strip()


# Form rolled out 2026-04-23. Before that, ismhorsh was blank for all rows but the data
# was reconciled manually elsewhere — those rows were already in the manual Sandhiya block.
# So from Apr 23 onwards, blank ismhorsh = doctor not in MH pilot → exclude.
ISMHORSH_FORM_START = "2026-04-23"


def classify(r):
    """Returns (sc_cat, ne, primary, secondary, bucket, mh_identified)"""
    elig = r[COL["treamentplan_eligiblity"]].strip()
    ism = r[COL["ismhorsh"]].strip().lower()
    mql = r[COL["moqlq"]].strip().lower()
    dx = r[COL["encounter_diagnoses"]]
    app_date = r[COL["app_date"]]

    if elig == "Not Eligible":
        if "mh only" in ism: under = "MH"
        elif "mh and sh" in ism: under = "MH"
        elif "sh only" in ism: under = "STI" if is_sti(dx) else "SH"
        else: under = "SH"
        return (under, True, None, None, None, False)

    if not ism:
        # Form rolled out Apr 23. Before that, blank ismhorsh is normal — count as Pure SH
        # (manually reconciled data already in manual Sandhiya block, so RX_Data blanks pre-Apr-23
        # represent uncategorized non-MH appointments). After Apr 23, blank = excluded.
        if app_date < ISMHORSH_FORM_START:
            return ("SH", False, None, None, None, False)
        return (None, False, None, None, None, False)

    if "mh and sh" in ism and is_sti(dx):
        if "sexual" in mql:  return ("SH", False, "SH", "MH", "STI_OVERLAP_MH", True)
        if "mental" in mql:  return ("MH", False, "MH", "SH", "STI_OVERLAP_MH", True)
        return ("MH", False, "MH", "SH", "STI_OVERLAP_MH", True)

    if "mh only" in ism:
        return ("MH", False, "MH", None, "MH_ONLY", True)

    if "mh and sh" in ism:
        if "mental" in mql:  return ("MH", False, "MH", "SH", "MH_ONLY", True)
        if "sexual" in mql:  return ("SH", False, "SH", "MH", "SH_TO_MH", True)
        return ("MH", False, "MH", "SH", "MH_ONLY", True)

    if "sh only" in ism:
        return ("STI", False, None, None, None, False) if is_sti(dx) else ("SH", False, None, None, None, False)

    return (None, False, None, None, None, False)


def build_blocks(csv_text):
    rows = list(csv.reader(io.StringIO(csv_text)))
    if len(rows) < 2:
        sys.exit("ERROR: CSV is empty or has only header")

    # Resolve column positions DYNAMICALLY from header row.
    # This survives column insertions/deletions/reorderings in the sheet.
    global COL
    COL = resolve_columns(rows[0])
    print(f"Resolved {len(COL)-1} columns from header (drug_cols={len(COL['drug_cols'])})")
    print(f"  Key columns: app_date={COL['app_date']}, provider={COL['provider_name']}, "
          f"ismhorsh={COL['ismhorsh']}, moqlq={COL['moqlq']}, mh_diagnosis={COL['mh_diagnosis']}")

    ncols = len(rows[0])
    print(f"CSV header has {ncols} columns")

    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    data = [r for r in rows[1:] if len(r) == ncols and date_re.match(r[COL["app_date"]])]
    post = [r for r in data if r[COL["app_date"]] >= DATE_CUTOFF]

    seen = set()
    unique = []
    for r in post:
        aid = r[COL["appointment_id"]]
        if aid and aid not in seen:
            seen.add(aid)
            unique.append(r)

    # === DIAGNOSTICS ===
    from collections import Counter
    print(f"DIAG: rows total={len(rows)}, valid date={len(data)}, post-Apr-13={len(post)}, unique aid={len(unique)}")
    if unique:
        print(f"DIAG: sample first row: aid={unique[0][COL['appointment_id']][:8]}, date={unique[0][COL['app_date']]}, provider={unique[0][COL['provider_name']]!r}")
        providers = Counter(r[COL['provider_name']] for r in unique)
        print(f"DIAG: provider distribution (top 10): {dict(providers.most_common(10))}")
        # Check doctor mapping
        mapped = Counter(map_doctor(r[COL['provider_name']]) for r in unique)
        print(f"DIAG: after map_doctor: {dict(mapped.most_common(10))}")
        in_allowed = sum(1 for r in unique if map_doctor(r[COL['provider_name']]) in ALLOWED_DOCTORS)
        print(f"DIAG: rows passing ALLOWED_DOCTORS filter: {in_allowed}/{len(unique)}")
    else:
        print("DIAG: unique list is EMPTY — date filter or appointment_id is dropping everything")
        # Show a few sample dates and aids from raw data
        if data:
            print(f"DIAG: data sample dates: {[r[COL['app_date']] for r in data[:5]]}")
            print(f"DIAG: data sample aids: {[r[COL['appointment_id']][:12] if r[COL['appointment_id']] else '<EMPTY>' for r in data[:5]]}")
            # Latest dates in data
            latest = sorted(set(r[COL['app_date']] for r in data))[-10:]
            print(f"DIAG: latest 10 unique dates in data: {latest}")
    # === END DIAGNOSTICS ===

    sc_entries = []
    patient_entries = []

    for r in unique:
        doc = map_doctor(r[COL["provider_name"]])
        if doc not in ALLOWED_DOCTORS:
            continue
        # Doctor-specific date cutoff: skip rows before doctor's pilot start date
        doc_start = DOCTOR_START_DATE.get(doc)
        if doc_start and r[COL["app_date"]] < doc_start:
            continue

        sc_cat, ne, primary, secondary, bucket, mh_id = classify(r)
        if sc_cat is None:
            continue

        sc_entries.append({
            "id": r[COL["appointment_id"]][:8],
            "date": r[COL["app_date"]],
            "clinic": r[COL["clinic"]].strip().title() if r[COL["clinic"]] else "Unknown",
            "city": map_city(r[COL["clinic"]]),
            "doctor": doc, "src": "Inbound", "cat": sc_cat, "ne": ne
        })

        if mh_id and bucket:
            try: age_int = int(float(r[COL["patient_age"]]))
            except: age_int = 0
            gender = r[COL["patient_gender"]].strip()
            if gender.lower().startswith("m"): gender = "Male"
            elif gender.lower().startswith("f"): gender = "Female"

            mhdx = r[COL["mh_diagnosis"]] or ""
            diag = mhdx if mhdx else r[COL["encounter_diagnoses"]]
            meds = map_meds(r)

            e = {
                "id": r[COL["appointment_id"]],
                "patientId": r[COL["patient_id"]],
                "date": r[COL["app_date"]], "src": "Inbound",
                "primary": primary, "secondary": secondary or "-",
                "gender": gender, "age": age_int,
                "l1": map_l1(mhdx), "l2": map_l2(mhdx, r[COL["diagnosis_severity"]]),
                "diag": js_esc(diag),
                "meds": js_esc(meds) if meds else "-",
                "therapy": "-", "doctor": doc,
                "clinic": r[COL["clinic"]].strip().title() if r[COL["clinic"]] else "Unknown",
                "city": map_city(r[COL["clinic"]]),
                "severity": r[COL["diagnosis_severity"]] or "Mildly ill",
                "bucket": bucket,
            }
            if secondary:
                e["shPresenting"] = js_esc(r[COL["encounter_diagnoses"]])
            patient_entries.append(e)

    return sc_entries, patient_entries


def to_sc_js(e):
    return (f"  {{id:'{e['id']}',date:'{e['date']}',clinic:'{e['clinic']}',"
            f"city:'{e['city']}',doctor:'{e['doctor']}',src:'{e['src']}',"
            f"cat:'{e['cat']}',ne:{str(e['ne']).lower()}}},")


def to_pt_js(e):
    parts = []
    for k, v in e.items():
        if isinstance(v, str):
            parts.append(f'{k}:"{v}"')
        else:
            parts.append(f"{k}:{v}")
    return "  {" + ",".join(parts) + "},"


SC_MARKER = ("  // ===== APR 13+ SC_APPTS · CSV-DERIVED · revised logic "
             "(NE first → STI overlap → ismhorsh+moqlq → STI keyword) · "
             "4 allowed doctors only =====")
PT_MARKER = ("  // ===== APR 13+ MH-IDENTIFIED · CSV-DERIVED · revised logic · "
             "NE excluded · 4 allowed doctors only =====")
NE_MARKER = "  // ===== NE patients counted in MH-identified per business rule ====="


def update_html(sc_entries, patient_entries):
    with open(DASHBOARD_PATH, encoding="utf-8") as f:
        content = f.read()

    # === Replace patients[] block ===
    sorted_pt = sorted(patient_entries, key=lambda x: (x["date"], x["doctor"]))
    new_pt = "\n".join([PT_MARKER] + [to_pt_js(e) for e in sorted_pt])

    PT_END = "  // ===== END CSV MH-IDENTIFIED ====="
    start = content.find(PT_MARKER)
    end = content.find(PT_END, start)
    if start == -1 or end == -1:
        sys.exit(f"ERROR: PT markers missing — start={start}, end={end}. HTML must contain PT_MARKER and PT_END markers.")
    content = content[:start] + new_pt + "\n" + content[end:]

    # === Replace SC_APPTS block ===
    sorted_sc = sorted(sc_entries, key=lambda x: (x["date"], x["doctor"]))
    sc_lines = [SC_MARKER] + [to_sc_js(e) for e in sorted_sc]
    if sc_lines[-1].endswith(","):
        sc_lines[-1] = sc_lines[-1][:-1]
    new_sc = "\n".join(sc_lines)

    SC_END = "  // ===== END CSV SC_APPTS ====="
    start = content.find(SC_MARKER)
    end = content.find(SC_END, start)
    if start == -1 or end == -1:
        sys.exit(f"ERROR: SC markers missing — start={start}, end={end}. HTML must contain SC_MARKER and SC_END markers.")
    # Replace from SC_MARKER through (but not including) the END marker line
    content = content[:start] + new_sc + "\n" + content[end:]

    # === Inject timestamp (IST) ===
    ist = timezone(timedelta(hours=5, minutes=30))
    ts = datetime.now(ist).strftime("%d %b %Y, %I:%M %p IST")
    content = re.sub(
        r"<!-- LAST_UPDATED_START -->.*?<!-- LAST_UPDATED_END -->",
        f"<!-- LAST_UPDATED_START -->{ts}<!-- LAST_UPDATED_END -->",
        content, flags=re.DOTALL,
    )

    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\n✓ Dashboard updated at {ts}")
    print(f"  SC_APPTS Apr 13+: {len(sc_entries)}")
    print(f"  MH patients Apr 13+: {len(patient_entries)}")

    bucket_dist = Counter(e["bucket"] for e in patient_entries)
    print(f"  Buckets: {dict(bucket_dist)}")
    ne_count = sum(1 for e in sc_entries if e["ne"])
    print(f"  NE flagged: {ne_count}")


if __name__ == "__main__":
    print("Fetching sheet via service account...")
    csv_text = fetch_sheet_as_csv()
    print(f"Downloaded {len(csv_text)} chars")

    sc, pt = build_blocks(csv_text)

    # ── SAFETY GUARD ────────────────────────────────────────────────────────
    # Today (May 10) we expect ~180+ SC entries Apr 13+. If we ever see <100,
    # something is wrong (CSV truncation, doctor name change, marker drift, etc.)
    # Abort rather than silently overwriting good data with bad.
    MIN_SC_THRESHOLD = 100
    if len(sc) < MIN_SC_THRESHOLD:
        sys.exit(
            f"ABORTED: Only {len(sc)} SC entries built (threshold {MIN_SC_THRESHOLD}). "
            f"This is suspiciously low — refusing to overwrite index.html. "
            f"Check the DIAG output above for what filtered out the rows. "
            f"Most common causes: (1) CSV column shift, (2) doctor name spelling change in sheet, "
            f"(3) sheet temporarily empty, (4) ALLOWED_DOCTORS too restrictive."
        )

    update_html(sc, pt)
    print("Done.")
