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

# === COLUMN MAP (new CSV format, 192 cols) ===
COL = {
    "patient_id": 0, "appointment_id": 1, "app_date": 2,
    "provider_name": 6, "city_type": 10, "patient_gender": 11, "patient_age": 12,
    "encounter_diagnoses": 30, "treamentplan_eligiblity": 44,
    "diagnosis_severity": 76, "ismhorsh": 148, "moqlq": 149, "mh_diagnosis": 162,
    "drug_cols": [46, 53, 82, 108, 124, 131, 139, 178],
}

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


def map_city(c):
    if not c: return "Unknown"
    cl = c.lower()
    if "beng" in cl or "bang" in cl: return "Bangalore"
    if "coimba" in cl: return "Coimbatore"
    if "pollach" in cl: return "Pollachi"
    if "ooty" in cl: return "Ooty"
    if "tenkasi" in cl: return "Tenkasi"
    if "mysore" in cl or "mysuru" in cl: return "Mysore"
    if "kgf" in cl: return "KGF"
    if "kottayam" in cl: return "Kottayam"
    if "guwahati" in cl: return "Guwahati"
    if "vasco" in cl: return "Vasco"
    return c.title()


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


def classify(r):
    """Returns (sc_cat, ne, primary, secondary, bucket, mh_identified)"""
    elig = r[COL["treamentplan_eligiblity"]].strip()
    ism = r[COL["ismhorsh"]].strip().lower()
    mql = r[COL["moqlq"]].strip().lower()
    dx = r[COL["encounter_diagnoses"]]

    if elig == "Not Eligible":
        if "mh only" in ism: under = "MH"
        elif "mh and sh" in ism: under = "MH"
        elif "sh only" in ism: under = "STI" if is_sti(dx) else "SH"
        else: under = "SH"
        return (under, True, None, None, None, False)

    if not ism:
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

    ncols = len(rows[0])
    print(f"CSV header has {ncols} columns")
    if ncols < 192:
        sys.exit(f"ERROR: Expected at least 192 cols, got {ncols} — CSV format may have changed")

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

    sc_entries = []
    patient_entries = []

    for r in unique:
        doc = map_doctor(r[COL["provider_name"]])
        if doc not in ALLOWED_DOCTORS:
            continue

        sc_cat, ne, primary, secondary, bucket, mh_id = classify(r)
        if sc_cat is None:
            continue

        sc_entries.append({
            "id": r[COL["appointment_id"]][:8],
            "date": r[COL["app_date"]],
            "city": map_city(r[COL["city_type"]]),
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
                "city": map_city(r[COL["city_type"]]),
                "severity": r[COL["diagnosis_severity"]] or "Mildly ill",
                "bucket": bucket,
            }
            if secondary:
                e["shPresenting"] = js_esc(r[COL["encounter_diagnoses"]])
            patient_entries.append(e)

    return sc_entries, patient_entries


def to_sc_js(e):
    return (f"  {{id:'{e['id']}',date:'{e['date']}',city:'{e['city']}',"
            f"doctor:'{e['doctor']}',src:'{e['src']}',cat:'{e['cat']}',"
            f"ne:{str(e['ne']).lower()}}},")


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

    start = content.find(PT_MARKER)
    end = content.find(NE_MARKER)
    if start == -1 or end == -1 or end < start:
        sys.exit("ERROR: Could not find patients[] markers in HTML")
    content = content[:start] + new_pt + "\n" + content[end:]

    # === Replace SC_APPTS block ===
    sorted_sc = sorted(sc_entries, key=lambda x: (x["date"], x["doctor"]))
    sc_lines = [SC_MARKER] + [to_sc_js(e) for e in sorted_sc]
    if sc_lines[-1].endswith(","):
        sc_lines[-1] = sc_lines[-1][:-1]
    new_sc = "\n".join(sc_lines)

    start = content.find(SC_MARKER)
    end = content.find("\n];", start)
    if start == -1 or end == -1:
        sys.exit("ERROR: Could not find SC_APPTS markers in HTML")
    content = content[:start] + new_sc + content[end:]

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
    update_html(sc, pt)
    print("Done.")
