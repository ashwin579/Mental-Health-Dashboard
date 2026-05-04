# MH Intelligence Dashboard

Live: https://ashwin579.github.io/Mental-Health-Dashboard/

Auto-refreshes from the **RX_Data** Google Sheet at **10:00 AM IST** and **3:00 PM IST** daily via GitHub Actions.

The "Last updated" timestamp in the top-right corner shows when the dashboard was last refreshed.

---

## One-time setup (do this once, then it runs forever)

### 1. Create a Google Cloud Service Account

1. Go to https://console.cloud.google.com/
2. Create a new project (or use an existing one) — call it e.g. `mh-dashboard`
3. **Enable APIs**: navigate to *APIs & Services → Library* → search for and enable:
   - **Google Drive API**
4. Go to *IAM & Admin → Service Accounts* → **Create Service Account**
   - Name: `dashboard-bot`
   - Role: skip (no project roles needed)
   - Click **Done**
5. Click on the new service account → **Keys** tab → **Add Key → Create new key → JSON** → download the file
6. **Copy the email address** of the service account (looks like `dashboard-bot@mh-dashboard-12345.iam.gserviceaccount.com`)

### 2. Share the Google Sheet with the service account

1. Open the *Mental health Category Launch* sheet
2. Click **Share** (top right)
3. Paste the service account email → set permission to **Viewer** → click **Send**
4. The sheet stays private to everyone else; only this bot can read it

### 3. Add the JSON key as a GitHub secret

1. In your GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**
2. Name: `GCP_SA_KEY`
3. Value: paste the *entire contents* of the JSON file you downloaded (open it in a text editor, copy everything)
4. Click **Add secret**

### 4. Enable GitHub Pages (if not already)

1. Repo **Settings → Pages**
2. Source: **Deploy from a branch**, branch: `main` (or `master`), folder: `/ (root)`
3. Save. Your dashboard URL is `https://<username>.github.io/<repo-name>/mh_dashboard.html`

### 5. Verify it works

1. Push these files to your repo
2. Go to **Actions** tab → click **Refresh Dashboard** workflow → click **Run workflow** (manual trigger)
3. Wait ~30 seconds. If it succeeds, your dashboard HTML in the repo will have a fresh commit and timestamp
4. From here on, it runs automatically at 10:00 IST and 15:00 IST every day

---

## How the auto-refresh works

```
GitHub Actions cron (10:00 + 15:00 IST)
  ↓
scripts/build_dashboard.py
  ├─ Authenticates as service account using GCP_SA_KEY secret
  ├─ Downloads sheet as CSV via Google Drive API
  ├─ Filters: Apr 13+ unique appointments, 4 allowed doctors only
  ├─ Classifies per locked logic:
  │    1. NE if treatmentplan = Not Eligible
  │    2. STI overlap if MH AND SH + STI keyword in dx
  │    3. MH only if ismhorsh = MH Only
  │    4. SH→MH if MH AND SH + moqlq = Sexual Health
  │    5. SH/STI based on dx keywords
  │    6. Skip blank-ismhorsh rows
  ├─ Regenerates patients[] and SC_APPTS blocks in mh_dashboard.html
  ├─ Injects current IST timestamp into <!-- LAST_UPDATED --> placeholder
  └─ git commit + push (only if HTML actually changed)
  ↓
GitHub Pages serves updated dashboard
```

---

## Files

| Path | What it does |
|---|---|
| `mh_dashboard.html` | The dashboard. Auto-edited by the workflow. |
| `scripts/build_dashboard.py` | Fetches CSV, applies classification, rewrites HTML. |
| `.github/workflows/refresh.yml` | Cron schedule + GitHub Action runner. |

---

## Manually trigger a refresh

From the GitHub Actions tab → Refresh Dashboard → **Run workflow**.
Useful when you want the dashboard updated immediately instead of waiting for the next scheduled run.

---

## Troubleshooting

**Workflow fails with "GCP_SA_KEY not set"**
→ Secret wasn't added to the repo correctly. Re-do step 3.

**Workflow fails with "403 Forbidden" or "File not found"**
→ Service account doesn't have access to the sheet. Re-share the sheet with the bot's email (step 2).

**Workflow runs but dashboard doesn't change**
→ No new data in the sheet since last run. The workflow only commits when HTML actually changes.

**Timestamp shows old value**
→ Browser cached the old HTML. Hard-refresh the page (Cmd/Ctrl + Shift + R).

**Sheet column structure changed**
→ Update column indices in `scripts/build_dashboard.py` (the `COL` dict at the top).
