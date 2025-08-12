# app.py
import streamlit as st
import pandas as pd
import json
import bcrypt
from datetime import datetime
from io import BytesIO
from collections import deque
from urllib.parse import quote

st.set_page_config(page_title="Aging + 43B Tool (with MSME)", layout="wide")

# -------------------------
# Authentication (reuse your method: st.secrets or users.json)
# -------------------------
def load_users_from_file(fname="users.json"):
    try:
        with open(fname, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def normalize_users(st_secrets):
    users_out = {}
    raw = st_secrets.get("users", {}) if st_secrets else {}
    for k, v in raw.items():
        if isinstance(v, dict):
            users_out[k] = {"password": v.get("password"), "expiry": v.get("expiry")}
        else:
            users_out[k] = {"password": v, "expiry": None}
    return users_out

def check_login(username: str, password: str, users_dict: dict):
    if username not in users_dict:
        return False, "Invalid username or password"
    stored = users_dict[username].get("password")
    if not stored:
        return False, "No password set for this user"
    try:
        if isinstance(stored, str) and stored.startswith("$2"):
            if bcrypt.checkpw(password.encode(), stored.encode()):
                expiry = users_dict[username].get("expiry")
                if expiry:
                    exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    if exp_date < datetime.today().date():
                        return False, "Subscription expired"
                return True, None
            else:
                return False, "Invalid username or password"
        else:
            if password == stored:
                expiry = users_dict[username].get("expiry")
                if expiry:
                    exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    if exp_date < datetime.today().date():
                        return False, "Subscription expired"
                return True, None
            else:
                return False, "Invalid username or password"
    except Exception as e:
        return False, f"Auth error: {e}"

# load users
if "users" in st.secrets:
    users = normalize_users(st.secrets)
else:
    users = load_users_from_file("users.json")

# -------------------------
# Ledger parsing & processing (existing logic)
# -------------------------
def parse_ledger_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    current_party = None
    for idx, row in df_raw.iterrows():
        first_col = str(row[0]) if not pd.isna(row[0]) else ""
        if isinstance(first_col, str) and first_col.strip().lower().startswith("ledger:"):
            current_party = str(row[1]).strip() if pd.notna(row[1]) else "Unknown"
            continue
        if current_party:
            date = pd.to_datetime(row[0], errors="coerce", dayfirst=True)
            if pd.isna(date) or date < pd.Timestamp("2000-01-01"):
                continue
            try:
                debit = float(row[5]) if not pd.isna(row[5]) else 0.0
                credit = float(row[6]) if not pd.isna(row[6]) else 0.0
            except Exception:
                continue
            records.append([current_party, date, debit, credit])
    data = pd.DataFrame(records, columns=["Party", "Date", "Debit", "Credit"])
    data = data.sort_values(by=["Party", "Date"]).reset_index(drop=True)
    return data

def calculate_creditor_aging_and_43b(df: pd.DataFrame, cutoff_date: pd.Timestamp, msme_map: pd.DataFrame):
    """
    msme_map: DataFrame with columns:
      'Supplier Name','Registered (Yes/No)','Category (Micro/Small/Medium)','Business Type (Trader/Manufacturer/Service Provider)'
    """
    # Normalize msme_map for quick lookup
    if msme_map is None or msme_map.empty:
        msme_map = pd.DataFrame(columns=['Supplier Name','Registered (Yes/No)','Category','Business Type'])
    msme_map = msme_map.rename(columns={
        'Registered (Yes/No)': 'Registered',
        'Category (Micro/Small/Medium)': 'Category',
        'Business Type (Trader/Manufacturer/Service Provider)': 'Business Type'
    }, errors='ignore')
    msme_map['Supplier Name'] = msme_map['Supplier Name'].astype(str).str.strip()

    def is_exempt(party_name):
        """
        Exemption rules (return (bool, reason_str)):
         - If Registered == 'No' => exempt (reason: Non-registered)
         - If Category == 'Medium' => exempt (reason: Medium category)
         - If Business Type == 'Trader' => exempt (reason: Trader)
        If party not found in msme_map, treat as unknown -> not exempt by MSME (user can edit)
        """
        row = msme_map[msme_map['Supplier Name'].str.lower() == str(party_name).strip().lower()]
        if row.empty:
            return False, ""  # unknown -> not exempt by MSME rules
        r = row.iloc[0]
        reg = str(r.get('Registered', '')).strip().lower()
        cat = str(r.get('Category', '')).strip().lower()
        btype = str(r.get('Business Type', '')).strip().lower()
        if reg in ('no', 'n', 'false', '0', ''):
            return True, "Exempt: Non-MSME registered"
        if cat == 'medium':
            return True, "Exempt: Medium category"
        if 'trader' in btype:
            return True, "Exempt: Trader"
        return False, ""

    aging_summary = []
    log_details = []
    disallow_43b = []

    for party, group in df.groupby("Party"):
        group = group.sort_values("Date").reset_index(drop=True)

        unmatched_bills = deque()
        unmatched_advances = deque()

        # Build unmatched invoices and advances using transactions <= cutoff
        for _, row in group.iterrows():
            txn_date = row["Date"]
            if pd.isna(txn_date):
                continue

            if row["Debit"] > 0 and txn_date <= cutoff_date:
                amt = row["Debit"]
                while amt > 0 and unmatched_bills:
                    bill = unmatched_bills[0]
                    avail = bill["amount"] - bill["matched"]
                    to_match = min(avail, amt)
                    bill["matched"] += to_match
                    amt -= to_match
                    if bill["matched"] == bill["amount"]:
                        unmatched_bills.popleft()
                if amt > 0:
                    unmatched_advances.append({"date": txn_date, "amount": amt})

            elif row["Credit"] > 0 and txn_date <= cutoff_date:
                bill_amt = row["Credit"]
                while bill_amt > 0 and unmatched_advances:
                    adv = unmatched_advances[0]
                    to_match = min(bill_amt, adv["amount"])
                    bill_amt -= to_match
                    adv["amount"] -= to_match
                    if adv["amount"] <= 0:
                        unmatched_advances.popleft()
                if bill_amt > 0:
                    unmatched_bills.append({"date": txn_date, "amount": bill_amt, "matched": 0})

        advance_amount = sum(a["amount"] for a in unmatched_advances)

        buckets = {"0-45": 0.0, "46-60": 0.0, "61-90": 0.0, ">90": 0.0}
        pending_invoices = []

        for bill in unmatched_bills:
            unpaid = bill["amount"] - bill["matched"]
            if unpaid <= 0:
                continue
            age = (cutoff_date - bill["date"]).days
            if age <= 45:
                bucket = "0-45"
            elif age <= 60:
                bucket = "46-60"
            elif age <= 90:
                bucket = "61-90"
            else:
                bucket = ">90"
            buckets[bucket] += unpaid

            log_details.append({
                "Party": party,
                "Invoice Date": bill["date"],
                "Invoice Amount": bill["amount"],
                "Matched Amount": bill["matched"],
                "Unpaid Amount": unpaid,
                "Age (in days)": age,
                "Aging Bucket": bucket,
                "Remarks": ""
            })

            pending_invoices.append({
                "date": bill["date"],
                "amount": bill["amount"],
                "remaining": unpaid
            })

        payments_after_cutoff = []
        for _, r in group.iterrows():
            if r["Debit"] > 0 and r["Date"] > cutoff_date:
                payments_after_cutoff.append({"date": r["Date"], "amount_remaining": r["Debit"]})
        payments_after_cutoff.sort(key=lambda x: x["date"])

        for inv in pending_invoices:
            inv["paid_amount_after_cutoff"] = 0.0
            inv["paid_date_after_cutoff"] = None

        for pay in payments_after_cutoff:
            if pay["amount_remaining"] <= 0:
                continue
            for inv in pending_invoices:
                if pay["amount_remaining"] <= 0:
                    break
                if inv["remaining"] <= 0:
                    continue
                alloc = min(inv["remaining"], pay["amount_remaining"])
                inv["remaining"] -= alloc
                inv["paid_amount_after_cutoff"] += alloc
                inv["paid_date_after_cutoff"] = pay["date"]
                pay["amount_remaining"] -= alloc

        # Build 43B disallowance rows (but incorporate MSME exemptions)
        for inv in pending_invoices:
            unpaid_after = inv["remaining"]
            paid_amt_after = inv.get("paid_amount_after_cutoff", 0.0)
            paid_date_after = inv.get("paid_date_after_cutoff", None)
            deadline = inv["date"] + pd.Timedelta(days=45)

            if paid_amt_after >= (inv.get("amount", 0.0)):
                within_45_days = "Yes" if (paid_date_after is not None and paid_date_after <= deadline) else "No"
            else:
                within_45_days = "No"

            # MSME exemption check
            exempt, reason = is_exempt(party)
            if exempt:
                disallowed_flag = "No"
                within_45_days = "Exempt"
                paid_amt_report = min(paid_amt_after, inv.get("amount", 0.0))
            else:
                disallowed_flag = "No" if within_45_days == "Yes" else "Yes"
                paid_amt_report = min(paid_amt_after, inv.get("amount", 0.0))

            disallow_43b.append({
                "Party": party,
                "Invoice Date": inv["date"],
                "Invoice Amount": inv.get("amount", 0.0),
                "Unpaid Amount (after cutoff allocations)": unpaid_after,
                "Paid Amount (after cutoff)": paid_amt_report,
                "Paid Date (after cutoff)": paid_date_after,
                "Within 45 Days": within_45_days,
                "Disallowed u/s 43B(h)": disallowed_flag,
                "MSME Exemption Applied": "Yes" if exempt else "No",
                "Exemption Reason": reason
            })

        party_summary = {
            "Party": party,
            "Total Outstanding": sum(buckets.values()),
            **buckets,
            "Advance to Supplier": advance_amount
        }
        aging_summary.append(party_summary)

    return pd.DataFrame(aging_summary), pd.DataFrame(log_details), pd.DataFrame(disallow_43b)

# -------------------------
# MSME template helpers
# -------------------------
def make_msme_template(parties_list):
    # If parties_list provided, prefill Supplier Name col
    rows = []
    for p in parties_list:
        rows.append({
            "Supplier Name": p,
            "Registered (Yes/No)": "",
            "Category (Micro/Small/Medium)": "",
            "Business Type (Trader/Manufacturer/Service Provider)": ""
        })
    df = pd.DataFrame(rows)
    return df

def to_excel_bytes(df_dict):
    """
    df_dict: {sheetname: dataframe}
    returns bytes of xlsx
    """
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in df_dict.items():
            df.to_excel(writer, sheet_name=name, index=False)
    out.seek(0)
    return out.getvalue()

# -------------------------
# UI & Session state
# -------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 Login — Aging + 43B Tool (with MSME)")
    col1, col2 = st.columns([2,1])
    with col1:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
    with col2:
        if st.button("Login"):
            ok, msg = check_login(username.strip(), password, users)
            if ok:
                st.session_state.logged_in = True
                st.session_state.user = username.strip()
                st.success("Login successful")
                st.rerun()
            else:
                st.error(msg)
    st.markdown("---")
    st.info("For Demo Please Fill this G form")
    phone = "918248979741"  # Your WhatsApp number in international format
    message = "hi"          # Or your desired message
    url = f"https://wa.me/{phone}?text={quote(message)}"
    st.markdown(f"[Open WhatsApp chat]({url})")
else:
    st.sidebar.write(f"Logged in as: **{st.session_state.user}**")
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.rerun()

    st.title("📊 Creditor Aging & 43B(h) — MSME Check")

    # Step 1: Upload ledger
    st.header("Step 1 — Upload Creditor Ledger")
    st.markdown("""
**Upload your ledger extract from Tally by following steps:**

1. From **Gateway of Tally**: `Alt+P` → **Others**  
2. Select **Group of Accounts**  
3. Choose **Sundry Creditors**  
4. Press `Alt+E` → **Configure**  
5. Set **Period**: 15/02/202X → 15/05/202X  
6. Export as **Excel**
""")
    uploaded_file = st.file_uploader("Upload Ledger Excel (xlsx/xls)", type=["xlsx", "xls"])

    parsed_data = None
    unique_parties = []
    if uploaded_file is not None:
        try:
            df_raw = pd.read_excel(uploaded_file, header=None)
            parsed_data = parse_ledger_df(df_raw)
            st.success("Ledger parsed successfully.")
            st.subheader("Preview (first 10 rows)")
            st.dataframe(parsed_data.head(10))
            unique_parties = parsed_data['Party'].drop_duplicates().sort_values().tolist()
            st.info(f"Found {len(unique_parties)} unique suppliers/parties.")
        except Exception as e:
            st.error(f"Error reading/processing ledger: {e}")

    # Step 2: MSME mapping (upload or download template)
    st.header("Step 2 — MSME Mapping")
    st.markdown("You can upload an MSME mapping file (CSV/XLSX) with supplier statuses, or download the template, edit, and re-upload. You can also edit values inline below.")

    col_a, col_b = st.columns([1,1])
    with col_a:
        st.markdown("**Download sample template**")
        sample_df = make_msme_template(unique_parties[:50])  # limit preview to first 50 for template
        csv_bytes = sample_df.to_csv(index=False).encode('utf-8')
        excel_bytes = to_excel_bytes({"MSME Template": sample_df})
        st.download_button("Download template (CSV)", data=csv_bytes, file_name="msme_template.csv", mime="text/csv")
        st.download_button("Download template (Excel)", data=excel_bytes, file_name="msme_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col_b:
        uploaded_msme = st.file_uploader("Upload MSME mapping (CSV / xlsx) (optional)", type=["csv","xlsx"])

    # Load or initialize msme_df in session
    if "msme_df" not in st.session_state:
        st.session_state.msme_df = pd.DataFrame(columns=['Supplier Name','Registered (Yes/No)','Category (Micro/Small/Medium)','Business Type (Trader/Manufacturer/Service Provider)'])

    if uploaded_msme is not None:
        try:
            if uploaded_msme.name.endswith('.csv'):
                msme_df = pd.read_csv(uploaded_msme)
            else:
                msme_df = pd.read_excel(uploaded_msme)
            # Ensure required columns
            required_cols = ['Supplier Name','Registered (Yes/No)','Category (Micro/Small/Medium)','Business Type (Trader/Manufacturer/Service Provider)']
            missing = [c for c in required_cols if c not in msme_df.columns]
            if missing:
                st.error(f"Uploaded MSME file is missing columns: {missing}. Please use the template.")
            else:
                # normalize supplier names
                msme_df['Supplier Name'] = msme_df['Supplier Name'].astype(str).str.strip()
                st.session_state.msme_df = msme_df.copy()
                st.success("MSME mapping loaded.")
        except Exception as e:
            st.error(f"Error reading MSME mapping: {e}")

    # If parsed ledger exists, ensure all parties are present in msme_df (add missing rows)
    if parsed_data is not None:
        current_msme = st.session_state.msme_df.copy()
        parties_in_map = current_msme['Supplier Name'].astype(str).str.strip().str.lower().tolist()
        added = 0
        for p in unique_parties:
            if str(p).strip().lower() not in parties_in_map:
                # append blank row
                current_msme = pd.concat([current_msme, pd.DataFrame([{
                    'Supplier Name': p,
                    'Registered (Yes/No)': '',
                    'Category (Micro/Small/Medium)': '',
                    'Business Type (Trader/Manufacturer/Service Provider)': ''
                }])], ignore_index=True)
                added += 1
        if added > 0:
            st.session_state.msme_df = current_msme
            st.info(f"Added {added} suppliers to MSME mapping for editing.")

    # Inline edit using data_editor (available in newer Streamlit)
    st.markdown("**Edit MSME mapping (inline)** — fill Registered as Yes/No, Category as Micro/Small/Medium, Business Type as Trader/Manufacturer/Service Provider")
    edited = st.data_editor(st.session_state.msme_df, num_rows="dynamic", use_container_width=True)
    # Save edited back to session
    st.session_state.msme_df = edited.copy()

    # Allow user to export the MSME mapping they edited
    if not st.session_state.msme_df.empty:
        out_msme_bytes = to_excel_bytes({"MSME Mapping": st.session_state.msme_df})
        st.download_button("⬇ Download MSME mapping you edited (Excel)", data=out_msme_bytes, file_name="msme_mapping_used.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Step 3: Run processing with MSME exemptions
    st.header("Step 3 — Run Aging & 43B(h) with MSME exemptions")
    cutoff_date = st.date_input("Select cutoff date", value=datetime(2025, 3, 31))
    if parsed_data is None:
        st.info("Upload a ledger file to enable processing.")
    else:
        run_col, note_col = st.columns([1,2])
        with run_col:
            if st.button("Run Processing (apply MSME exemptions)"):
                with st.spinner("Processing..."):
                    msme_map = st.session_state.msme_df.copy()
                    aging_df, log_df, df_43b_log = calculate_creditor_aging_and_43b(parsed_data, pd.to_datetime(cutoff_date), msme_map)
                    st.success("Processing complete.")

                    st.subheader("Aging Summary (per supplier)")
                    st.dataframe(aging_df)

                    st.subheader("FIFO Log (outstanding invoices as of cutoff)")
                    st.dataframe(log_df)

                    st.subheader("43B(h) Disallowance (with MSME exemption info)")
                    st.dataframe(df_43b_log)

                    # Export all sheets + MSME mapping used
                    out_bytes = to_excel_bytes({
                        "Aging Summary": aging_df,
                        "FIFO Log": log_df,
                        "43B Disallowance": df_43b_log,
                        "MSME Mapping Used": msme_map
                    })
                    filename = f"aging_43b_msme_{st.session_state.user}_{cutoff_date}.xlsx"
                    st.download_button("⬇ Download Final Report (Excel)", data=out_bytes, file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with note_col:
            st.markdown("""
            **Notes on MSME exemptions applied:**  
            - A supplier marked **Registered = No** is treated as *non-registered* and **exempt** from 43B(h).  
            - A supplier with **Category = Medium** is **exempt**.  
            - A supplier with **Business Type = Trader** is **exempt**.  
            - If a supplier is *not present* in the MSME mapping, they are treated as **not exempt** (so they will be assessed for disallowance) — please edit mapping inline if needed.
            """)
    st.write("---")
    st.markdown("This app processes uploaded files in-memory only.")
