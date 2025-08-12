import streamlit as st
import pandas as pd
import json
import bcrypt
from datetime import datetime
from io import BytesIO
from collections import deque

st.set_page_config(page_title="Aging + 43B Tool (with MSME)", layout="wide")

# -------------------------
# Authentication
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

if "users" in st.secrets:
    users = normalize_users(st.secrets)
else:
    users = load_users_from_file("users.json")

# -------------------------
# Ledger Parsing
# -------------------------
def parse_ledger_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    current_party = None
    for _, row in df_raw.iterrows():
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

# -------------------------
# MSME Template Helpers
# -------------------------
def make_msme_template(parties_list):
    return pd.DataFrame([{
        "Supplier Name": p,
        "Registered (Yes/No)": "",
        "Category (Micro/Small/Medium)": "",
        "Business Type (Trader/Manufacturer/Service Provider)": ""
    } for p in parties_list])

def to_excel_bytes(df_dict):
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in df_dict.items():
            df.to_excel(writer, sheet_name=name, index=False)
    out.seek(0)
    return out.getvalue()

# -------------------------
# Aging + 43B Processing
# -------------------------
def calculate_creditor_aging_and_43b(df: pd.DataFrame, cutoff_date: pd.Timestamp, msme_map: pd.DataFrame):
    msme_map = msme_map.rename(columns={
        'Registered (Yes/No)': 'Registered',
        'Category (Micro/Small/Medium)': 'Category',
        'Business Type (Trader/Manufacturer/Service Provider)': 'Business Type'
    }, errors='ignore')
    msme_map['Supplier Name'] = msme_map['Supplier Name'].astype(str).str.strip()

    def is_exempt(party_name):
        row = msme_map[msme_map['Supplier Name'].str.lower() == str(party_name).strip().lower()]
        if row.empty:
            return False, ""
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

    aging_summary, log_details, disallow_43b = [], [], []

    for party, group in df.groupby("Party"):
        group = group.sort_values("Date").reset_index(drop=True)
        unmatched_bills, unmatched_advances = deque(), deque()

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
            if age <= 45: bucket = "0-45"
            elif age <= 60: bucket = "46-60"
            elif age <= 90: bucket = "61-90"
            else: bucket = ">90"
            buckets[bucket] += unpaid
            log_details.append({
                "Party": party, "Invoice Date": bill["date"], "Invoice Amount": bill["amount"],
                "Matched Amount": bill["matched"], "Unpaid Amount": unpaid,
                "Age (in days)": age, "Aging Bucket": bucket, "Remarks": ""
            })
            pending_invoices.append({"date": bill["date"], "amount": bill["amount"], "remaining": unpaid})

        payments_after_cutoff = [{"date": r["Date"], "amount_remaining": r["Debit"]}
                                 for _, r in group.iterrows() if r["Debit"] > 0 and r["Date"] > cutoff_date]
        payments_after_cutoff.sort(key=lambda x: x["date"])

        for inv in pending_invoices:
            inv["paid_amount_after_cutoff"] = 0.0
            inv["paid_date_after_cutoff"] = None
        for pay in payments_after_cutoff:
            for inv in pending_invoices:
                if pay["amount_remaining"] <= 0: break
                if inv["remaining"] <= 0: continue
                alloc = min(inv["remaining"], pay["amount_remaining"])
                inv["remaining"] -= alloc
                inv["paid_amount_after_cutoff"] += alloc
                inv["paid_date_after_cutoff"] = pay["date"]
                pay["amount_remaining"] -= alloc

        for inv in pending_invoices:
            unpaid_after, paid_amt_after, paid_date_after = inv["remaining"], inv["paid_amount_after_cutoff"], inv["paid_date_after_cutoff"]
            deadline = inv["date"] + pd.Timedelta(days=45)
            within_45_days = "Yes" if paid_date_after and paid_date_after <= deadline else "No"
            exempt, reason = is_exempt(party)
            if exempt:
                disallowed_flag, within_45_days = "No", "Exempt"
            else:
                disallowed_flag = "No" if within_45_days == "Yes" else "Yes"
            disallow_43b.append({
                "Party": party, "Invoice Date": inv["date"], "Invoice Amount": inv["amount"],
                "Unpaid Amount (after cutoff allocations)": unpaid_after,
                "Paid Amount (after cutoff)": min(paid_amt_after, inv["amount"]),
                "Paid Date (after cutoff)": paid_date_after,
                "Within 45 Days": within_45_days,
                "Disallowed u/s 43B(h)": disallowed_flag,
                "MSME Exemption Applied": "Yes" if exempt else "No",
                "Exemption Reason": reason
            })
        aging_summary.append({"Party": party, "Total Outstanding": sum(buckets.values()), **buckets, "Advance to Supplier": advance_amount})

    return pd.DataFrame(aging_summary), pd.DataFrame(log_details), pd.DataFrame(disallow_43b)

# -------------------------
# App State & Navigation
# -------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "step" not in st.session_state:
    st.session_state.step = 1

def goto_step(n): st.session_state.step = n

# -------------------------
# Login Page
# -------------------------
if not st.session_state.logged_in:
    st.title("🔐 Login — Aging + 43B Tool (with MSME)")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        ok, msg = check_login(username.strip(), password, users)
        if ok:
            st.session_state.logged_in, st.session_state.user = True, username.strip()
            st.success("Login successful")
            st.rerun()
        else:
            st.error(msg)
else:
    st.sidebar.write(f"Logged in as: **{st.session_state.user}**")
    if st.sidebar.button("Logout"):
        st.session_state.logged_in, st.session_state.step = False, 1
        st.rerun()

    # ---- Step 1: Ledger Upload ----
    if st.session_state.step == 1:
        st.header("Step 1 — Upload Creditor Ledger")
        st.markdown("""
        **From Tally:** `Alt+P` → **Others** → Group of Accounts → Sundry Creditors → `Alt+E` → Configure → Period → Export Excel
        """)
        uploaded_file = st.file_uploader("Upload Ledger Excel", type=["xlsx", "xls"])
        if uploaded_file:
            try:
                df_raw = pd.read_excel(uploaded_file, header=None)
                st.session_state.parsed_data = parse_ledger_df(df_raw)
                st.session_state.unique_parties = st.session_state.parsed_data['Party'].drop_duplicates().sort_values().tolist()
                st.success("Ledger parsed successfully.")
                st.dataframe(st.session_state.parsed_data.head(10))
                if st.button("Next → MSME Mapping"): goto_step(2)
            except Exception as e:
                st.error(f"Error: {e}")

    # ---- Step 2: MSME Mapping ----
    elif st.session_state.step == 2:
        st.header("Step 2 — MSME Mapping")
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Download Template**")
            tmpl = make_msme_template(st.session_state.unique_parties[:50])
            st.download_button("CSV", tmpl.to_csv(index=False), "msme_template.csv")
            st.download_button("Excel", to_excel_bytes({"MSME Template": tmpl}), "msme_template.xlsx")
        with col_b:
            uploaded_msme = st.file_uploader("Upload MSME mapping", type=["csv","xlsx"])
        if "msme_df" not in st.session_state:
            st.session_state.msme_df = pd.DataFrame(columns=tmpl.columns)
        if uploaded_msme:
            try:
                msme_df = pd.read_csv(uploaded_msme) if uploaded_msme.name.endswith('.csv') else pd.read_excel(uploaded_msme)
                st.session_state.msme_df = msme_df
                st.success("MSME mapping loaded.")
            except Exception as e:
                st.error(f"Error: {e}")
        parties_in_map = st.session_state.msme_df['Supplier Name'].astype(str).str.strip().str.lower().tolist()
        for p in st.session_state.unique_parties:
            if p.lower() not in parties_in_map:
                st.session_state.msme_df = pd.concat([st.session_state.msme_df, pd.DataFrame([{
                    'Supplier Name': p, 'Registered (Yes/No)': '', 'Category (Micro/Small/Medium)': '', 'Business Type (Trader/Manufacturer/Service Provider)': ''
                }])], ignore_index=True)
        st.session_state.msme_df = st.data_editor(st.session_state.msme_df, num_rows="dynamic", use_container_width=True)
        col1, col2 = st.columns(2)
        if col1.button("← Back"): goto_step(1)
        if col2.button("Next → Run Processing"): goto_step(3)

    # ---- Step 3: Run Processing ----
    elif st.session_state.step == 3:
        st.header("Step 3 — Run Aging & 43B(h)")
        cutoff_date = st.date_input("Cutoff date", value=datetime(2025, 3, 31))
        col1, col2 = st.columns(2)
        if col1.button("← Back"): goto_step(2)
        if col2.button("Run Processing"):
            aging_df, log_df, df_43b_log = calculate_creditor_aging_and_43b(
                st.session_state.parsed_data, pd.to_datetime(cutoff_date), st.session_state.msme_df
            )
            st.session_state.aging_df, st.session_state.log_df, st.session_state.df_43b_log = aging_df, log_df, df_43b_log
            goto_step(4)

    # ---- Step 4: Results ----
    elif st.session_state.step == 4:
        st.header("Step 4 — Results & Download")
        st.subheader("Aging Summary")
        st.dataframe(st.session_state.aging_df)
        st.subheader("FIFO Log")
        st.dataframe(st.session_state.log_df)
        st.subheader("43B(h) Disallowance")
        st.dataframe(st.session_state.df_43b_log)
        out_bytes = to_excel_bytes({
            "Aging Summary": st.session_state.aging_df,
            "FIFO Log": st.session_state.log_df,
            "43B Disallowance": st.session_state.df_43b_log,
            "MSME Mapping Used": st.session_state.msme_df
        })
        st.download_button("⬇ Download Final Report", out_bytes, file_name="final_report.xlsx")
        if st.button("← Back"): goto_step(3)
