import streamlit as st
import pandas as pd
import json
import bcrypt
from datetime import datetime
from io import BytesIO
from collections import deque

st.set_page_config(page_title="Creditors Ageing + 43B(h) Tool", layout="wide")

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
    # ... unchanged code ...
    # (This function is unchanged, same as before)
    # ... unchanged code ...
    # For brevity, not repeated here. Assume original logic.

# MSME template helpers
def make_msme_template(parties_list):
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
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in df_dict.items():
            df.to_excel(writer, sheet_name=name, index=False)
    out.seek(0)
    return out.getvalue()

# UI & Session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 Login — Aging + 43B(h) Tool (with MSME)")
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
    st.info("Contact ______ for Login details ")
else:
    st.sidebar.write(f"Logged in as: **{st.session_state.user}**")
    # Sidebar logout button
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.rerun()

    st.title("📊 Creditor Aging & 43B(h) — MSME Enhanced")

    # Main area refresh/reset button - just before Step 1 header
    if st.button("🔄 Refresh/Reset"):
        for k in ["msme_df", "parsed_data", "unique_parties", "outstanding_parties", "cutoff_date"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    # Step 1: Upload ledger
    st.header("Step 1 — Upload Creditor Ledger")
    st.markdown("""
**Upload your ledger extract from Tally by following steps:**

1. From **Gateway of Tally**: `Alt+P` → **Others**  
2. Select **Group of Accounts**  
3. Choose **Sundry Creditors**  
4. Press `Alt+E` → **Configure**  
5. Set **Period**: 13/02/2025 → 15/05/2025 for FY 2024-25  
6. Export as **Excel**
""")
    uploaded_file = st.file_uploader("Upload Ledger Excel (xlsx/xls)", type=["xlsx", "xls"])

    parsed_data = None
    unique_parties = []
    if uploaded_file is not None:
        try:
            df_raw = pd.read_excel(uploaded_file, header=None)
            parsed_data = parse_ledger_df(df_raw)
            st.session_state.parsed_data = parsed_data
            st.success("Ledger parsed successfully.")
            st.subheader("Preview (first 2 rows)")
            st.dataframe(parsed_data.head(2))
            unique_parties = parsed_data['Party'].drop_duplicates().sort_values().tolist()
            st.session_state.unique_parties = unique_parties
            st.info(f"Found {len(unique_parties)} unique suppliers/parties.")
        except Exception as e:
            st.error(f"Error reading/processing ledger: {e}")

    # Step 2: Select cutoff date and MSME mapping
    st.header("Step 2 — MSME Mapping")
    st.markdown(
        "Select cutoff date and map MSME details for only those suppliers who have a payable balance as of the cutoff date."
    )

    # Select cutoff date here (before MSME mapping)
    if "cutoff_date" not in st.session_state:
        default_cutoff = datetime(2025, 3, 31)
    else:
        default_cutoff = st.session_state.cutoff_date
    cutoff_date = st.date_input("Select cutoff date", value=default_cutoff)
    st.session_state.cutoff_date = cutoff_date

    # Only prepare outstanding_parties if ledger is uploaded
    outstanding_parties = []
    if (
        "parsed_data" in st.session_state
        and st.session_state.parsed_data is not None
        and cutoff_date is not None
    ):
        # Prepare empty MSME map for function call (not used for aging, just for structure)
        empty_msme_map = pd.DataFrame(
            columns=[
                "Supplier Name",
                "Registered (Yes/No)",
                "Category (Micro/Small/Medium)",
                "Business Type (Trader/Manufacturer/Service Provider)",
            ]
        )
        aging_df, _, _ = calculate_creditor_aging_and_43b(
            st.session_state.parsed_data, pd.to_datetime(cutoff_date), empty_msme_map
        )
        outstanding_parties = aging_df[aging_df["Total Outstanding"] > 0]["Party"].tolist()
        st.session_state.outstanding_parties = outstanding_parties

    # MSME mapping UI
    st.markdown(
        "You can upload an MSME mapping file (CSV/XLSX) with supplier statuses, or download the template, edit, and re-upload. You can also edit values inline below. **Only suppliers with payable balance as on cutoff are shown.**"
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        st.markdown("**Download sample template**")
        sample_df = make_msme_template(outstanding_parties[:50])  # preview limit
        csv_bytes = sample_df.to_csv(index=False).encode("utf-8")
        excel_bytes = to_excel_bytes({"MSME Template": sample_df})
        st.download_button(
            "Download template (CSV)",
            data=csv_bytes,
            file_name="msme_template.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download template (Excel)",
            data=excel_bytes,
            file_name="msme_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col_b:
        uploaded_msme = st.file_uploader(
            "Upload MSME mapping (CSV / xlsx) (optional)", type=["csv", "xlsx"]
        )

    # Load or initialize msme_df in session
    if "msme_df" not in st.session_state:
        st.session_state.msme_df = make_msme_template(outstanding_parties)

    if uploaded_msme is not None:
        try:
            if uploaded_msme.name.endswith(".csv"):
                msme_df = pd.read_csv(uploaded_msme)
            else:
                msme_df = pd.read_excel(uploaded_msme)
            required_cols = [
                "Supplier Name",
                "Registered (Yes/No)",
                "Category (Micro/Small/Medium)",
                "Business Type (Trader/Manufacturer/Service Provider)",
            ]
            missing = [c for c in required_cols if c not in msme_df.columns]
            if missing:
                st.error(
                    f"Uploaded MSME file is missing columns: {missing}. Please use the template."
                )
            else:
                msme_df["Supplier Name"] = msme_df["Supplier Name"].astype(str).str.strip()
                # Filter uploaded mapping to only outstanding_parties
                msme_df = msme_df[msme_df["Supplier Name"].isin(outstanding_parties)].reset_index(drop=True)
                st.session_state.msme_df = msme_df.copy()
                st.success("MSME mapping loaded.")
        except Exception as e:
            st.error(f"Error reading MSME mapping: {e}")

    # Ensure all outstanding_parties are present in msme_df
    if outstanding_parties and st.session_state.msme_df is not None:
        current_msme = st.session_state.msme_df.copy()
        parties_in_map = (
            current_msme["Supplier Name"].astype(str).str.strip().str.lower().tolist()
        )
        added = 0
        for p in outstanding_parties:
            if str(p).strip().lower() not in parties_in_map:
                current_msme = pd.concat(
                    [
                        current_msme,
                        pd.DataFrame(
                            [
                                {
                                    "Supplier Name": p,
                                    "Registered (Yes/No)": "",
                                    "Category (Micro/Small/Medium)": "",
                                    "Business Type (Trader/Manufacturer/Service Provider)": "",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
                added += 1
        if added > 0:
            st.session_state.msme_df = current_msme
            st.info(
                f"Added {added} suppliers with payable balance to MSME mapping for editing."
            )

    st.markdown(
        """
**Edit MSME mapping (inline)** — Edit here.
Any supplier included below and left blank will be treated as Non MSME registered.
If you don't know the MSME status of the supplier and want to leave it blank, it will be treated as non-registered MSME.
"""
    )
    edited = st.data_editor(
        st.session_state.msme_df, num_rows="dynamic", use_container_width=True
    )
    st.session_state.msme_df = edited.copy()

    # Allow user to export the MSME mapping they edited
    if not st.session_state.msme_df.empty:
        out_msme_bytes = to_excel_bytes({"MSME Mapping": st.session_state.msme_df})
        st.download_button(
            "⬇ Download MSME mapping you edited (Excel)",
            data=out_msme_bytes,
            file_name="msme_mapping_used.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Step 3: Run processing with MSME exemptions
    st.header("Step 3 — Run Aging & 43B(h) with MSME exemptions")
    # Use cutoff_date from session (already selected above)
    if "parsed_data" not in st.session_state or st.session_state.parsed_data is None:
        st.info("Upload a ledger file to enable processing.")
    else:
        run_col, note_col = st.columns([1, 2])
        with run_col:
            if st.button("Run & Download Final Report (Excel)"):
                with st.spinner("Processing..."):
                    msme_map = st.session_state.msme_df.copy()
                    aging_df, log_df, df_43b_log = calculate_creditor_aging_and_43b(
                        st.session_state.parsed_data,
                        pd.to_datetime(st.session_state.cutoff_date),
                        msme_map,
                    )
                    out_bytes = to_excel_bytes(
                        {
                            "Aging Summary": aging_df,
                            "FIFO Log": log_df,
                            "43B Disallowance": df_43b_log,
                            "MSME Mapping Used": msme_map,
                        }
                    )
                    filename = f"aging_43b_msme_{st.session_state.user}_{st.session_state.cutoff_date}.xlsx"
                    st.download_button(
                        "⬇ Download Final Report (Excel)",
                        data=out_bytes,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
        with note_col:
            st.markdown(
                """
            **Notes on MSME exemptions applied:**  
            - A supplier marked **Registered = No** is treated as *non-registered* and **exempt** from 43B(h).  
            - A supplier with **Category = Medium** is **exempt**.  
            - A supplier with **Business Type = Trader** is **exempt**.  
            - If a supplier is *not present* in the MSME mapping, they are treated as **not exempt** (so they will be assessed for disallowance) — please edit mapping inline if needed.
            """
            )
    st.write("---")
    st.markdown("This app processes uploaded files in-memory only.")
