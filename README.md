# Creditors Ageing with 43B(h) Check

A Streamlit app that prepares FIFO creditor ageing from a Tally ledger export and a preliminary Section 43B(h) working.

## What it does
- Matches payments to bills on a first-in, first-out basis
- Groups dues into 0-45, 46-60, 61-90 and over 90 days
- Tracks advances to suppliers
- Uses supplier MSME details to mark exemptions (non-registered, medium, trader)
- Exports Aging Summary, FIFO Log, 43B Disallowance and MSME Mapping Used sheets

## How to use
1. In Tally, open **Alt+P > Others > Group of Accounts > Sundry Creditors**.
2. Press **Alt+E > Configure**, set the period, and export to Excel.
3. Sign in to the app and upload the file.
4. Upload or edit the MSME details.
5. Select the cut-off date, then run and download the report.

## Run on your computer
```
pip install -r requirements.txt
streamlit run app.py
```
Login users come from Streamlit secrets or a local `users.json`. Never commit either one.

## Limits
- Uses 45 days from the invoice date for every supplier. It does not apply the 15-day limit or use agreement or acceptance dates.
- A bill partly paid by the cut-off date is flagged even if its balance is later paid within 45 days.
- Considers only bills in the exported period; check the period and opening balances.
- Suppliers left blank are treated as non-registered. Suppliers removed from the list are not treated as exempt.
- The downloadable template lists only the first 50 suppliers.

This is a working aid. Review every result professionally before use.
