import pandas as pd


VTR = pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VTR.csv")
VLR_ACTIVE=pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VLR_ACTIVE.csv")
VCOMPONENT = pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VCOMPONENT.csv")
VCOSTCENTER = pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VCOSTCENTER.csv")
VPROFITCENTER = pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VPROFITCENTER.csv")
VLESSEE = pd.read_csv("C:/Users/warre.devleeschauwer/Downloads/VLESSEE.csv")
UPLOADNLSUBGROUP = pd.read_csv("C:/Users\warre.devleeschauwer/Downloads/1. Upload NL subgroup.xlsm")

print(VTR)
print(VTR.head())
print(VTR.columns)

##Making bookingVoucherDebit
# Step 1: Remove specified credit columns
columns_to_remove = ["CREDIT_ACCOUNTNUMBER", "CREDIT_ACCOUNTDESC", "CREDIT_ACCOUNTTYPE", "CREDIT_TRANSACTIONTYPE", "VALUE", "Credit_Value"]
VTR_wo_creditColumns = VTR.drop(columns=columns_to_remove)

# Step 2: Rename debit columns to the desired names
columns_to_rename = {
    "Debit_Value": "VALUE",
    "DEBIT_ACCOUNTNUMBER": "ACCOUNTNUMBER",
    "DEBIT_ACCOUNTDESC": "ACCOUNTDESC",
    "DEBIT_ACCOUNTTYPE": "ACCOUNTTYPE",
    "DEBIT_TRANSACTIONTYPE": "TRANSACTIONTYPE"
}
bookingVoucherDebit = VTR_wo_creditColumns.rename(columns=columns_to_rename)

print(bookingVoucherDebit)

##Making bookingVoucherCredit
# Step 1: Remove specified debit columns
columns_to_remove = ["DEBIT_ACCOUNTNUMBER", "DEBIT_ACCOUNTDESC", "DEBIT_ACCOUNTTYPE", "DEBIT_TRANSACTIONTYPE", "Debit_Value", "VALUE"]
VTR_wo_debitColumns = VTR.drop(columns=columns_to_remove)

# Step 2: Rename credit columns to the desired names
columns_to_rename = {
    "CREDIT_ACCOUNTNUMBER": "ACCOUNTNUMBER",
    "CREDIT_ACCOUNTDESC": "ACCOUNTDESC",
    "CREDIT_ACCOUNTTYPE": "ACCOUNTTYPE",
    "CREDIT_TRANSACTIONTYPE": "TRANSACTIONTYPE",
    "Credit_Value": "VALUE"
}
bookingVoucherCredit = VTR_wo_debitColumns.rename(columns=columns_to_rename)

print(bookingVoucherCredit.columns)

##Combine Debit and credit into one bookingVoucher

bookingVoucher = pd.concat([bookingVoucherDebit, bookingVoucherCredit], ignore_index=True)

print(bookingVoucher.columns)

##Adjusting bookingVoucher

# Step 1: Convert ACCOUNTNUMBER to string and filter rows where it does not start with '9' or '14'
bookingVoucher['ACCOUNTNUMBER'] = bookingVoucher['ACCOUNTNUMBER'].astype(str)
filtered_rows = bookingVoucher[
    ~bookingVoucher['ACCOUNTNUMBER'].str.startswith(('9', '14'))
]

# Step 2: Extract the year from DOCUMENT_DATE
filtered_rows['DOCUMENT_DATE'] = pd.to_datetime(filtered_rows['DOCUMENT_DATE'])
filtered_rows['DOCUMENT_DATE'] = filtered_rows['DOCUMENT_DATE'].dt.year

# Step 3: Merge with VLR_ACTIVE on LEASERECORD_ID
merged_positionID = pd.merge(filtered_rows, VLR_ACTIVE[['LEASERECORDID', 'POSITIONID']],
                             left_on='LEASERECORD_ID', right_on='LEASERECORDID', how='left')

# Step 4: Merge with VCOMPONENT on POSITIONID
merged_cost_center = pd.merge(merged_positionID, VCOMPONENT[['POSITIONID', 'COST_CENTER_ID', 'PROFIT_CENTER_ID']],
                              on='POSITIONID', how='left')

# Step 5: Merge with VCOSTCENTER on COST_CENTER_ID

print(VCOSTCENTER.columns)

# Create 'CC Short' by taking the first 4 characters of the 'name' column
VCOSTCENTER['CC Short'] = VCOSTCENTER['name'].str[:4]

# Perform the merge operation with the 'merged_cost_center' DataFrame
merged_cc = pd.merge(merged_cost_center, VCOSTCENTER[['id', 'CC Short']],
                     left_on='COST_CENTER_ID', right_on='id', how='left')


# Step 6: Merge with VPROFITCENTER on PROFIT_CENTER_ID
merged_pc = pd.merge(merged_cc, VPROFITCENTER[['id', 'name']],
                     left_on='PROFIT_CENTER_ID', right_on='id', how='left')

# Step 7: Create a combined Cost_Center column
merged_pc['Cost_Center'] = merged_pc.apply(lambda row: row['name'] if pd.isna(row['CC Short']) else row['CC Short'], axis=1)

# Step 8: Group by specific columns and sum the VALUE
grouped_rows = merged_pc.groupby(['PARTNER_ID', 'DOCUMENT_DATE', 'ACCOUNTNUMBER', 'Cost_Center'], as_index=False).agg({'VALUE': 'sum'})

# Step 9: Add and adjust columns (multiply VALUE by -1)
grouped_rows['Value'] = grouped_rows['VALUE'] * -1
grouped_rows.drop(columns=['VALUE'], inplace=True)

# Assuming BookingVoucherDefTax is another DataFrame you want to append
# Step 10: Append the additional DataFrame
final_df = pd.concat([grouped_rows, BookingVoucherDefTax], ignore_index=True)

# Step 11: Change the data type of the Value column to numeric if not already
final_df['Value'] = pd.to_numeric(final_df['Value'])

# Print or inspect the final DataFrame to confirm the transformations
print(final_df.head())
