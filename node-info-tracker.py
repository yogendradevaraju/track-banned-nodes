import pandas as pd
import glob
import os
from collections import defaultdict
import matplotlib.pyplot as plt

# Define the folder path containing the CSV files
folder = 'folder path containing csv files'
files = glob.glob(os.path.join(folder, '*.csv'))

# Group files by day using the naming pattern: node_info_YYYYMMDD_HHMMSS.csv
files_by_day = defaultdict(list)
for file in files:
    # Extract filename without path
    print(f"file name : {file}")
    filename = os.path.basename(file)
    # Assuming the filename always starts with 'node_info_' and then the date
    # Example: node_info_20250226_230001.csv -> date portion is '20250226'
    date_str = filename.split('_')[2]  # Splitting gives: ['node', 'info', '20250226', '230001.csv']
    files_by_day[date_str].append(file)

# Define the list of standard ban reasons
standard_ban_reasons = [
    "HcaFatalError",
    "xAI-gpu_remapped_rows",
    "xAI-gpu_ecc_error",
    "xAI-gpu_topo_check",
    "nvidia-smi hanging",
    "xAIMeta-NPDSource-ConnectionRefused",
    "xAIMeta-NPDSource-RefreshFailure",
    "FailedMount",
    "xAI-host_kernel_stack",
    "xAI-ib_link_state",
    "xAI-ib_link_status",
    "RowRemapFailure",
    "xAI-gpu_ecc_error_xai_reboot",
    "xAI-gpu_remapped_rows_xai_reboot",
    "LinkDownFatal"
]

# Define OCI states and conditions to exclude for no matching ban reason
excluded_oci_states = [
    "send_to_oci",
    "bvs",
    "validate",
    "hold-rdma",
    "triage",
    "run_hpl",
    "nhc_error",
    "run_tinymeg",
    "validate",
    "hold-1n-nccl",
    "hold-2n-nccl",
    "hold-UnexpectedAdmissionError",
    "hold-lustre",
    "slow-tinymeg"
]


# Dictionary to store daily results
daily_results = {}

# Helper function to determine if a ban reason is valid.
def is_valid_ban_reason(reason):
    if pd.isna(reason):
        return False
    # Split compound ban reasons by the pipe symbol
    parts = reason.split('|')
    # Return True if any of the parts matches a standard ban reason
    return any(part in standard_ban_reasons for part in parts)


# Process each day
for day, file_list in files_by_day.items():
    total_banned_list = []
    validated_banned_list = []
    active_customer_banned_list = []
    no_matching_ban_reason_list = []
    print(f"Date in the loop : {day}")
    
    for file in file_list:
        print(f"File in the loop : {file}\n")
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        # Convert 'Ban Time' and 'Validated Time' to datetime for comparison
        if 'Ban Time' in df.columns and 'Validated Time' in df.columns:
            df['Ban Time'] = pd.to_datetime(df['Ban Time'], errors='coerce')
            df['Validated Time'] = pd.to_datetime(df['Validated Time'], errors='coerce')
        
        # 1. Total banned nodes: rows where 'Banned' is True
        count_total_banned = df[df['Banned'] == True].shape[0]
        print(f"Banned count per file : {count_total_banned}\n")
        total_banned_list.append(count_total_banned)
        
        # 2. Validated banned nodes: conditions applied simultaneously:
        #    - 'Banned' is True
        #    - 'OCI state' equals 'handed_back'
        #    - Both 'Ban Time' and 'Validated Time' are present
        #    - 'Validated Time' > 'Ban Time'
        cond_validated = (
            (df['Banned'] == True) &
            (df['OCI State'] == 'handed_back') &
            (df['Ban Time'].notnull()) &
            (df['Validated Time'].notnull()) &
            (df['Validated Time'] > df['Ban Time'])
        )
        count_validated = df[cond_validated].shape[0]
        print(f"Validated banned nodes count : {count_validated}\n")
        validated_banned_list.append(count_validated)
        
        # 3. Banned nodes with active customer: where both 'Customer Active' and 'Banned' are True
        count_active_banned = df[(df['Customer Active'] == True) & (df['Banned'] == True)].shape[0]
        print(f"Banned nodes with active customer : {count_active_banned}\n")
        active_customer_banned_list.append(count_active_banned)

        # 4. Banned nodes with no matching ban reason:
        #    Count rows where 'Banned' is True and the Ban Reason is either missing or not valid.
        cond_no_matching = (
            (df['Banned'] == True) &
            (df['Customer Active'] != True) &
            (~cond_validated) &
            (~df['OCI State'].isin(excluded_oci_states))&
            (~df['Ban Reason'].apply(is_valid_ban_reason))
        )
        count_no_matching = df[cond_no_matching].shape[0]
        print(f"Banned nodes with no matching ban reason : {count_no_matching}\n")
        no_matching_ban_reason_list.append(count_no_matching)

    
    # Calculate daily averages by dividing the sum of counts by the number of files
    num_files = len(file_list)
    print("")
    print(f"number of files : {num_files}")
    avg_total_banned = sum(total_banned_list) / num_files if num_files else 0
    print("")
    print(f"average total banned  : {avg_total_banned}")
    avg_validated_banned = sum(validated_banned_list) / num_files if num_files else 0
    print("")
    print(f"average validated banned : {avg_validated_banned}")
    avg_active_banned = sum(active_customer_banned_list) / num_files if num_files else 0
    print("")
    print(f"average customer active : {avg_active_banned}")
    avg_no_matching_ban = sum(no_matching_ban_reason_list) / num_files if num_files else 0
    print("")
    print(f"average no matching ban reason : {avg_no_matching_ban}")
    print("")
    
    daily_results[day] = {
        'avg_total_banned_nodes': avg_total_banned,
        'avg_validated_banned_nodes': avg_validated_banned,
        'avg_customer_active_banned_nodes': avg_active_banned,
        'avg_no_matching_ban_reason_nodes': avg_no_matching_ban
    }

# Example: Print the daily results
for day, results in sorted(daily_results.items()):
    print(f"Date: {day}")
    print(f"  Average Total Banned Nodes: {results['avg_total_banned_nodes']}\n")
    print(f"  Average Validated Banned Nodes: {results['avg_validated_banned_nodes']}\n")
    print(f"  Average Active Customer Banned Nodes: {results['avg_customer_active_banned_nodes']}\n")
    print(f"  Average Banned Nodes with No Matching Ban Reason: {results['avg_no_matching_ban_reason_nodes']}\n")

# ------------------------------
# Step 2: Plot the Line Graph
# ------------------------------

# Sort the dates (keys) and convert them to datetime objects for plotting
dates = sorted(daily_results.keys())
dates_dt = [pd.to_datetime(date, format="%Y%m%d") for date in dates]

# Extract the average counts for each metric
total_banned = [daily_results[date]['avg_total_banned_nodes'] for date in dates]
validated_banned = [daily_results[date]['avg_validated_banned_nodes'] for date in dates]
active_banned = [daily_results[date]['avg_customer_active_banned_nodes'] for date in dates]
no_matching_ban = [daily_results[date]['avg_no_matching_ban_reason_nodes'] for date in dates]

# Create the plot: x-axis will now be dates and y-axis the average number of nodes
plt.figure(figsize=(10, 6))

plt.plot(dates_dt, total_banned, marker='o', label='Total Banned Nodes')
plt.plot(dates_dt, validated_banned, marker='o', label='Validated Banned Nodes')
plt.plot(dates_dt, active_banned, marker='o', label='Banned Nodes With Active Customer')
plt.plot(dates_dt, no_matching_ban, marker='o', label='No Matching Ban Reason Nodes')

plt.xlabel("Dates")
plt.ylabel("Average Number of Nodes")
plt.title("Daily Node Statistics")
plt.legend()

plt.tight_layout()
plt.show()