import os
import subprocess
from datetime import datetime, timedelta

# Base URL for the object storage (ensure it ends with a slash)
BASE_URL = "File storage URL"

# Folder where files will be downloaded
DOWNLOAD_FOLDER = "Local folder to store files"

def ensure_download_folder(folder):
    """Ensure that the download folder exists."""
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Created download folder: {folder}")
    else:
        print(f"Download folder already exists: {folder}")

def download_file_for_hour(date_obj, hour, download_folder):
    """
    For a given date and hour, try incrementing one second at a time to find a file.
    As soon as a file is successfully downloaded, move to the next hour.
    """
    found = False
    # There are 3600 seconds in an hour, we try from 1 to 3599 seconds.
    for sec_offset in range(1, 20):
        # Convert seconds offset into minute and second
        print(f"seconds : {sec_offset}")
        minute = sec_offset // 60
        second = sec_offset % 60
        timestamp = f"{date_obj.strftime('%Y%m%d')}_{hour:02d}{minute:02d}{second:02d}"
        filename = f"node_info_{timestamp}.csv"
        print(f"filename : {filename}")
        url = BASE_URL + filename
        print(f"Trying {filename}...", end=" ")
        # Use curl with the -f flag so that it fails if the file does not exist
        cmd = ["curl", "-f", "-O", url]
        result = subprocess.run(cmd, cwd=download_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print("Found and downloaded!")
            found = True
            break  # Found the file for this hour; move to the next hour.
        else:
            # File not found, continue trying the next second.
            # Uncomment the next line for detailed error info if needed.
            # print(f"Not found: {result.stderr.strip()}")
            continue
    if not found:
        print(f"No file found for {date_obj.strftime('%Y-%m-%d')} hour {hour:02d}")

def main():
    ensure_download_folder(DOWNLOAD_FOLDER)

    # Define the date range: from December 1, 2024 to February 26, 2025.
    start_date = datetime(2025, 2, 27)
    end_date = datetime(2025, 2, 27)
    current_date = start_date

    while current_date <= end_date:
        print(f"\nProcessing date: {current_date.strftime('%Y-%m-%d')}")
        # Iterate over each hour (0 through 23)
        for hour in range(24):
            print(f"hours : {hour}")
            download_file_for_hour(current_date, hour, DOWNLOAD_FOLDER)
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
