import os
import wget
import zipfile

def make_destination():
    # Create a folder named "files" in the current directory
    folder_path = os.path.join(os.getcwd(), "files")
    try:
        os.makedirs(folder_path, exist_ok=True)
        return folder_path
    except OSError as e:
        print(f"Failed to create destination folder: {folder_path}")
        return None

def get_files(url, destination):
    if destination is None:
        print("Destination folder not available.")
        return

    try:
        # Check if the file already exists in the destination folder
        file_name = url.split("/")[-1]
        file_path = os.path.join(destination, file_name)
        print("Checking if file already exists...")
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
        else:
            # Download the file using wget
            print("File not found")
            print("File downloading...")
            file_path = wget.download(url, out=destination)
            print(f"File downloaded successfully: {file_path}")

        # Extract the file using zipfile
        print("Extracting file...")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(destination)
        print("File successfully extracted")

        # Delete the zip file
        os.remove(file_path)
        print("Zip file deleted")

        # Set the location of the CSV file
        csv_destination = os.path.join(destination, "Most-Recent-Cohorts-Institution.csv")
        return csv_destination

    except Exception as e:
        print(f"Failed to download file from: {url}")
        print(e)

def download_local():
    url = "https://ed-public-download.app.cloud.gov/downloads/Most-Recent-Cohorts-Institution_04192023.zip"
    
    destination = make_destination()
    csv_destination = get_files(url, destination)

    return csv_destination

csv_path = download_local()
