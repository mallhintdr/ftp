import os
import logging
import tkinter as tk
from tkinter import filedialog
import asyncio
import subprocess
import ftplib
import aiohttp
from dotenv import load_dotenv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

MAX_GROUP_SIZE = 200 * 1024 * 1024
SUBFOLDER_BATCH_SIZE = 200 * 1024 * 1024

def get_directory_size(directory):
    total = 0
    for root, _, files in os.walk(directory):
        for file in files:
            try:
                total += os.path.getsize(os.path.join(root, file))
            except Exception as e:
                logging.error(f"Error getting size for {file} in {root}: {e}")
    return total

def batch_subfolders_by_size(folder, max_size=SUBFOLDER_BATCH_SIZE):
    all_items = []
    for entry in os.listdir(folder):
        path = os.path.join(folder, entry)
        size = get_directory_size(path) if os.path.isdir(path) else os.path.getsize(path)
        all_items.append((path, size))
    all_items.sort(key=lambda x: -x[1])
    batches = []
    batch, batch_size = [], 0
    for path, size in all_items:
        if size > max_size:
            if batch:
                batches.append(batch)
            batches.append([path])
            batch, batch_size = [], 0
        elif batch_size + size <= max_size:
            batch.append(path)
            batch_size += size
        else:
            if batch:
                batches.append(batch)
            batch, batch_size = [path], size
    if batch:
        batches.append(batch)
    return batches

def group_folders_by_size(folder_paths, max_group_size=MAX_GROUP_SIZE):
    groups = []
    current_group, current_size = [], 0
    print(f"Scanning {len(folder_paths)} folders for grouping...")
    folder_sizes = []
    for folder in tqdm(folder_paths, desc="Size scan", unit="folder"):
        folder_sizes.append((folder, get_directory_size(folder)))
    for folder, folder_size in tqdm(folder_sizes, desc="Grouping", unit="group"):
        if folder_size > max_group_size:
            print(f"[Split] {os.path.basename(folder)} ({folder_size//1024//1024} MB) too large, splitting...")
            batches = batch_subfolders_by_size(folder, max_group_size)
            for batch in batches:
                groups.append(batch)
        elif current_size + folder_size <= max_group_size:
            current_group.append(folder)
            current_size += folder_size
        else:
            groups.append(current_group)
            current_group, current_size = [folder], folder_size
    if current_group:
        groups.append(current_group)
    return groups

def zip_group_7z(folders, base_dir, zip_path, timeout=900):
    rel_paths = [os.path.relpath(folder, base_dir) for folder in folders]
    try:
        cmd = [r"C:\Program Files\7-Zip\7z.exe", "a", "-tzip", "-mx1", zip_path] + rel_paths
        subprocess.run(cmd, cwd=base_dir, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        return True
    except Exception as e:
        logging.error(f"Error zipping group to {zip_path}: {e}")
        return False

def remove_double_public_html(path):
    while True:
        norm = path.lstrip("/\\")
        if norm.lower().startswith("public_html"):
            path = norm[10:]
        else:
            break
    return path.lstrip("/\\")

def upload_file_ftp(ftp_host, ftp_user, ftp_password, local_file, remote_dir, timeout=120):
    try:
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, 21, timeout=timeout)
        ftp.login(ftp_user, ftp_password)
        ftp.set_pasv(True)
        for part in remote_dir.strip("/").split("/"):
            if part == "":
                continue
            try:
                ftp.cwd(part)
            except ftplib.error_perm:
                try:
                    ftp.mkd(part)
                    ftp.cwd(part)
                except Exception:
                    ftp.cwd(part)
        with open(local_file, 'rb') as f:
            ftp.storbinary("STOR " + os.path.basename(local_file), f, blocksize=8192)
        ftp.quit()
        return True
    except Exception as e:
        logging.error(f"Error uploading {local_file}: {e}")
        return False

def delete_remote_zip(host, user, password, remote_dir, filename, port=21):
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host, port, timeout=60)
            ftp.login(user, password)
            ftp.cwd("dashboard")
            for part in remote_dir.strip('/').split('/'):
                ftp.cwd(part)
            ftp.delete(filename)
    except Exception as e:
        logging.error(f"Error deleting remote zip {filename}: {e}")

async def trigger_unzip(php_url, remote_dir, zip_filename):
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(php_url, params={'path': remote_dir, 'file': zip_filename}) as resp:
                return resp.status == 200
        except Exception as e:
            logging.error(f"Error triggering unzip for {zip_filename}: {e}", exc_info=True)
            return False

def load_status(status_path):
    if os.path.exists(status_path):
        try:
            with open(status_path, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_status(status_path, status_dict):
    with open(status_path, "w") as f:
        json.dump(status_dict, f, indent=2)

def update_status_threadsafe(status_path, status_dict, zip_name, step):
    # Use this for thread/process safe status writes
    status_dict.setdefault(zip_name, {})
    status_dict[zip_name][step] = True
    save_status(status_path, status_dict)

def pipeline_group(
    group_folders, base_dir, ftp_host, ftp_user, ftp_password, remote_ftp_dir,
    php_unzip_url, clean_remote_folder, zip_name, zip_path, job_index, total_jobs,
    status_path, status_dict, progress
):
    status = status_dict.get(zip_name, {})
    if not isinstance(status, dict):
        status = {}
        status_dict[zip_name] = status

    # ZIP
    if status.get("zipped"):
        progress.update(1)
    else:
        zipped = zip_group_7z(group_folders, base_dir, zip_path)
        if not zipped or not os.path.exists(zip_path):
            print(f"[{job_index}/{total_jobs}] {zip_name} ZIP Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "zipped")
        progress.update(1)

    # UPLOAD
    if status.get("uploaded"):
        progress.update(1)
    else:
        uploaded = upload_file_ftp(ftp_host, ftp_user, ftp_password, zip_path, remote_ftp_dir)
        if not uploaded:
            print(f"[{job_index}/{total_jobs}] {zip_name} UPLOAD Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "uploaded")
        progress.update(1)

    # UNZIP (async, but wrapped synchronously)
    if status.get("unzipped"):
        progress.update(1)
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        unzipped = loop.run_until_complete(trigger_unzip(php_unzip_url, clean_remote_folder, zip_name))
        loop.close()
        if not unzipped:
            print(f"[{job_index}/{total_jobs}] {zip_name} UNZIP Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "unzipped")
        progress.update(1)

    # DELETE REMOTE ZIP
    if status.get("deleted"):
        progress.update(1)
    else:
        try:
            delete_remote_zip(ftp_host, ftp_user, ftp_password, clean_remote_folder, zip_name)
            update_status_threadsafe(status_path, status_dict, zip_name, "deleted")
            progress.update(1)
        except Exception:
            print(f"[{job_index}/{total_jobs}] {zip_name} DELETE REMOTE Failed.")

    # DELETE LOCAL ZIP
    if status.get("deleted_local"):
        progress.update(1)
    else:
        try:
            os.remove(zip_path)
            update_status_threadsafe(status_path, status_dict, zip_name, "deleted_local")
            progress.update(1)
        except Exception:
            print(f"[{job_index}/{total_jobs}] {zip_name} DELETE LOCAL Failed.")
    return True

def main():
    logging.basicConfig(
        filename="hybrid_workflow_7z.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    root = tk.Tk()
    root.withdraw()
    selected_dir = filedialog.askdirectory(title="Select Directory Containing Folders to Process")
    if not selected_dir:
        print("No directory selected. Exiting.")
        return

    all_subfolders = [
        os.path.join(selected_dir, d)
        for d in os.listdir(selected_dir)
        if os.path.isdir(os.path.join(selected_dir, d))
    ]
    if not all_subfolders:
        print("No subfolders found. Exiting.")
        return

    groups = group_folders_by_size(all_subfolders)
    print(f"\nTotal {len(groups)} groups to process.")

    ftp_host = os.getenv("FTP_HOST")
    ftp_user = os.getenv("FTP_USER")
    ftp_password = os.getenv("FTP_PASSWORD")
    remote_base_dir = "Shajra Parcha"
    remote_folder = remote_base_dir + '/' + os.path.basename(selected_dir)
    php_unzip_url = "https://dashboard.naqsha-zameen.pk/php/unzip.php"

    if not all([ftp_host, ftp_user, ftp_password]):
        print("ERROR: FTP credentials missing.")
        return

    status_path = os.path.join(selected_dir, "upload_status.json")
    status_dict = load_status(status_path)

    jobs = []
    group_idx = 1
    for group in groups:
        zip_name = f"mauza_group_{group_idx}.zip"
        zip_path = os.path.join(selected_dir, zip_name)
        jobs.append((group, zip_name, zip_path, group_idx))
        group_idx += 1

    total_jobs = len(jobs)
    if total_jobs == 0:
        print("All groups already uploaded!")
        return

    clean_remote_folder = remove_double_public_html(remote_folder)
    remote_ftp_dir = f"dashboard/{clean_remote_folder}".replace("//", "/").replace("\\", "/")

    print(f"\nProcessing started (4 parallel pipelines)...\n")
    with ThreadPoolExecutor(max_workers=4) as executor:
        with tqdm(total=total_jobs * 5, desc="Groups Progress (all steps)", unit="step") as progress:
            futures = [
                executor.submit(
                    pipeline_group,
                    group, selected_dir, ftp_host, ftp_user, ftp_password, remote_ftp_dir,
                    php_unzip_url, clean_remote_folder,
                    zip_name, zip_path, idx, total_jobs, status_path, status_dict, progress
                )
                for group, zip_name, zip_path, idx in jobs
            ]
            for f in as_completed(futures):
                pass

    print("\nAll groups processed.")

if __name__ == "__main__":
    main()
