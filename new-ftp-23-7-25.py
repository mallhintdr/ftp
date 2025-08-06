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
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

MAX_GROUP_SIZE = 200 * 1024 * 1024
SUBFOLDER_BATCH_SIZE = 200 * 1024 * 1024


def log(message: str) -> None:
    tqdm.write(message)


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
    batches, batch, batch_size = [], [], 0
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
    log(f"Scanning {len(folder_paths)} folders for grouping...")

    with ThreadPoolExecutor() as executor:
        sizes = list(zip(
            folder_paths,
            tqdm(executor.map(get_directory_size, folder_paths),
                 total=len(folder_paths), desc="Size scan", unit="folder")
        ))

    for folder, folder_size in tqdm(sizes, desc="Grouping", unit="group"):
        if folder_size > max_group_size:
            log(f"[Split] {os.path.basename(folder)} ({folder_size//1024//1024} MB) too large, splitting...")
            for batch in batch_subfolders_by_size(folder, max_group_size):
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
    while path.lower().startswith("public_html") or path.startswith("/"):
        path = path[len("public_html"):].lstrip("/\\")
    return path


def upload_file_ftp(ftp_host, ftp_user, ftp_password, local_file, remote_dir, timeout=120):
    try:
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, 21, timeout=timeout)
        ftp.login(ftp_user, ftp_password)
        ftp.set_pasv(True)
        for part in remote_dir.strip("/").split("/"):
            if part:
                try:
                    ftp.cwd(part)
                except ftplib.error_perm:
                    ftp.mkd(part)
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
    status_dict.setdefault(zip_name, {})
    status_dict[zip_name][step] = True
    save_status(status_path, status_dict)


def pipeline_group(
    group_folders,
    base_dir,
    ftp_host,
    ftp_user,
    ftp_password,
    remote_ftp_dir,
    php_unzip_url,
    clean_remote_folder,
    zip_name,
    zip_path,
    job_index,
    total_jobs,
    status_path,
    status_dict,
    zip_progress,
    upload_progress,
    unzip_progress,
    delete_progress,
):
    """Run zip → upload → unzip → delete pipeline for a single group.

    Individual ``tqdm`` progress bars are provided for each stage so that
    progress is clearly visible even when multiple groups are processed in
    parallel.
    """

    status = status_dict.get(zip_name, {})

    # --- Zip ---
    if status.get("zipped"):
        zip_progress.update(1)
    else:
        log(f"[{job_index}/{total_jobs}] {zip_name}: zipping...")
        zipped = zip_group_7z(group_folders, base_dir, zip_path)
        if not zipped or not os.path.exists(zip_path):
            log(f"[{job_index}/{total_jobs}] {zip_name} ZIP Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "zipped")
        log(f"[{job_index}/{total_jobs}] {zip_name}: zip complete")
        zip_progress.update(1)

    # --- Upload ---
    if status.get("uploaded"):
        upload_progress.update(1)
    else:
        log(f"[{job_index}/{total_jobs}] {zip_name}: uploading...")
        uploaded = upload_file_ftp(
            ftp_host, ftp_user, ftp_password, zip_path, remote_ftp_dir
        )
        if not uploaded:
            log(f"[{job_index}/{total_jobs}] {zip_name} UPLOAD Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "uploaded")
        log(f"[{job_index}/{total_jobs}] {zip_name}: upload complete")
        upload_progress.update(1)

    # --- Unzip ---
    if status.get("unzipped"):
        unzip_progress.update(1)
    else:
        log(f"[{job_index}/{total_jobs}] {zip_name}: triggering unzip...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        unzipped = loop.run_until_complete(
            trigger_unzip(php_unzip_url, clean_remote_folder, zip_name)
        )
        loop.close()
        if not unzipped:
            log(f"[{job_index}/{total_jobs}] {zip_name} UNZIP Failed.")
            return False
        update_status_threadsafe(status_path, status_dict, zip_name, "unzipped")
        log(f"[{job_index}/{total_jobs}] {zip_name}: unzip triggered")
        unzip_progress.update(1)

    # --- Delete remote & local zip ---
    if status.get("deleted"):
        delete_progress.update(1)
    else:
        log(f"[{job_index}/{total_jobs}] {zip_name}: deleting remote zip...")
        try:
            delete_remote_zip(
                ftp_host, ftp_user, ftp_password, clean_remote_folder, zip_name
            )
            update_status_threadsafe(status_path, status_dict, zip_name, "deleted")
            log(f"[{job_index}/{total_jobs}] {zip_name}: remote zip deleted")
        except Exception as e:
            log(f"[{job_index}/{total_jobs}] {zip_name} DELETE REMOTE Failed.")
        delete_progress.update(1)

    if status.get("deleted_local"):
        pass
    else:
        try:
            os.remove(zip_path)
            update_status_threadsafe(status_path, status_dict, zip_name, "deleted_local")
        except Exception as e:
            log(f"[{job_index}/{total_jobs}] {zip_name} DELETE LOCAL Failed.")

    return True


def main():
    logging.basicConfig(filename="hybrid_workflow_7z.log",
                        level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    root = tk.Tk()
    root.withdraw()
    selected_dir = filedialog.askdirectory(title="Select Directory Containing Folders to Process")
    if not selected_dir:
        log("No directory selected. Exiting.")
        return

    all_subfolders = [
        os.path.join(selected_dir, d)
        for d in os.listdir(selected_dir)
        if os.path.isdir(os.path.join(selected_dir, d))
    ]
    if not all_subfolders:
        log("No subfolders found. Exiting.")
        return

    groups = group_folders_by_size(all_subfolders)
    log(f"\nTotal {len(groups)} groups to process.")

    ftp_host = os.getenv("FTP_HOST")
    ftp_user = os.getenv("FTP_USER")
    ftp_password = os.getenv("FTP_PASSWORD")
    remote_base_dir = "Shajra Parcha"
    remote_folder = os.path.join(remote_base_dir, os.path.basename(selected_dir))
    php_unzip_url = "https://dashboard.naqsha-zameen.pk/php/unzip.php"

    if not all([ftp_host, ftp_user, ftp_password]):
        log("ERROR: FTP credentials missing.")
        return

    status_path = os.path.join(selected_dir, "upload_status.json")
    status_dict = load_status(status_path)

    jobs = []
    for idx, group in enumerate(groups, start=1):
        zip_name = f"mauza_group_{idx}.zip"
        zip_path = os.path.join(selected_dir, zip_name)
        jobs.append((group, zip_name, zip_path, idx))

    total_jobs = len(jobs)
    if total_jobs == 0:
        log("All groups already uploaded!")
        return

    clean_remote_folder = remove_double_public_html(remote_folder)
    remote_ftp_dir = f"dashboard/{clean_remote_folder}".replace("//", "/").replace("\\", "/")

    log(f"\nProcessing started (4 parallel pipelines)...\n")
    with ThreadPoolExecutor(max_workers=3) as executor:
        with tqdm(total=total_jobs, desc="Zipping", position=0) as zip_progress, \
                tqdm(total=total_jobs, desc="Uploading", position=1) as upload_progress, \
                tqdm(total=total_jobs, desc="Unzipping", position=2) as unzip_progress, \
                tqdm(total=total_jobs, desc="Deleting", position=3) as delete_progress:
            futures = [
                executor.submit(
                    pipeline_group,
                    group,
                    selected_dir,
                    ftp_host,
                    ftp_user,
                    ftp_password,
                    remote_ftp_dir,
                    php_unzip_url,
                    clean_remote_folder,
                    zip_name,
                    zip_path,
                    idx,
                    total_jobs,
                    status_path,
                    status_dict,
                    zip_progress,
                    upload_progress,
                    unzip_progress,
                    delete_progress,
                )
                for group, zip_name, zip_path, idx in jobs
            ]
            for f in as_completed(futures):
                pass

    log("\nAll groups processed.")


if __name__ == "__main__":
    main()
