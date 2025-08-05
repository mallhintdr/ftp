import os
import logging
import tkinter as tk
from tkinter import filedialog
import asyncio
import aioftp
import aiohttp
import zipfile
import json

CONCURRENCY = 6
RETRY_LIMIT = 2
LOG_FILE = "hybrid_async_workflow.log"

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def setup_logging():
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

def all_files_in_folder(folder):
    files = []
    for root, _, fs in os.walk(folder):
        for f in fs:
            files.append(os.path.join(root, f))
    return files

def zip_folder_to_disk(folder, base_dir, zip_path, job_label=""):
    files = all_files_in_folder(folder)
    total_files = len(files)
    print(f"{job_label} Zipping {total_files} files to {os.path.basename(zip_path)} ...")
    logging.info(f"{job_label} Zipping {total_files} files to {os.path.basename(zip_path)} ...")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, fp in enumerate(files, 1):
            arcname = os.path.relpath(fp, base_dir)
            zf.write(fp, arcname)
            if i % 1000 == 0 or i == total_files:
                msg = f"{job_label} Zipped {i}/{total_files} files ..."
                print(msg)
                logging.info(msg)
    print(f"{job_label} ZIP done ({os.path.basename(zip_path)})")
    logging.info(f"{job_label} ZIP done ({os.path.basename(zip_path)})")
    return zip_path

async def ensure_remote_dir(client, remote_dir):
    parts = remote_dir.strip('/').split('/')
    for part in parts:
        try:
            await client.change_directory(part)
        except aioftp.StatusCodeError:
            await client.make_directory(part)
            await client.change_directory(part)

async def upload_file_ftp(host, user, password, file_path, remote_dir, filename, job_label=""):
    total = os.path.getsize(file_path)
    print(f"{job_label} Uploading {filename} ({total // (1024*1024)} MB) ...")
    logging.info(f"{job_label} Uploading {filename} ({total // (1024*1024)} MB) ...")
    async with aioftp.Client.context(host, user, password) as client:
        await client.change_directory('/public_html/dashboard')
        await ensure_remote_dir(client, remote_dir)
        with open(file_path, "rb") as f:
            await client.upload_stream(f, filename)
    print(f"{job_label} Upload done ({filename})")
    logging.info(f"{job_label} Upload done ({filename})")
    return True

async def trigger_unzip(php_url, remote_dir, zip_filename, job_label=""):
    print(f"{job_label} Requesting unzip for {zip_filename} ...")
    logging.info(f"{job_label} Requesting unzip for {zip_filename} ...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(php_url, params={'path': remote_dir, 'file': zip_filename}) as resp:
                await resp.text()
                ok = resp.status == 200
        except Exception as e:
            logging.error(f"{job_label} Error triggering unzip for {zip_filename}: {e}")
            ok = False
    print(f"{job_label} Unzip {'done' if ok else 'failed'} ({zip_filename})")
    logging.info(f"{job_label} Unzip {'done' if ok else 'failed'} ({zip_filename})")
    return ok

async def delete_remote_zip(host, user, password, remote_dir, filename, job_label=""):
    print(f"{job_label} Deleting remote ZIP {filename} ...")
    logging.info(f"{job_label} Deleting remote ZIP {filename} ...")
    async with aioftp.Client.context(host, user, password) as client:
        await client.change_directory('/public_html/dashboard')
        for part in remote_dir.strip('/').split('/'):
            await client.change_directory(part)
        await client.remove_file(filename)
    print(f"{job_label} Remote ZIP deleted ({filename})")
    logging.info(f"{job_label} Remote ZIP deleted ({filename})")
    return True

async def worker(queue, base_dir, ftp_host, ftp_user, ftp_password, remote_folder, php_unzip_url, status_path, status_dict, total_jobs):
    while True:
        item = await queue.get()
        if item is None:
            break
        folder, job_index = item
        foldername = os.path.basename(folder)
        zip_name = f"{foldername}.zip"
        job_label = f"[{job_index}/{total_jobs}] {zip_name}:"
        zip_path = os.path.join(base_dir, zip_name)
        retry_count = 0
        success = False

        while not success and retry_count <= RETRY_LIMIT:
            try:
                # ZIP
                if not status_dict.get(zip_name, {}).get("zipped"):
                    zip_folder_to_disk(folder, base_dir, zip_path, job_label)
                    status_dict.setdefault(zip_name, {})["zipped"] = True
                    with open(status_path, "w") as f:
                        json.dump(status_dict, f, indent=2)
                else:
                    print(f"{job_label} ZIP ✓ (skip)")
                    logging.info(f"{job_label} ZIP ✓ (skip)")

                # UPLOAD
                if not status_dict.get(zip_name, {}).get("uploaded"):
                    await upload_file_ftp(ftp_host, ftp_user, ftp_password, zip_path, remote_folder, zip_name, job_label)
                    status_dict[zip_name]["uploaded"] = True
                    with open(status_path, "w") as f:
                        json.dump(status_dict, f, indent=2)
                else:
                    print(f"{job_label} Upload ✓ (skip)")
                    logging.info(f"{job_label} Upload ✓ (skip)")

                # UNZIP
                if not status_dict.get(zip_name, {}).get("unzipped"):
                    ok = await trigger_unzip(php_unzip_url, remote_folder, zip_name, job_label)
                    if not ok:
                        raise Exception("Unzip failed")
                    status_dict[zip_name]["unzipped"] = True
                    with open(status_path, "w") as f:
                        json.dump(status_dict, f, indent=2)
                else:
                    print(f"{job_label} Unzip ✓ (skip)")
                    logging.info(f"{job_label} Unzip ✓ (skip)")

                # DELETE REMOTE
                if not status_dict.get(zip_name, {}).get("deleted_remote"):
                    await delete_remote_zip(ftp_host, ftp_user, ftp_password, remote_folder, zip_name, job_label)
                    status_dict[zip_name]["deleted_remote"] = True
                    with open(status_path, "w") as f:
                        json.dump(status_dict, f, indent=2)
                else:
                    print(f"{job_label} Remote Delete ✓ (skip)")
                    logging.info(f"{job_label} Remote Delete ✓ (skip)")

                # DELETE LOCAL ZIP
                if not status_dict.get(zip_name, {}).get("deleted_local"):
                    os.remove(zip_path)
                    status_dict[zip_name]["deleted_local"] = True
                    with open(status_path, "w") as f:
                        json.dump(status_dict, f, indent=2)
                    print(f"{job_label} Local Delete Done")
                    logging.info(f"{job_label} Local Delete Done")
                else:
                    print(f"{job_label} Local Delete ✓ (skip)")
                    logging.info(f"{job_label} Local Delete ✓ (skip)")

                success = True
            except Exception as e:
                print(f"{job_label} Error: {e}")
                logging.error(f"{job_label} Error: {e}")
                retry_count += 1
                if retry_count <= RETRY_LIMIT:
                    print(f"{job_label} Retrying... ({retry_count}/{RETRY_LIMIT})")
                    logging.info(f"{job_label} Retrying... ({retry_count}/{RETRY_LIMIT})")
                    await asyncio.sleep(2)
                else:
                    print(f"{job_label} FAILED after {RETRY_LIMIT} retries.")
                    logging.error(f"{job_label} FAILED after {RETRY_LIMIT} retries.")

        queue.task_done()

def main():
    clear_console()
    setup_logging()
    print("==== Hybrid Async Zip/FTP/Unzip Pipeline Started ====\n")
    logging.info("Hybrid Async Zip/FTP/Unzip Pipeline Started.")

    root = tk.Tk()
    root.withdraw()
    selected_dir = filedialog.askdirectory(title="Select Directory Containing Folders to Process")
    if not selected_dir:
        print("No directory selected. Exiting.")
        logging.info("No directory selected. Exiting.")
        return

    all_subfolders = [
        os.path.join(selected_dir, d)
        for d in os.listdir(selected_dir)
        if os.path.isdir(os.path.join(selected_dir, d))
    ]
    if not all_subfolders:
        print("No subfolders found. Exiting.")
        logging.info("No subfolders found. Exiting.")
        return

    print(f"\nTotal {len(all_subfolders)} folders to process.")
    logging.info(f"Total {len(all_subfolders)} folders to process.")

    ftp_host = "ftp.gb.stackcp.com"
    ftp_user = "naqsha-zameen.pk"
    ftp_password = "03146796144@gG"
    remote_base_dir = "Shajra Parcha"
    remote_folder = remote_base_dir + '/' + os.path.basename(selected_dir)
    php_unzip_url = "https://dashboard.naqsha-zameen.pk/php/unzip.php"

    status_path = os.path.join(selected_dir, "upload_status.json")
    if os.path.exists(status_path):
        with open(status_path, "r") as f:
            status_dict = json.load(f)
    else:
        status_dict = {}

    async def main_async():
        queue = asyncio.Queue()
        for idx, folder in enumerate(all_subfolders, 1):
            await queue.put((folder, idx))

        workers = [
            asyncio.create_task(
                worker(queue, selected_dir, ftp_host, ftp_user, ftp_password, remote_folder,
                       php_unzip_url, status_path, status_dict, len(all_subfolders))
            ) for _ in range(CONCURRENCY)
        ]
        await queue.join()
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
        print("\nAll folders processed.")
        logging.info("All folders processed.")

    asyncio.run(main_async())

if __name__ == "__main__":
    main()
