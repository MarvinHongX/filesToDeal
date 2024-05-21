# files_to_deal.py
#########################################################################################
# Author  : Hong
# Created : 5/8/2024
# Modified: 5/21/2024
# Notes   :
#########################################################################################
import time
import shutil
import tarfile
import os
import pyAesCrypt
import sys
import datetime
import subprocess
from dotenv import load_dotenv

# .env file
load_dotenv()


def bytes_to_gib(bytes):
    gib = bytes / (1024 ** 3)  # 1 GiB = 1024^3 bytes
    return gib


def get_next_file_number(file_prefix, target_dir):
    max_file_number = 0
    found_files = False

    for filename in os.listdir(target_dir):
        if filename.startswith(file_prefix):
            found_files = True
            file_parts = filename.split('-')
            if len(file_parts) == 2 and (".tar" in file_parts[1]):
                try:
                    file_number = int(file_parts[1].split('.')[0])
                    max_file_number = max(max_file_number, file_number)
                except ValueError:
                    pass  # Ignore if the file number is invalid.

    if found_files:
        return max_file_number + 1
    else:
        return 1


def get_rm_number(file_number):
    if file_number % 10 == 1:
        new_number = file_number - 10
        new_file_number = f"{new_number // 10:05d}*"
        return new_file_number
    return None


def get_log_time():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")


def log_message(level, message):
    log_time = get_log_time()
    prefix = {
        'INFO': 'INFO',
        'WARN': 'WARNING',
        'ERROR': 'ERROR'
    }.get(level, 'INFO')

    print(f"{log_time}\t{prefix}\t{message}")
    sys.stdout.flush()


def sort_user_folders(source_dir):
    user_folders = [f for f in os.listdir(source_dir) if os.path.isdir(os.path.join(source_dir, f))]
    sorted_user_folders = sorted(user_folders, key=str.lower, reverse=False)
    return sorted_user_folders


def create_or_update_job_cur(sh_dir, selected_user_id, selected_file_name):
    job_cur_path = os.path.join(sh_dir, "job.cur")
    if not os.path.exists(job_cur_path):
        with open(job_cur_path, 'w') as job_cur_file:
            job_cur_file.write(f"{selected_user_id}\t{selected_file_name}\n")
    else:
        with open(job_cur_path, 'r+') as job_cur_file:
            lines = job_cur_file.readlines()
            if not lines:  # If file is empty
                job_cur_file.write(f"{selected_user_id}\t{selected_file_name}\n")
            else:
                # Update the first line
                job_cur_file.seek(0)
                job_cur_file.write(f"{selected_user_id}\t{selected_file_name}\n")
                job_cur_file.truncate()


def get_selected_user_and_file(source_dir, sh_dir, sorted_user_folders):
    selected_user_id = None
    selected_file_name = None
    job_cur_path = os.path.join(sh_dir, "job.cur")

    if os.path.exists(job_cur_path):
        with open(job_cur_path, 'r') as job_cur_file:
            lines = job_cur_file.readlines()
            if lines:
                first_line = lines[0].strip()
                selected_user_id, selected_file_name = first_line.split('\t')

    if not selected_user_id:
        selected_user_id = sorted_user_folders[0] if sorted_user_folders else None
        if selected_user_id:
            user_folder_path = os.path.join(source_dir, selected_user_id, "files")
            if os.path.exists(user_folder_path) and os.path.isdir(user_folder_path):
                user_files = [os.path.join(user_folder_path, file) for file in os.listdir(user_folder_path)]
                selected_file_name = user_files[0] if user_files else None

    return selected_user_id, selected_file_name


def get_payload_cid(car_file_path):
    car_path = os.getenv("CAR_PATH")
    payload_cid_command = [car_path, "root", car_file_path]
    payload_cid_process = subprocess.Popen(payload_cid_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    payload_cid_output, _ = payload_cid_process.communicate()
    payload_cid = payload_cid_output.decode('utf-8').strip()

    return payload_cid


def get_commp_info(car_file_path):
    boostx_command = ["boostx", "commp", car_file_path]
    boostx_process = subprocess.Popen(boostx_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    boostx_output, _ = boostx_process.communicate()
    boostx_output = boostx_output.decode('utf-8')
    lines = boostx_output.split('\n')

    log_message("INFO", f"boostx command: {boostx_command}")
    log_message("INFO", f"boostx command excuted:\n {boostx_output}")

    commp_cid = None
    piece_size = None
    car_file_size = None

    if len(lines) >= 3:
        commp_cid = lines[0].split(':')[1].strip() if len(lines[0].split(':')) > 1 else None
        piece_size = lines[1].split(':')[1].strip() if len(lines[1].split(':')) > 1 else None
        car_file_size = lines[2].split(':')[1].strip() if len(lines[2].split(':')) > 1 else None


    return commp_cid, piece_size, car_file_size


def write_deal_commands(deal_file, miner_ids, archive_dir_name, commp_cid, piece_size, car_file_size, payload_cid, wallet_address):
    web_server_ip = os.getenv("WEB_SERVER_IP")
    for miner_id in miner_ids:
        time.sleep(2)
        deal_file.write(f"boost -vv deal --verified=true --provider={miner_id} "
                        f"--http-url=http://{web_server_ip}/http/{archive_dir_name}.tar.aes.car "
                        f"--commp={commp_cid} "
                        f"--car-size={car_file_size} "
                        f"--piece-size={piece_size} "
                        f"--payload-cid={payload_cid} "
                        f"--duration=1555200 --wallet={wallet_address}\n")


def get_miner_ids(last_4_digits):
    if last_4_digits % 2 == 0:  # Even
        return [
            os.getenv("MINER02"),
            os.getenv("MINER03"),
            os.getenv("MINER04"),
            os.getenv("MINER05")
        ]
    else:  # Odd
        return [
            os.getenv("MINER01"),
            os.getenv("MINER02"),
            os.getenv("MINER05"),
            os.getenv("MINER06")
        ]


def read_commands_from_file(file_path):
    with open(file_path, 'r') as file:
        commands = file.readlines()
    return commands


def find_first_deal_file(target_dir):
    first_file = None

    deal_files = [file for file in os.listdir(target_dir) if file.endswith('.deal')]
    deal_files.sort(reverse=False)

    if deal_files:
        first_file = os.path.join(target_dir, deal_files[0])

    return first_file


def compare_commp_cid(file_path, commp_cid):
    commp_cid_1 = get_commp_cid(file_path)
    commp_cid_2 = commp_cid

    log_message("INFO", f"commp_cid1: {commp_cid_1}")
    log_message("INFO", f"commp_cid2: {commp_cid_2}")

    return commp_cid_1 == commp_cid_2


def get_commp_cid(file_path):
    boostx_command = ["boostx", "commp", file_path]
    boostx_process = subprocess.Popen(boostx_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    boostx_output, _ = boostx_process.communicate()
    boostx_output = boostx_output.decode('utf-8')
    lines = boostx_output.split('\n')

    log_message("INFO", f"boostx command: {boostx_command}")
    log_message("INFO", f"boostx command excuted:\n {boostx_output}")

    commp_cid = lines[0].split(':')[1].strip()
    log_message("INFO", f"commp_cid: {commp_cid}")

    return commp_cid


def files_to_archive():
    server_id = os.getenv("SERVER_ID")
    sh_dir = os.getenv("SH_DIR")
    source_dir = os.getenv("SOURCE_DIR")
    target_dir = os.getenv("TARGET_DIR")
    time_diff = float(os.getenv("TIME_DIFF"))
    max_size = float(os.getenv("MAX_SIZE"))
    min_size = float(os.getenv("MIN_SIZE"))
    password = os.getenv("PASSWORD")
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    target_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=time_diff) # time ago from now
    target_files = sorted([f for f in os.listdir(source_dir)], reverse=False)
    selected_files = []
    total_size = 0.0

    log_message("INFO", f"Initiating Archive Process for {source_dir}")


    # Create directories if they don't exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        log_message("INFO", f"Initiating target dir {target_dir}")


    sorted_user_folders = sort_user_folders(source_dir)
    
    log_message("INFO", f"source_dir, sorted_user_folders {source_dir} {sorted_user_folders}")

    selected_user_id, selected_file_name = get_selected_user_and_file(source_dir, sh_dir, sorted_user_folders)
    log_message("INFO", f"selected_user_id: {selected_user_id}")
    log_message("INFO", f"selected_file_name: {selected_file_name}")
    

    # Define a flag to indicate if the last selected file has been encountered
    file_encountered = False

    for user_id in sorted_user_folders[sorted_user_folders.index(selected_user_id):]:
        user_folder_path = os.path.join(source_dir, user_id, "files")
        # Exit loop if total size meets the minimum size condition
        if total_size >= min_size * (1024 ** 3):
            break

        if os.path.exists(user_folder_path) and os.path.isdir(user_folder_path):
            for root, dirs, files in os.walk(user_folder_path):
                # Exit loop if total size meets the minimum size condition
                if total_size >= min_size * (1024 ** 3):
                    break

                for file_name in files:
                    # Exit loop if total size meets the minimum size condition
                    if total_size >= min_size * (1024 ** 3):
                        break

                    file_path = os.path.join(root, file_name)
                    file_size = os.path.getsize(file_path)
                    file_m_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)) # File modification time
                    is_target = file_m_time < target_hours_ago


                    if file_encountered:
                        file = {
                            'file_path': file_path,
                            'file_name': file_name,
                            'file_size': file_size,
                            'file_m_time': file_m_time,
                            'is_target': is_target
                        }

                        if is_target:
                            if (total_size + file_size) <= max_size * (1024 ** 3):
                                selected_files.append(file)
                                log_message("INFO", f"appending files: {file}")
                                total_size += file_size
                                selected_user_id = user_id
                                selected_file_name = file_path
                            else:
                                log_message("INFO", "Skipping file addition, max_size exceeded")

	                # Check if the current file is the last selected file
                    if file_path == selected_file_name:
                        file_encountered = True

    if not selected_files:
        log_message("WARN", "No files selected")
        return


    if total_size < min_size * (1024 ** 3):
        log_message("WARN", f"Not enough files collected. total size is {total_size} ({bytes_to_gib(total_size)}GiB)")
        return

    log_message("INFO", f"total_size is {total_size} ({bytes_to_gib(total_size)}GiB)")

    # Create archive
    file_prefix = f"{timestamp}{server_id}-"
    archive_file_number = get_next_file_number(file_prefix, target_dir)
    archive_dir_name = f"{file_prefix}{archive_file_number:05}"
    archive_file_name = f"{archive_dir_name}.tar"
    aes_archive_file_name = f"{archive_dir_name}.tar.aes"
    deal_file_name = f"{archive_dir_name}.deal"
    archive_file_path = os.path.join(target_dir, archive_file_name)
    aes_archive_file_path = os.path.join(target_dir, aes_archive_file_name)
    deal_file_path = os.path.join(target_dir, deal_file_name)
    selected_file_paths = [file['file_path'] for file in selected_files]

   
    rm_number = get_rm_number(archive_file_number)
    if rm_number:
        rm_dir_name = f"{file_prefix}{rm_number}"
        rm_command = f"rm {os.path.join(target_dir, rm_dir_name)}"
        log_message("INFO", f"Executing rm command: {rm_command}")        
        subprocess.run(rm_command, shell=True)


    log_message('INFO', f"Generating archive name: {archive_file_name}")
    
    try:
        log_message('INFO', f"Generating archive {selected_file_paths}")

        with tarfile.open(archive_file_path, "w") as tar:
            for selected_file in selected_files:
                file_path = selected_file['file_path']
                tar.add(file_path, arcname=selected_file['file_name'])

        buffer_size = 64 * 1024
        log_message('INFO', f"archive completed. {archive_file_path}")

        pyAesCrypt.encryptFile(archive_file_path, aes_archive_file_path, password, buffer_size)
        os.remove(archive_file_path)

        log_message('INFO', f"aes archive completed. {aes_archive_file_path}")
    except Exception as e:
        log_message('ERROR', f"Error compressing files: {e}")
        return

    # Create car
    car_file_name = f"{archive_dir_name}.tar.aes.car"
    car_file_path = os.path.join(target_dir, car_file_name)
    car_path = os.getenv("CAR_PATH")
    car_command = [car_path, "create", "-f", car_file_path, "--version", "1", aes_archive_file_path]

    try:
        subprocess.run(car_command, check=True)
        os.remove(aes_archive_file_path)
        time.sleep(3)
        log_message("INFO", f"CAR file created: {car_file_name}")
    except subprocess.CalledProcessError as e:
        log_message("ERROR", f"Error creating CAR file: {e}")
        return


    # Create deal
    deal_file_name = f"{archive_dir_name}.deal"
    deal_file_path = os.path.join(target_dir, deal_file_name)
    commp_cid = ''

    wallet_address = os.getenv("WALLET_ADDRESS")
    last_5_digits = int(archive_dir_name[-5:])
    miner_ids = get_miner_ids(last_5_digits)

    log_message("INFO", f"{miner_ids} {wallet_address} {car_file_path}")

    with open(deal_file_path, 'w') as deal_file:
        # Step 1: Get payload CID
        payload_cid = get_payload_cid(car_file_path)
        log_message("INFO", f"payload cid: {payload_cid}")

        # Step 2: Get CommP CID, piece size, and CAR file size
        commp_cid, piece_size, car_file_size = get_commp_info(car_file_path)
        log_message("INFO", f"commp_cid: {commp_cid}")
        log_message("INFO", f"piece_size: {piece_size}")
        log_message("INFO", f"car_file_size: {car_file_size}")


        # Step 3: Write deal information to the file
        write_deal_commands(deal_file, miner_ids, archive_dir_name, commp_cid, piece_size, car_file_size, payload_cid, wallet_address)

    log_message("INFO", f"Deal file completed. {deal_file_path}")


    if deal_file_path:
        log_message("INFO", f"Found *.deal file: {deal_file_path}")

        # Find corresponding .tar.aes.car file
        if not os.path.exists(car_file_path):
            log_message("ERROR", f"No corresponding .tar.aes.car file found for {car_file_path}")
            return

        # Compare CommP CID
        if not compare_commp_cid(car_file_path, commp_cid):
            log_message("ERROR", f"Commp CID mismatch: {car_file_path}")

            # Delete
            if os.path.exists(deal_file_path):
                os.remove(deal_file_path)
                log_message("INFO", f"Deleted {deal_file_path}")

            if os.path.exists(car_file_path):
                os.remove(car_file_path)
                log_message("INFO", f"Deleted {car_file_path}")

            return

        log_message("INFO", "Commp CID matches.")



        # Execute commands in the .deal file
        commands = read_commands_from_file(deal_file_path)
        for i, command in enumerate(commands, start=1):
            log_message("INFO", f"Command {i}: {command.strip()}")

            try:
                subprocess.run(command.strip(), shell=True, check=True)
                log_message("INFO", f"Command {i} executed successfully.")
            except subprocess.CalledProcessError as e:
                log_message("ERROR", f"Error executing command {i}: {e}")
                return

        os.rename(deal_file_path, deal_file_path.replace('.deal', '.done'))
        log_message("INFO", f"Renamed {deal_file_path} to {deal_file_path.replace('.deal', '.done')}")
    
        #update job cur
        create_or_update_job_cur(sh_dir, selected_user_id, selected_file_name)
    else:
        log_message("WARN", "No *.deal files found in the target directory.")
        return



if __name__ == '__main__':
    files_to_archive()
