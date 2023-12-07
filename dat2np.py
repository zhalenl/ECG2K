import sys
from pathlib import Path
import pandas as pd
from pprint import pprint
import wfdb
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import os
import multiprocessing as mp
import json
import pickle
from datetime import datetime, timedelta

required_sigs = ["II"]
main_folders = ["p00", "p01", "p02", "p03", "p04", "p05", "p06", "p07", "p08", "p09"]
main_path = "/ssd-shared/physionet.org/files/mimic3wdb-matched/1.0/"
segments_list = []
PID_list = []
base_time_list = []
base_date_list = []
segment_time_list = []
segment_len_list = []
signal_list = []
segment_path_list = []
segment_date_list = []

patients_w_multiple_lead2 = []
for folder in tqdm(main_folders):
    new_path = f"{main_path}{folder}/"
    patient_folders = os.listdir(new_path)

    for patient_folder in tqdm(patient_folders):
        if patient_folder[-4:] == "html":
            continue
        patient_path = f"{new_path}{patient_folder}"
        patient_files = os.listdir(patient_path)

        for file in patient_files:
            if (file[:3] == folder) and (file[-3:] == "hea") and (file[-5:] != "n.hea"):
                header = wfdb.rdheader(f"{patient_path}/{file[:-4]}")
                segments = header.seg_name

                # Keeping track of date
                days_passed = 0
                t1 = datetime.strptime(str(header.base_time)[:8], "%H:%M:%S")
                base_date = datetime.strptime(str(header.base_date), "%Y-%m-%d")

                for segment in segments:
                    if segment[-7:] != "_layout" and segment != "~":
                        # read_segment here
                        record_data = wfdb.rdrecord(f"{patient_path}/{segment}")
                        sigs_present = record_data.sig_name

                        if all(x in sigs_present for x in required_sigs):
                            if (
                                len(
                                    np.where(
                                        np.array(vars(record_data)["sig_name"]) == "II"
                                    )[0]
                                )
                                > 1
                            ):
                                continue

                            # saving the ecg npy file

                            # outfile_dir = f"/ssd-shared/mimiciii_waveform_matched/{patient_folder}_{segment}_ECG_II.npy"
                            # lead_loc = int(np.where(np.array(vars(record_data)['sig_name']) == 'II')[0])
                            # np.save(outfile_dir, vars(record_data)['p_signal'][:,lead_loc])

                        segment_header = wfdb.rdheader(f"{patient_path}/{segment}")
                        segments_list.append(segment)
                        PID_list.append(patient_folder)
                        base_time_list.append(header.base_time)
                        base_date_list.append(header.base_date)
                        segment_time_list.append(segment_header.base_time)

                        t2 = datetime.strptime(
                            str(segment_header.base_time)[:8], "%H:%M:%S"
                        )

                        delta_t = (t2 - t1).total_seconds()

                        if delta_t < 0:
                            days_passed += 1
                            # print("HERE!", days_passed)
                            # updating the start time
                            t1 = datetime.strptime(
                                str(segment_header.base_time)[:8], "%H:%M:%S"
                            )

                        # print("header_time", header.base_time)
                        # print("header date", base_date)
                        # print("segment time", segment_header.base_time)
                        # print("New date", base_date + timedelta(days=days_passed))
                        # print("*"*100)

                        segment_date_list.append(
                            base_date + timedelta(days=days_passed)
                        )
                        segment_len_list.append(segment_header.sig_len)
                        signal_list.append(segment_header.sig_name)
                        segment_path_list.append(f"{patient_path}/{segment}")


ecg_files_meta_data = pd.DataFrame()
ecg_files_meta_data["PID"] = PID_list
ecg_files_meta_data["base_time"] = base_time_list
ecg_files_meta_data["base_date"] = base_date_list
ecg_files_meta_data["segment"] = segments_list
ecg_files_meta_data["segment_path"] = segment_path_list
ecg_files_meta_data["segment_time"] = segment_time_list
ecg_files_meta_data["segment_date"] = segment_date_list
ecg_files_meta_data["segment_len"] = segment_len_list
ecg_files_meta_data["available_signals"] = signal_list
# ecg_files_meta_data['flag_multiple_lead_readings'] = patients_w_multiple_lead2

ecg_files_meta_data.to_csv(
    f"/ssd-shared/mimiciii_waveform_matched/ecg_matched_files_meta_data.csv",
    index=False,
)
