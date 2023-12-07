import pickle
import os
import pandas as pd
import numpy as np
from tqdm import tqdm


metadata = pd.read_csv(f"/ssd-shared/mimiciii_waveform_matched/ecg_matched_files_meta_data.csv")


print("Finding all the available signals...")

all_available_signals = []
for i in tqdm(range(len(metadata))):
    for j in range(len(metadata.iloc[i]['available_signals'][2:-2].split("', '"))):

        all_available_signals.append(metadata.iloc[i]['available_signals'][2:-2].split("', '")[j])
        
        
new_columns = list(set(all_available_signals))

print("Adding subject specific info...")
meta_data_w_lead_info = metadata
meta_data_w_lead_info[new_columns] = 0
for i in tqdm(range(len(meta_data_w_lead_info))):
    for j in range(len(new_columns)):
        if new_columns[j] in meta_data_w_lead_info.iloc[i]['available_signals']:
            meta_data_w_lead_info.at[i , new_columns[j]] = 1
            

print("saving the updated meta data...")
meta_data_w_lead_info.to_csv(f"/ssd-shared/mimiciii_waveform_matched/ecg_matched_files_meta_data_w_lead_info.csv", index = False)
