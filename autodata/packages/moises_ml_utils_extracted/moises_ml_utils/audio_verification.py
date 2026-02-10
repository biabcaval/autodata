import pandas as pd
from google.cloud import bigquery, storage
from collections import defaultdict
from moises_ml_utils.utils import get_bq_client, get_sa_credentials_from_secret, generate_signed_url, upload_dataframe_to_gcs
from musicdata.utils.files import MediaFile
from concurrent.futures import ThreadPoolExecutor
import torchaudio
import torchaudio.functional as F
import numpy as np
import json
import os
import tempfile
from loguru import logger

class VerificationError(Exception):
    """Raised when there's a configuration issue."""
    pass

class AudioVerification:

    def __init__(self, 
                storage_client: str = 'moises-ds-data',
                bucket_name: str = 'moises-data-catalog',
                signed_url_secret_id: str = 'sign-read-url-only',
                n_workers_parallel: int = 20):

        self.client = get_bq_client('moises-ds-data')

        if storage_client == 'moises-ds-data':
            self.storage_client = storage.Client.from_service_account_info(
                get_sa_credentials_from_secret(signed_url_secret_id, storage_client)
            )
        else:
            self.storage_client = storage.Client(storage_client)

        self.buckets = {bucket_name: self.storage_client.bucket(bucket_name)}

        self.n_workers_parallel = n_workers_parallel

        self.file_data_cols = ['duration', 'channels', 'bit_rate', 'format', 'codec', 'sample_rate', 'n_samples']

        self.errors_dfs = []
    
    def _check_paths_exist_lambda(self, path):
        bucket_name, blob_name = path.replace('gs://', '').split('/', 1)
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = self.storage_client.bucket(bucket_name)

        return storage.Blob(
            bucket=self.buckets[bucket_name], 
            name=blob_name
        ).exists(self.storage_client)

    def _check_paths_exist(self):

        with ThreadPoolExecutor(max_workers=10) as executor:
            self.df['file_exists'] = list(executor.map(
                lambda p: self._check_paths_exist_lambda(p), 
                self.df['path']
            ))
        
        self.df['errors'] = self.df.apply(lambda x: {'Paths not found': True} if not x['file_exists'] else {}, axis=1)

    def _get_existing_files(self):

        query = f"""
                SELECT file_id, song_id, path
                FROM `moises-ds-data.data_catalog.stem_types_annotations`
                WHERE song_id IN UNNEST(@ids)
                """
            
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "STRING", list(self.df['song_id']))
            ]
        )
        df_existing = self.client.query(query, job_config=job_config).to_dataframe()

        df_existing['file_exists'] = True
        df_existing['on_catalog'] = True
        
        self.df = pd.concat([self.df, df_existing], ignore_index=True)


    def _get_metadata(self, path):
        
        try:
            bucket_name, blob_name = path.replace('gs://', '').split('/', 1)

            if bucket_name not in self.buckets:
                self.buckets[bucket_name] = self.storage_client.bucket(bucket_name)

            blob = self.buckets[bucket_name].blob(blob_name)

            with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
                temp_file_path = temp_file.name

                blob.download_to_file(temp_file)
                temp_file.seek(0)

                waveform, sample_rate = torchaudio.load(temp_file_path)
                loudness = F.loudness(waveform, sample_rate)
                loudness_value = loudness.item()

                mf = MediaFile(temp_file_path)
                mf_cleaned = mf.strip_metadata(temp_file_path)
                md5_hash = mf_cleaned.get_md5_hash()

                audio_stream = mf_cleaned.media_info.get("streams", [])[0]
                audio_format = mf_cleaned.media_info.get("format", {})

            return {'lufs': loudness_value,
                    'duration': mf_cleaned.duration,
                    'sample_rate': audio_stream.get("sample_rate"),
                    'n_samples': audio_stream.get("duration_ts"),
                    'n_waveform_samples': waveform.shape[1],
                    'duration_ffprobe': int(audio_stream.get("duration_ts")) / int(audio_stream.get('time_base')[2:]),
                    'duration_waveform': waveform.shape[1] / sample_rate,
                    'channels': audio_stream.get("channels"),
                    'bit_rate': audio_stream.get("bit_rate"),
                    'format': audio_format.get("format_name"),
                    'codec': audio_stream.get("codec_name"),
                    'md5_original': mf.get_md5_hash(),
                    'md5_cleaned': md5_hash
                    }

        except Exception as e:
            logger.error(f"Failed to process {path}: {str(e)}")
            return None
            
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def _compare_metadata(self):

        grouped_df = self.df.groupby('song_id')

        for song_id, song_df in grouped_df:
            error_detected = False
            for col in self.file_data_cols:
                if song_df[col].nunique() > 1:
                    song_df['errors'] = song_df['errors'].apply(lambda  x: {**x, f'{col} consistency on song': True})
                    error_detected = True
            
            if error_detected:
                self.df.loc[song_df.index, 'errors'] = song_df['errors']

    def _compare_md5_table(self):

        for md5_type in ['cleaned', 'original']:
            duplicates = self.df[self.df.duplicated(subset=[f'md5_{md5_type}'], keep=False)]
            if duplicates.empty:
                continue

            for md5, df_group in duplicates.groupby(f'md5_{md5_type}'):
                df_group_copy = df_group.copy(deep=True)
                for idx, row in df_group.iterrows():
                    ids_with_same_md5 = df_group[df_group['file_id'] != row['file_id']]['file_id'].tolist()
                    df_group_copy.at[idx, 'errors'][f'md5_check_intra_dataset_{md5_type}'] = ids_with_same_md5
                
                self.df.loc[df_group_copy.index, 'errors'] = df_group_copy['errors'].values
        
    def _compare_md5_catalog(self):

        query = f"""
                SELECT id as file_id_remote, md5_hash as md5
                FROM `moises-ds-data.raw_ds_catalog_psql_stream.public_files`
                WHERE md5_hash IN UNNEST(@md5_cleaned) or md5_hash in UNNEST(@md5_original)
                """
            
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("md5_cleaned", "STRING", list(self.df['md5_cleaned'])),
                bigquery.ArrayQueryParameter("md5_original", "STRING", list(self.df['md5_original']))
            ]
        )

        df_md5 = self.client.query(query, job_config=job_config).to_dataframe()

        if df_md5.empty:
            return

        for md5_type in ['cleaned', 'original']:
            df_matches = self.df.reset_index().merge(
                df_md5,
                left_on=f'md5_{md5_type}',
                right_on='md5',
                how='inner'
            ).set_index('index')

            if df_matches.empty:
                continue

            error_lists = df_matches.groupby(level=0)['file_id_remote'].apply(list)

            for idx, remote_ids in error_lists.items():
                current_errors = self.df.at[idx, 'errors']
                current_errors[f'md5_check_catalog_{md5_type}'] = remote_ids
                self.df.at[idx, 'errors'] = current_errors


    def _check_audio_content(self):

        with ThreadPoolExecutor(max_workers=self.n_workers_parallel) as executor:
            self.df['metadata'] = list(executor.map(
                lambda p: self._get_metadata(p), 
                self.df['path']
            ))

        failed_to_decode = self.df[~self.df['metadata'].notnull()]
        failed_to_decode = failed_to_decode[~failed_to_decode['on_catalog']]
        failed_to_decode['errors'] = failed_to_decode.apply(lambda x: {'Failed to process audio (Likely invalid data)': True}, axis=1)

        self.df = self.df[self.df['metadata'].notnull()]
        
        for col in self.file_data_cols + ['lufs', 'duration_ffprobe', 'duration_waveform', 'md5_cleaned', 'md5_original']:
            self.df[col] = self.df['metadata'].apply(lambda x: x[col])
        
        self.df['errors'] = self.df.apply(lambda x: {'Silent Audio': True} if np.isnan(x['lufs']) else {}, axis=1)
        self.df['errors'] = self.df.apply(lambda x: {**x['errors'], f'Audio duration and metadata duration are different': True} if abs(x['duration_ffprobe'] - x['duration_waveform']) > 0.01 else x['errors'], axis=1)

        self._compare_metadata()

        self.df = self.df[~self.df['on_catalog']]
        self._compare_md5_table()
        self._compare_md5_catalog()

        self.df = pd.concat([self.df[['file_id', 'song_id', 'path', 'metadata', 'errors']], failed_to_decode[['file_id', 'song_id', 'path', 'metadata', 'errors']]])

    def _raise_error_and_upload(self):

        error_df = self.df[self.df['errors'] != {}]

        if error_df.empty:
            return

        print("\nERRORS DETECTED:")
        # Print only first 10 errors for console

        n_print = 10
        print_df = error_df[:n_print] if len(error_df) > n_print else error_df
        for idx, row in print_df.iterrows():
            print(f"\nFile: [{row['file_id']}]:")
            print(f"{row['errors']}")
        
        if len(error_df) > n_print:
            print(f'\nand {len(error_df) - n_print} more errors...')
        print('\n')


        file_path = upload_dataframe_to_gcs(error_df, folder_error_name='load-audio_checks', bucket_name='data_load_errors', project_id='machine-learning')

        gcs_console_url = f"https://console.cloud.google.com/storage/browser/_details/data_load_errors/{file_path}?project=moises-machine-learning"
        raise VerificationError(f"Errors detected on dataframe verification: {gcs_console_url}")


    def verify_table(self, df, new_songs=True):

        self.df = df.copy(deep=True)

        if not len(self.df) or not isinstance(self.df, pd.DataFrame):
            raise VerificationError('DataFrame is invalid.')

        self.df = self.df[['file_id', 'song_id', 'path']]
        self._check_paths_exist()
        self._raise_error_and_upload()

        self.df['on_catalog'] = False
        if not new_songs:
            self._get_existing_files()

        self._check_audio_content()
        self._raise_error_and_upload()