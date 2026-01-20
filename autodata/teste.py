from moises_ml_utils import AudioVerification
import pandas as pd
df = pd.DataFrame({    'file_id': ['file_001', 'file_002'],    'song_id': ['song_001', 'song_001'], 'path': ['beatriz-temp/beatpulse-multistems/Afro House/#00000 - Counting_Whook - 102 - F minor-stem/#00000 - Counting_Whook - stem - 102 - F minor - Bass_0_0.wav', 'beatriz-temp/beatpulse-multistems/Afro House/#00000 - Counting_Whook - 102 - F minor-stem/#00000 - Counting_Whook - stem - 102 - F minor - Arpeggiator_0_1.wav'],})
audio_verifier = AudioVerification(storage_client='data-sourcing', bucket_name='beatriz-temp')
audio_verifier.verify_table(    df=df,    new_songs=True)