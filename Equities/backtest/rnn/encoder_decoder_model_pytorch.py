import pandas as pd
pd.options.mode.chained_assignment = None
import os 
import numpy as np
import math 
from datetime import datetime

import torch
import torch.nn as nn
import torch.nn.functional as F

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

from functions import *

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
bqstorageclient = bigquery_storage.BigQueryReadClient()
client = bigquery.Client()



## pytorch model
class DeepETA(nn.Module):
    def __init__(self, feature_dim, num_heads, num_encoder_layers, num_decoder_layers, hidden_dim):
        super(DeepETA, self).__init__()
        self.embedding = nn.Linear(feature_dim, hidden_dim)
        encoder_layer = nn.TransformerEncoderLayer(d_model=hidden_dim, nhead=num_heads)
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_encoder_layers)
        decoder_layer = nn.TransformerDecoderLayer(d_model=hidden_dim, nhead=num_heads)
        self.decoder = nn.TransformerDecoder(decoder_layer, num_layers=num_decoder_layers)
        self.output_layer = nn.Linear(hidden_dim, 1)

    def forward(self, src, tgt):
        src_emb = self.embedding(src)
        tgt_emb = self.embedding(tgt)
        memory = self.encoder(src_emb)
        output = self.decoder(tgt_emb, memory)
        output = self.output_layer(output)
        return output
