import pandas as pd
pd.options.mode.chained_assignment = None
import os 
import numpy as np
import math 
from datetime import datetime

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

import tensorflow as tf
from tensorflow.keras import layers, models, optimizers

from backtest.functions import read_table

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

class SelfAttention(layers.Layer):
    def __init__(self, attention_dim):
        super(SelfAttention, self).__init__()
        self.attention_dim = attention_dim

    def build(self, input_shape):
        self.query_dense = layers.Dense(self.attention_dim)
        self.key_dense = layers.Dense(self.attention_dim)
        self.value_dense = layers.Dense(self.attention_dim)
        self.softmax = layers.Softmax(axis=-1)

    def call(self, inputs):
        query = self.query_dense(inputs)
        key = self.key_dense(inputs)
        value = self.value_dense(inputs)

        attention_scores = tf.matmul(query, key, transpose_b=True)
        attention_scores = attention_scores / tf.math.sqrt(tf.cast(self.attention_dim, tf.float32))
        attention_weights = self.softmax(attention_scores)

        output = tf.matmul(attention_weights, value)
        return output

def build_encoder(input_dim, embedding_dim, attention_dim):
    inputs = layers.Input(shape=(input_dim,))
    embeddings = layers.Embedding(input_dim=input_dim, output_dim=embedding_dim)(inputs)
    attention_output = SelfAttention(attention_dim)(embeddings)
    encoder_output = layers.GlobalAveragePooling1D()(attention_output)
    return models.Model(inputs, encoder_output)

def build_decoder(encoder_output_dim, hidden_units, output_dim):
    inputs = layers.Input(shape=(encoder_output_dim,))
    x = layers.Dense(hidden_units, activation='relu')(inputs)
    outputs = layers.Dense(output_dim)(x)
    return models.Model(inputs, outputs)

def build_encoder_decoder_model(input_dim, embedding_dim, attention_dim, hidden_units, output_dim):
    encoder = build_encoder(input_dim, embedding_dim, attention_dim)
    decoder = build_decoder(encoder.output_shape[-1], hidden_units, output_dim)

    inputs = layers.Input(shape=(input_dim,))
    encoder_output = encoder(inputs)
    outputs = decoder(encoder_output)

    model = models.Model(inputs, outputs)
    return model

# Define model parameters
input_dim = len(X_var)  # Example input dimension
embedding_dim = 128
attention_dim = 64
hidden_units = 256
output_dim = 1  # For ETA prediction

# Build the model
model = build_encoder_decoder_model(input_dim, embedding_dim, attention_dim, hidden_units, output_dim)

# Compile the model
model.compile(optimizer=optimizers.Adam(learning_rate=0.001), loss='mean_squared_error')


# X_train and y_train should be your training data and labels
model.fit(X_train, Y_train, epochs=10, batch_size=32, validation_split=0.2)




