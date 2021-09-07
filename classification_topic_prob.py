import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
# import datetime
from datetime import date, datetime
import json
import numpy as np
import torch
import boto3
import tempfile
from transformers import BertTokenizer
from torch.utils.data import TensorDataset
from transformers import BertForSequenceClassification
from torch.utils.data import DataLoader, SequentialSampler
import functools
import torch.nn.functional as F

import sys
import pandas as pd
from database import Database, CursorFromConnectionFromPool, Persistence
from dataclasses import dataclass, field
from typing import List
from rows import *
import copy
# import atexit
import utils
from urllib.request import urlopen as uReq
import re
import requests
import unidecode
from bs4 import BeautifulSoup as soup
from nameparser import HumanName
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import time
import random
from collections import namedtuple
import exceptions
from pandas.core.computation.ops import UndefinedVariableError
import numpy
import pdfplumber
import io

# Get path to the root directory so we can import necessary modules

# makes dummy table in database
# Persistence.write_sam_data_test('test_table_sam')
# print('Done!')

# grab all file names from us_legislators folder

def add_topics(bill_text):

    """
        Pulls a model from an S3 bucket as a temporary file
        Uses the model to classify the bill text
        Returns the dataframe with the topics filled in

        If you want a different model just upload it to the S3
        and change the code in this function to implement that model instead

        Currently this function is being called in both the US and CA prov/terr write_data functions,
        at some point might want two different models/ add topic functions for the different countries

        Takes either a list of dictionaries or a list of rows

        """
    # convert input into dataframe form
    df = pd.DataFrame(bill_text)
    print('Loading model...')
    s3 = boto3.client('s3')
    # load model from S3 bucket named bill-topic-classifier-sample
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
        s3.download_fileobj('bill-topic-classifier-sample', 'bert_data_dem4.pt', f)
        mlmodel = f.name
        # print(mlmodel)

    print('Model loaded.')

    df = pd.DataFrame(df)

    possible_labels = ['government operations', 'health', 'education', 'macroeconomics', '',
                        'international affairs', 'civil rights', 'social welfare', 'public lands',
                        'defense', 'domestic commerce', 'law and crime', 'culture', 'transportation',
                        'environment', 'labor', 'housing', 'technology', 'immigration', 'energy',
                        'agriculture', 'foreign trade']
    # the possible labes (CAP topics) are assigned numbers
    label_dict = {'government operations': 0, 'health': 1, 'education': 2, 'macroeconomics': 3, '': 4,
                    'international affairs': 5, 'civil rights': 6, 'social welfare': 7, 'public lands': 8,
                    'defense': 9,
                    'domestic commerce': 10, 'law and crime': 11, 'culture': 12, 'transportation': 13,
                    'environment': 14,
                    'labor': 15, 'housing': 16, 'technology': 17, 'immigration': 18, 'energy': 19, 'agriculture': 20,
                    'foreign trade': 21}
    for index, possible_label in enumerate(possible_labels):
        label_dict[possible_label] = index

    # default initial value is empty string, or informal for all topic entries
    df = df.assign(topic="")
    l = df.topic.replace(label_dict)

    df = df.assign(label=l)

    eval_texts = df.bill_text.values

    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)

    encoded_data_val = tokenizer.batch_encode_plus(
        eval_texts,
        add_special_tokens=True,
        return_attention_mask=True,
        padding=True,
        truncation=True,
        max_length=256,
        return_tensors='pt'
    )

    input_ids_val = encoded_data_val['input_ids']
    attention_masks_val = encoded_data_val['attention_mask']
    labels_val = torch.tensor(df.label.values)

    dataset_val = TensorDataset(input_ids_val, attention_masks_val, labels_val)

    dataloader_validation = DataLoader(dataset_val,
                                        sampler=SequentialSampler(dataset_val),
                                        batch_size=1,
                                        )
    # get pretrained model
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = BertForSequenceClassification.from_pretrained("bert-base-uncased",
                                                            num_labels=len(label_dict),
                                                            output_attentions=False,
                                                            output_hidden_states=False)
    model.to(device)

    model.load_state_dict(torch.load(mlmodel, map_location=torch.device('cpu')))

    model.eval()

    loss_val_total = 0

    # make predictions
    predictions, probabilities = [], []

    for batch in dataloader_validation:
        batch = tuple(b.to(device) for b in batch)

        inputs = {'input_ids': batch[0],
                    'attention_mask': batch[1],
                    'labels': batch[2],
                    }

        with torch.no_grad():
            outputs = model(**inputs)

        # loss = outputs[0]
        logits = outputs[1]
        # print(logits)

        # probability
        prob = F.softmax(logits, dim=-1).tolist()
        # print(f'\nThe type of prob is {type(prob)}\n')
        probabilities.append(prob)
        # loss_val_total += loss.item()

        logits = logits.detach().cpu().numpy()

        predictions.append(logits)

    predictions = np.concatenate(predictions, axis=0)


    return_lst = []
    topic_lst, prob_lst = [], []
    for i in range(len(predictions)):
        probs = probabilities[i][0]
        prob_dict = {}
        for _ in range(len(probs)):
            prob_dict[possible_labels[_]] = probs[_]
        pred_label = possible_labels[np.argmax(predictions[i])]
        # return_lst.append({
        #     'topic': pred_label,
        #     'probabilities': prob_dict
        # })
        topic_lst.append(pred_label)
        prob_lst.append(prob_dict)


    return topic_lst, prob_lst
    # for pred in predictions:
    #     pred_label = possible_labels[np.argmax(pred)]



    # i = 0
    # # fill the dataframe topic column, line by line
    # for pred in predictions: 
    #     # print('text:')
    #     txt = eval_texts[i]

    #     # print(txt)
    #     # print("predicted label:")
    #     pred_label = (possible_labels[np.argmax(pred)])
    #     # print(pred_label)
    #     # put predicted value in its row in the topic column of the dataframe
    #     df.loc[i, 'topic'] = pred_label
    #     #
    #     i = i + 1
    # get rid of this label row that was only used for classification
    # df = df.drop(columns=['label'])
    # print(df)
    # return the dataframe to a list of dictionaries
    # dicts = df.to_dict('records')
    # print(df['topic'])
    # return df['topic']
    
def json_serial(obj):
    """
    Serializes objects so they may be placed into JSON format.

    Author: Jay Taylor
    Source: https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# SCRIPT STARTS HERE

with CursorFromConnectionFromPool() as cur:
    try:
        # legislator_data = pd.read_sql("SELECT * FROM ca_fed_legislators", cur.connection)
        legislation_data = pd.read_sql("SELECT * FROM ca_fed_legislation", cur.connection)
        cur.connection.commit()
    except Exception as e:
        print(f'An exception occured executing a query: \n{e}')

ml_data = add_topics(legislation_data['bill_text'])
df = legislation_data[['goverlytics_id', 'bill_name']]
df = pd.DataFrame(df.to_dict('records'))
df['topic'] = ml_data[0]
df['probabilities'] = ml_data[1]

# compression_opts = dict(method='zip',
#                         archive_name='out1.csv')  
# df.to_csv('out1.zip', index=False,
#           compression=compression_opts) 

data = df.to_dict('records')

table = 'ca_fed_legislation_topic_probs'

with CursorFromConnectionFromPool() as cur:
    try:
        create_table_query = sql.SQL("""

            CREATE TABLE IF NOT EXISTS {table} (
                bill_name text,
                goverlytics_id text UNIQUE,
                topic text,
                probabilities json
            );

            ALTER TABLE {table} OWNER TO rds_ad;
        """).format(table=sql.Identifier(table))

        cur.execute(create_table_query)
        cur.connection.commit()
    except Exception as e:
        print(f'An exception occured executting a query:\n{e}')
        cur.connection.rollback()

    insert_legislator_query = sql.SQL("""
            INSERT INTO {table}
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (goverlytics_id) DO UPDATE SET
                bill_name = excluded.bill_name,
                goverlytics_id = excluded.goverlytics_id,
                topic = excluded.topic,
                probabilities = excluded.probabilities
            """).format(table=sql.Identifier(table))

    # This is used to convert dictionaries to rows. Need to test it out!
    for item in data:
        # if isinstance(item, dict):
        #     item = utils.DotDict(item)
        try:
            tup = (
                item['bill_name'],
                item['goverlytics_id'],
                item['topic'],
                json.dumps(item['probabilities'], default=json_serial)
            )

            cur.execute(insert_legislator_query, tup)
        except Exception as e:
            print(f'Exception occured inserting the following data:\n{tup}')
            print(e)
            cur.connection.rollback()
