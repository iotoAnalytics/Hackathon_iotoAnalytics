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
import pandas as pd


class Classifier:

    def __init__(self):
        
        print('Loading model...')
        s3 = boto3.client('s3')
        # load model from S3 bucket named bill-topic-classifier-sample
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
            s3.download_fileobj('bill-topic-classifier-sample', 'bert_data_dem4.pt', f)
            self.mlmodel = f.name
            # print(mlmodel)
        print('Model loaded.')

        # df = pd.DataFrame(df)

        self.possible_labels = ['government operations', 'health', 'education', 'macroeconomics', '',
                            'international affairs', 'civil rights', 'social welfare', 'public lands',
                            'defense', 'domestic commerce', 'law and crime', 'culture', 'transportation',
                            'environment', 'labor', 'housing', 'technology', 'immigration', 'energy',
                            'agriculture', 'foreign trade']
        # the possible labes (CAP topics) are assigned numbers
        # label_dict = {'government operations': 0, 'health': 1, 'education': 2, 'macroeconomics': 3, '': 4,
        #                 'international affairs': 5, 'civil rights': 6, 'social welfare': 7, 'public lands': 8,
        #                 'defense': 9,
        #                 'domestic commerce': 10, 'law and crime': 11, 'culture': 12, 'transportation': 13,
        #                 'environment': 14,
        #                 'labor': 15, 'housing': 16, 'technology': 17, 'immigration': 18, 'energy': 19, 'agriculture': 20,
        #                 'foreign trade': 21}
        self.label_dict = {label: index for index, label in enumerate(possible_labels)}
        # for index, possible_label in enumerate(possible_labels):
        #     label_dict[possible_label] = index

        # default initial value is empty string, or informal for all topic entries
        # df = df.assign(topic="")
        # l = df.topic.replace(label_dict)

        # df = df.assign(label=l)

        # eval_texts = df.bill_text.values

        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)

        # encoded_data_val = tokenizer.encode_plus(
        #     bill_text,
        #     add_special_tokens=True,
        #     return_attention_mask=True,
        #     padding=True,
        #     truncation=True,
        #     max_length=256,
        #     return_tensors='pt'
        # )

        # input_ids_val = encoded_data_val['input_ids']
        # attention_masks_val = encoded_data_val['attention_mask']
        # labels_val = torch.tensor(df.label.values)

        # dataset_val = TensorDataset(input_ids_val, attention_masks_val, labels_val)

        # dataloader_validation = DataLoader(dataset_val,
        #                                     sampler=SequentialSampler(dataset_val),
        #                                     batch_size=1,
        #                                     )
        # get pretrained model
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = BertForSequenceClassification.from_pretrained("bert-base-uncased",
                                                                num_labels=len(label_dict),
                                                                output_attentions=False,
                                                                output_hidden_states=False)
        self.model.to(device)

        self.model.load_state_dict(torch.load(self.mlmodel, map_location=torch.device('cpu')))

        self.model.eval()

        # loss_val_total = 0

        # # make predictions
        # predictions, true_vals = [], []

        # for batch in dataloader_validation:
        #     batch = tuple(b.to(device) for b in batch)

        #     inputs = {'input_ids': batch[0],
        #                 'attention_mask': batch[1],
        #                 'labels': batch[2],
        #                 }

        #     with torch.no_grad():
        #         outputs = model(**inputs)

        #     # loss = outputs[0]
        #     logits = outputs[1]
        #     # loss_val_total += loss.item()

        #     logits = logits.detach().cpu().numpy()

        #     predictions.append(logits)

        # predictions = np.concatenate(predictions, axis=0)

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
        # # get rid of this label row that was only used for classification
        # df = df.drop(columns=['label'])
        # # print(df)
        # # return the dataframe to a list of dictionaries
        # dicts = df.to_dict('records')

        # return dicts


    def predict(self, bill_text):

        encoded_data_val = self.tokenizer.encode_plus(
            bill_text,
            add_special_tokens=True,
            return_attention_mask=True,
            padding=True,
            truncation=True,
            max_length=256,
            return_tensors='pt'
        )

        input_ids_val = encoded_data_val['input_ids']
        attention_masks_val = encoded_data_val['attention_mask']
        # labels_val = torch.tensor(df.label.values)
        labels_val = torch.tensor(self.label_dict.values())

        dataset_val = TensorDataset(input_ids_val, attention_masks_val, labels_val)

        dataloader_validation = DataLoader(dataset_val,
                                            sampler=SequentialSampler(dataset_val),
                                            batch_size=1,
                                            )
        # get pretrained model
        # device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        # model = BertForSequenceClassification.from_pretrained("bert-base-uncased",
        #                                                         num_labels=len(label_dict),
        #                                                         output_attentions=False,
        #                                                         output_hidden_states=False)
        # model.to(device)

        # model.load_state_dict(torch.load(mlmodel, map_location=torch.device('cpu')))

        # model.eval()

        # loss_val_total = 0

        # make predictions
        predictions, true_vals = [], []

        for batch in dataloader_validation:
            batch = tuple(b.to(self.device) for b in batch)

            inputs = {'input_ids': batch[0],
                        'attention_mask': batch[1],
                        'labels': batch[2],
                        }

            with torch.no_grad():
                outputs = self.model(**inputs)

            # loss = outputs[0]
            logits = outputs[1]
            # loss_val_total += loss.item()

            logits = logits.detach().cpu().numpy()

            predictions.append(logits)

        predictions = np.concatenate(predictions, axis=0)

        i = 0
        # fill the dataframe topic column, line by line
        pred_label = ''
        for pred in predictions:
            # print('text:')
            txt = bill_text

            # print(txt)
            # print("predicted label:")
            pred_label = (self.possible_labels[np.argmax(pred)])
            # print(pred_label)
            # put predicted value in its row in the topic column of the dataframe
            # df.loc[i, 'topic'] = pred_label
            # #
            # i = i + 1
        # # get rid of this label row that was only used for classification
        # df = df.drop(columns=['label'])
        # # print(df)
        # # return the dataframe to a list of dictionaries
        # dicts = df.to_dict('records')

        return pred_label