import csv
import os


with open(f'{os.path.dirname(__file__)}/data/all_need_items.csv', 'r') as _file:
    reader = csv.reader(_file)
    ITEM_IDS = reader.__next__()


ITEM_ID_TO_COLUMN = dict()
with open(f'{os.path.dirname(__file__)}/data/needed_items_app.csv', 'r') as _file:
    reader = csv.reader(_file)
    reader.__next__()
    for row in reader:
        _, item_id, column_name, *_ = row
        ITEM_ID_TO_COLUMN[int(item_id)] = column_name
