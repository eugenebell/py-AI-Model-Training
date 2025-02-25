
# importing required modules
from pypdf import PdfReader
import os
import datasets
# creating a pdf reader object
#reader = PdfReader('C:\\Users\\Eugen\\models\\Training-Data\\TKGI\\tkgi-1_20-doc.pdf')
reader = PdfReader('../models/training-data/tkgi/tkgi-1_20-doc.pdf')
# printing number of pages in pdf file
print(len(reader.pages))

i = 0
 

code_dir = "../models/training-data/tkgi/pdf-text-extraction"
#Retrieve the python scripts:
#for page in reader.pages:
#    i += 1
#    file_path = os.path.join(code_dir, str(i) + ".txt")
#    os.makedirs(os.path.dirname(file_path), exist_ok=True)
#    with open(file_path, "wb") as file:
#        file.write(page.extract_text().encode('utf-8'))



#files = os.listdir(code_dir)
#for file in files:
#    print(file)
# getting a specific page from the pdf file
#page = reader.pages[75]

# extracting text from page
#text = page.extract_text()
#print(text)

#Concatenate scripts into a list: 
code_dataset = []
for file in os.listdir(code_dir):
    code_dataset.append(
        {'text': open(os.path.join(code_dir, file), 'r').read()}
    )  

#Convert list to Hugging Face Dataset object:    
code_dataset = datasets.Dataset.from_list(code_dataset)
print(code_dataset)



##################Combine the python code dataset with the pretraining dataset you downloaded above:
#skiping the pretraining dataset for now and just using the code dataset
#dataset = datasets.concatenate_datasets(
dataset = code_dataset
#dataset = datasets.concatenate_datasets(
#    [pretraining_dataset, code_dataset]
#)
print(dataset)

dataset.num_rows

#Data cleaning......

# Step 1 - remove samples that are too short
import heapq

def paragraph_length_filter(x):
    """Returns False if a page has too few lines or lines are too short."""
    lines = x['text'].split('\n')
    if (
        len(lines) < 3
        or min(heapq.nlargest(3, [len(line) for line in lines])) < 3
    ):
        print("Hit false so found sample too small")
        return False
    return True

dataset = dataset.filter(
    paragraph_length_filter,
    load_from_cache_file=False
)

dataset.num_rows




# Remove repetitions within a single text example
def find_duplicates(paragraphs):
    """
    Use this function to find the number of repetitions 
    in the paragraphs.
    """
    unique_x = set()
    duplicate_chars = 0
    duplicate_elements = 0
    for element in paragraphs:
        if element in unique_x:
            duplicate_chars += len(element)
            duplicate_elements += 1
        else:
            unique_x.add(element)
    return duplicate_elements, duplicate_chars

import re

def paragraph_repetition_filter(x):
    """
    Returns False iff a page has too many repetitions.
    """
    text = x['text']
    paragraphs = re.compile(r"\n{2,}").split(text.strip())                # Split by paragraphs (2 or more newlines)
    paragraphs_duplicates, char_duplicates = find_duplicates(paragraphs)  # Find number of duplicates in paragraphs
    if paragraphs_duplicates / len(paragraphs) > 0.3:
        return False
    if char_duplicates / len(text) > 0.2:
        return False
    return True

dataset = dataset.filter(
    paragraph_repetition_filter,
    load_from_cache_file=False
)

dataset.num_rows




# Remove duplicated documents
def deduplication(ds):
    def dedup_func(x):
        """Use this function to remove duplicate entries"""
        if x['text'] in unique_text:
            return False
        else:
            unique_text.add(x['text'])
            return True

    unique_text = set()

    ds = ds.filter(dedup_func, load_from_cache_file=False, num_proc=1)
    return ds

dataset = deduplication(dataset)

dataset.num_rows

#Save the dataset to disk
#Read more about the parquet data format here (https://parquet.apache.org/).
file_path = "../models/training-data/tkgi/preprocessed_dataset.parquet"
dataset.to_parquet(file_path)
