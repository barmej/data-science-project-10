import pickle
import json
import numpy as np


with open('model.pickle', 'rb') as file:
    model = pickle.load(file)

with open('vectorizer.pickle', 'rb') as file:
    vectorizer = pickle.load(file)


def clean_text(text):
    import re
    text = re.sub(r"[^a-zA-Z]+", ' ', text)
    return text


def get_all_data(count,skip,sort_order):
    import requests

    get_text=[]
    get_label=[]
    while skip < 50000:

        try:
            payload = {'count': count, 'skip': skip ,'sort_order':sort_order}
            result =requests.get("http://127.0.0.1:3000/get_data", params=payload , headers={'Content-Type': 'application/json'})
            response=result.json()
            texts=[]
            labels=[]
            for i in range(len(response)):
                text=response[i]
                label=response[i][-1]
                texts.append(text)
                labels.append(label)
            get_text.extend(texts)
            get_label.extend(labels)


        except Exception as error:
            print ("ERROR IN GET_TEXT FUNCTION")
            print ("Exception TYPE:", type(error))

    return get_text , get_label


gwt_text , get_label = get_all_data(1000,1,'ASC')


def get_total(label):
    import requests
    try:
        payload = {'label_name': label}
        result = requests.get('http://127.0.0.1:3000/get_total_data_count',params=payload , headers={'Content-Type': 'application/json'})
        response=result.json()
        return response[0]

    except Exception as error:
        print ("ERROR IN get_total FUNCTION")
        print ("Exception TYPE:", type(error))




for i, text in enumerate(get_text):
    get_text[i]=clean_text(get_text[i])

vectorize = vectorizer.transform(get_text)

predictions = model.predict(vectorize)

predict_positive = (predictions==1).sum()
predict_negative = (predictions==0).sum()

positive=get_total("positive")
negative=get_total("negative")


from sklearn.metrics import accuracy_score
accuracy=accuracy_score(get_label, predictions)

print("the total retrieved data = ",len(get_text),"\n")
print("the total of positive values  = ",positive , " \n And the total of negative values = ",Negative"\n")
print("the total of positive values in the predictions = ",predict_positive,
    "\n and the total of negative values in the predictions = ",predict_negative"\n")

print('the model accuracy',accuracy)








