import psycopg2 as psy
from flask import Flask, jsonify
from flask import request
import json
import traceback

connection = psy.connect(user="postgres", password="password",host="ip", database="database_name")


app = Flask(__name__)

@app.route('/get_data_count', methods=['GET'])
def get_data_count():
    count = request.args.get('count') or 0
    count = int(count)
    cursor = connection.cursor()
    label_name = request.args.get('label_name')
    try:
        if count > 0:
            limit_part = f"limit {count}"
        else:
            limit_part = ""
        if label_name == "positive":  
            cursor.execute(f"select count(x.label_id) from (select label_id from  data_labeling {limit_part} ) x where x.label_id = 1  ;")
            count =  cursor.fetchall()
        elif label_name == "negative":  
            cursor.execute(f"select count(x.label_id) from (select label_id from  data_labeling {limit_part} ) x where x.label_id = 0  ;")
            count = cursor.fetchall()
        elif label_name == "all":  
            cursor.execute("select count(label_id) from data_labeling;")
            count =  cursor.fetchall()

        return jsonify(count)

    except Exception as error:
        print ("ERROR IN get_data_count", str(e))
        traceback.print_exc()
        



@app.route('/get_data', methods=['GET'])
def get_data():
    cursor = connection.cursor()
    count = request.args.get('count') or 1000
    skip = request.args.get('skip') or 0
    sort_order = request.args.get('sort_order') or 'asc'
    try:
        query = "select x.data , y.label_id from data_input as x INNER JOIN data_labeling as y ON x.id=y.id order by x.input_date %s limit %s offset %s ;"
        param = (sort_order, count, skip)
        cursor.execute(query % param)
        label_text = cursor.fetchall()
        return jsonify(label_text)

    except Exception as error:
        print("Exception TYPE:", type(error))




if __name__ == "__main__":
    app.run(debug=True, port=3000)

