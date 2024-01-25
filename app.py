from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId, json_util
import pandas as pd
import glob
import os

app = Flask(__name__)

# Configuración de MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["mortalidad_db"]  # Nombre la base de datos
collection = db["mortalidad"]  # Nombre de la coleccion

def read_csv(file_path):
    """Lee un archivo CSV y lo convierte en una lista de diccionarios."""
    data = pd.read_csv(file_path)
    return data.to_dict(orient='records')

def load_data_to_mongodb(data):
    """Carga una lista de diccionarios en la colección de MongoDB."""
    if data:
        collection.insert_many(data)

# Función para añadir datos
@app.route('/add', methods=['POST'])
def add_data():
    data = request.json
    collection.insert_one(data)
    return jsonify({"status": "success", "message": "Data added"}), 201
# Funció para borrar datos
@app.route('/delete/<id>', methods=['DELETE'])
def delete_data(id):
    collection.delete_one({"_id": ObjectId(id)})  
    return jsonify({"status": "success", "message": "Data deleted"}), 200

# Función para acutalizar datos
@app.route('/update/<id>', methods=['PUT'])
def update_data(id):
    data = request.json
    collection.update_one({"_id": ObjectId(id)}, {"$set": data})
    return jsonify({"status": "success", "message": "Data updated"}), 200

# Función de consulta de la información
@app.route('/get', methods=['GET'])
def get_data():
    data = list(collection.find({}))
    return json_util.dumps(data), 200 
# Función para cargar el archivo
@app.route('/load_csv', methods=['GET'])
def load_csv():
    file_path = '/Users/angel.cabrera/Desktop/rates/H_Rates.csv'
    data = read_csv(file_path)
    load_data_to_mongodb(data)
    return jsonify({"status": "success", "message": "Data loaded from CSV"}), 200


if __name__ == '__main__':
    app.run(debug=True)
