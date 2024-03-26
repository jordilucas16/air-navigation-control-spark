import requests
import json
import time
import random
import socket
import re
from dotenv import load_dotenv
import os

# Cloudera port assigned
puerto=10029

url = "https://aircraftscatter.p.rapidapi.com/lat/41.3/lon/2.1/"

load_dotenv()

api_key = os.getenv("API_KEY")

headers = {
    "X-RapidAPI-Key": api_key,
    "X-RapidAPI-Host": "aircraftscatter.p.rapidapi.com"
}

# Rest of the code...
def prepara_json(datos):
    claves=[("flight",""),("lat",0),("lon",0), ("alt_baro",0),("category","") ]
    #nuevosDatos=datos
    lista=datos['ac']
    #nuevosDatos['ac']=[]
    listado=[]
    
    for elemento in lista:
        diccionario={}
        for clave in claves:
            diccionario[clave[0]]=elemento.get(clave[0],clave[1])
        listado.append(diccionario)
            
    datos['ac']=listado
    return datos

tiempo=0
contador=0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
print("Waiting Spark connection...")
s.bind(('localhost', puerto))
s.listen(1)
conn, addr = s.accept() 
print("Init to AirCraftScatter connection")

while (tiempo<60 and contador < 50):    
    inicio = time.time()
    respuesta=requests.get(url, headers=headers)    
    fin = time.time()
    tiempo=fin-inicio
    datos=prepara_json(respuesta.json())
    vuelos=datos.get('ac')
    n=len(vuelos)
    print()
    contador+=1
    print(contador,":","Flights over Europe: ",n)
    datosLimpios=  json.dumps(datos)
    conn.sendall((datosLimpios+"\n").encode('utf-8'))  
    time.sleep(10) # wait 10 seconds to avoid overload server

conn.close()
