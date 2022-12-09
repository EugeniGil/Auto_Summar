import prepro
import presencial

import os
import pandas as pd
import cx_Oracle
import requests
import warnings
from io import StringIO
import sys

from dotenv import load_dotenv


#post statement to endpoint

#traer df
dataframe = prepro.dataframe
presencial =presencial.df_p


#juntamos todos los dataframes


dataframe = dataframe.append(presencial) 


dataframe['fecha_ini'] = dataframe['fecha_ini'].astype(str)

#ajustar a la solicitud xml creamos los headers
envelope_open = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mood="urn:microsoft-dynamics-schemas/codeunit/MoodleAsistencia">'
header_open = '<soapenv:Header/>'
body_open = '<soapenv:Body>'
mood_open = '<mood:CrearAsistenciaFormacion>'
mood_close = '</mood:CrearAsistenciaFormacion>'
body_close = '</soapenv:Body>'
envelope_close = '</soapenv:Envelope>'

#convertimos las variables del df en listas

lista_id = list(dataframe['idnumber'])
lista_aaff = list(dataframe['aaff'])
lista_fecha_ini = list(dataframe['fecha_ini'])
lista_duracion = list(dataframe['duracion'])
lista_situacion = list(dataframe['situacion'])


lista_id1 = []
lista_aaff1 = []
lista_fecha_ini1 = []
lista_duracion1 = []
lista_situacion1 = []

#transformaciones necesarias para ajustarse a la petición
for i in lista_id:
    lista_id1.append('<mood:CrearAsistenciaFormacion><mood:iDEmpleado>'+str(i)+'</mood:iDEmpleado>')
for i in lista_aaff:
    lista_aaff1.append('<mood:iDAccionFormativa>'+str(i)+'</mood:iDAccionFormativa>')
for i in lista_fecha_ini:
    lista_fecha_ini1.append('<mood:fechaRealizacionFormacion>'+str(i)+'</mood:fechaRealizacionFormacion>')
for i in lista_duracion:
    lista_duracion1.append('<mood:numeroHorasRealizadas>'+str(i)+'</mood:numeroHorasRealizadas>')
for i in lista_situacion:
    lista_situacion1.append('<mood:situacionAsistente>'+str(i)+'</mood:situacionAsistente></mood:CrearAsistenciaFormacion>')

#primer bloque de intercalado
res = lista_id1+ lista_aaff1
res[::2] = lista_id1
res[1::2] = lista_aaff1

#segundo bloque de intercalado
a = iter(res)
b = iter(lista_fecha_ini1)
new_list = []
try:
    while True:
        if len(new_list) % 3 == 2:
            new_list.append(next(b))
        else:
            new_list.append(next(a))
except StopIteration:
    pass

#tercer bloque de intercalado
a = iter(new_list)
b = iter(lista_duracion1)
new_list1 = []
try:
    while True:
        if len(new_list1) % 4 == 3:
            new_list1.append(next(b))
        else:
            new_list1.append(next(a))
except StopIteration:
    pass

#cuarto bloque de intercalado
a = iter(new_list1)
b = iter(lista_situacion1)
new_list2 = []
try:
    while True:
        if len(new_list2) % 5 == 4:
            new_list2.append(next(b))
        else:
            new_list2.append(next(a))
except StopIteration:
    pass

#cargamos el print en el sistema

buffer = StringIO()
sys.stdout = buffer
print(*new_list2, sep="")
res = buffer.getvalue()

# restore stdout to default for print()
sys.stdout = sys.__stdout__

#parte nueva para iterar por elemento en la lista y enviar automaticamente

def split_list(list):
    return [list[i:i+5] for i in range(0, len(list), 5)]

lists =split_list(new_list2)

#transform each sublist into a string
def list_to_string(list):
    return ["".join(x) for x in list]  

list_a = list_to_string(lists)


#lo juntamos todo
#res1 = envelope_open + header_open + body_open+res +body_close +envelope_close


     
headers = {'content-type': 'application/xml', 'SOAPAction':'urn:microsoft-dynamics-schemas/codeunit/MoodleAsistencia'}




def send_post(list):
    for i in list:
        response = requests.post(BASE_URL, data=i, headers=headers)
        return response

send_post(list_a)

#response = requests.post(BASE_URL,data=res2,headers=headers, auth=(USERNAME, PASS))
#print(response.content)









    


