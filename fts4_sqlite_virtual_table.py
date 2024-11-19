
import requests
from datetime import datetime
from collections import Counter

# Define la ubicación y la fecha de interés
latitude = -27.3671  # Latitud de Posadas, Misiones, Argentina
longitude = -55.8961  # Longitud de Posadas, Misiones, Argentina
date = '2023-01-15'  # Fecha en formato AAAA-MM-DD

# Diccionario de códigos de clima a descripciones
weather_descriptions = {
            0: 'Despejado',
            1: 'Principalmente despejado',
            2: 'Parcialmente nublado',
            3: 'Nublado',
            45: 'Niebla',
            48: 'Niebla con escarcha',
            51: 'Llovizna ligera',
            53: 'Llovizna moderada',
            55: 'Llovizna densa',
            56: 'Llovizna helada ligera',
            57: 'Llovizna helada densa',
            61: 'Lluvia ligera',
            63: 'Lluvia moderada',
            65: 'Lluvia intensa',
            66: 'Lluvia helada ligera',
            67: 'Lluvia helada intensa',
            71: 'Nevada ligera',
            73: 'Nevada moderada',
            75: 'Nevada intensa',
            77: 'Granos de nieve',
            80: 'Chubascos ligeros',
            81: 'Chubascos moderados',
            82: 'Chubascos violentos',
            85: 'Chubascos de nieve ligeros',
            86: 'Chubascos de nieve intensos',
            95: 'Tormenta ligera o moderada',
            96: 'Tormenta con granizo ligero',
            99: 'Tormenta con granizo intenso',
        }


import sqlite3
# Conectar a una base de datos en memoria
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

cursor.execute( '''
    CREATE VIRTUAL TABLE IF NOT EXISTS clima USING fts4 (
        fecha DATE PRIMARY KEY,
    temperatura_prom REAL,
    descripcion TEXT
    ); 
''') 
# Confirmar los cambios
conn.commit()

# Cerrar la conexión
conn.close()

# termina la creacion de la tabla virtual
conn = sqlite3.connect('datos.db')
cursor = conn.cursor()

# Obtener todas las fechas únicas de la tabla 'datos'
cursor.execute('SELECT DISTINCT SaleDate FROM datos')
fechas = cursor.fetchall()

for fecha in fechas:
    date = fecha[0]

    # Verificar si la fecha ya está en la tabla clima
    cursor.execute('SELECT 1 FROM clima WHERE fecha = ?', (date,))
    if cursor.fetchone():
        print(f'Datos climáticos para {date} ya existen en la base de datos.')
        continue

    # Construir la URL de la API
    url = (
        f'https://archive-api.open-meteo.com/v1/era5?'
        f'latitude={latitude}&longitude={longitude}&start_date={date}&end_date={date}&'
        'hourly=temperature_2m,weathercode'
    )

    # Realizar la solicitud GET
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        temperatures = data.get('hourly', {}).get('temperature_2m', [])
        weather_codes = data.get('hourly', {}).get('weathercode', [])

        if temperatures and weather_codes:
            # Calcular la temperatura promedio del día
            avg_temp = sum(temperatures) / len(temperatures)

            # Obtener el código de clima más frecuente del día
            most_common_code = Counter(weather_codes).most_common(1)[0][0]

            # Obtener la descripción del código de clima
            weather_description = weather_descriptions.get(most_common_code, 'Descripción no disponible')

            # Insertar los datos en la tabla clima
            cursor.execute(
                'INSERT INTO clima (fecha, temperatura_prom, descripcion) VALUES (?, ?, ?)',
                (date, avg_temp, weather_description)
            )
            conn.commit()
            print(f'Datos climáticos para {date} insertados correctamente.')
        else:
            print(f'No se encontraron datos para la fecha {date}.')
    else:
        print(f'Error en la solicitud para la fecha {date}: {response.status_code}')

# Cerrar la conexión a la base de datos
conn.close()
