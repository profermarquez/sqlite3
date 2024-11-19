import sqlite3

# Crear conexión SQLite en memoria
#conn = sqlite3.connect(":memory:")  # Usa ":memory:" para no guardar en disco
conn = sqlite3.connect("datos.db") 
cursor = conn.cursor()

import csv

# Crear una tabla en SQLite
cursor.execute("""
CREATE TABLE datos (
    SaleID INTEGER PRIMARY KEY,
    Salesperson TEXT,
    SaleAmount INTEGER,
    SaleDate DATE
)
""")
cursor.execute("CREATE INDEX idx_salesperson ON datos (Salesperson)")

# Leer el CSV y cargarlo en la tabla
with open("data.csv", "r") as archivo_csv:
    reader = csv.reader(archivo_csv)
    columnas = next(reader)  # Leer encabezados
    cursor.executemany(f"INSERT INTO datos VALUES ({','.join(['?'] * len(columnas))})", reader)

conn.commit()

# Ejemplo 1: Seleccionar todas las filas
cursor.execute("SELECT * FROM datos")
for fila in cursor.fetchall():
    print(fila)

conn.close()