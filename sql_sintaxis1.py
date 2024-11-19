# Crear el trigger

import sqlite3

conn = sqlite3.connect("datos.db") 
cursor = conn.cursor()
cursor.execute("""
CREATE TRIGGER IF NOT EXISTS check_sale_date
BEFORE INSERT ON datos
FOR EACH ROW
WHEN NEW.SaleDate <= '2023-01-31'
BEGIN
    SELECT RAISE(ABORT, 'La fecha debe ser posterior a enero de 2023');
END;
""")

try:
    cursor.execute("""
    INSERT INTO datos(SaleID, Salesperson, SaleAmount, SaleDate)
    VALUES (17, 'Juan Pérez', 1500, '2023-02-01')
    """)
    print("Inserción válida realizada.")
except sqlite3.IntegrityError as e:
    print("Error, insercion invalida!:", e)

# Intentar insertar datos no válidos
try:
    cursor.execute("""
    INSERT INTO datos(SaleID, Salesperson, SaleAmount, SaleDate)
    VALUES (16, 'Ana López', 2000, '2022-12-31')
    """)
except sqlite3.IntegrityError as e:
    print("Error:", e)

conn.commit()
conn.close()