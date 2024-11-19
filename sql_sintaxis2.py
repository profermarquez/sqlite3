import sqlite3

# Conexión a la base de datos
conn = sqlite3.connect('datos.db')
cursor = conn.cursor()

# Crear la vista
cursor.execute('''
CREATE VIEW IF NOT EXISTS ventas_por_vendedor AS
SELECT
    Salesperson,
    SUM(SaleAmount) AS TotalVentas
FROM
    datos
GROUP BY
    Salesperson;
''')

# Consultar la vista
cursor.execute('SELECT * FROM ventas_por_vendedor ORDER BY TotalVentas DESC;')
resultados = cursor.fetchall()

# Mostrar los resultados
for fila in resultados:
    print(f'Vendedor: {fila[0]}, Total de Ventas: {fila[1]}')

# Cerrar la conexión
conn.close()
