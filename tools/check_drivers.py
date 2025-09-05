import pyodbc

print('✅ Các ODBC driver đã được cài đặt trên máy:')
try:
    [print(f'- {driver}.') for driver in pyodbc.drivers()]
except Exception as ex:
    print(f'Lỗi khi liệt kê driver: {ex}')
