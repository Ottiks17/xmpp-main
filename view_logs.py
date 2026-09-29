import sqlite3
import json

conn = sqlite3.connect('logs/messages.db')
cursor = conn.cursor()

print("=" * 60)
print("ПОСЛЕДНИЕ 10 СООБЩЕНИЙ")
print("=" * 60)

cursor.execute('''
    SELECT type, sender, recipient, message, send_time, delivery_time, log_time 
    FROM messages 
    ORDER BY id DESC 
    LIMIT 10
''')

for row in cursor.fetchall():
    print(f"\n[{row[6]}] [{row[0]}]")
    print(f"  От: {row[1]}")
    print(f"  Кому: {row[2]}")
    print(f"  Текст: {row[3][:50]}")
    print(f"  Время отправки: {row[4]}")
    print(f"  Время доставки: {row[5]}")

print("\n" + "=" * 60)
print(f"Всего записей: {cursor.execute('SELECT COUNT(*) FROM messages').fetchone()[0]}")
print("=" * 60)

conn.close()

