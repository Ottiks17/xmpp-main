import sqlite3
import os

db_path = "logs/messages.db"

if not os.path.exists(db_path):
    print("База данных не найдена")
    exit()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Количество записей
cursor.execute("SELECT COUNT(*) FROM messages")
count = cursor.fetchone()[0]
print(f"Всего сообщений: {count}")
print("=" * 80)

# Последние 20 сообщений
cursor.execute('''
    SELECT id, type, sender, recipient, message, send_time, delivery_time, log_time 
    FROM messages 
    ORDER BY id DESC 
    LIMIT 20
''')

print(f"{'ID':<5} {'Type':<6} {'Sender':<25} {'Recipient':<25} {'Message':<30}")
print("-" * 100)

for row in cursor.fetchall():
    msg_id = row[0]
    msg_type = row[1]
    sender = row[2][:24] if row[2] else "-"
    recipient = row[3][:24] if row[3] else "-"
    message = row[4][:28] + ".." if len(row[4]) > 30 else row[4]
    print(f"{msg_id:<5} {msg_type:<6} {sender:<25} {recipient:<25} {message:<30}")

conn.close()

