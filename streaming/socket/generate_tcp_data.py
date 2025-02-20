import socket
import time
import random

states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
          'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
          'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
          'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
          'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

company = ["Google", "Facebook", "Instagram",
           "Spotify", "Amazon", "Netflix", "Dropbox", "Reddit"]

host = 'localhost'
port = 9998
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print("Binding host and port...")
s.bind((host, port))
print("Listening...")
s.listen(1)

try:
    while True:
        conn, addr = s.accept()
        try:
            for j in range(50):
                print("Sending Data...")
                st_idx = random.randint(0, 50)
                cmp_idx = random.randint(0, 7)
                print(f"{states[st_idx]} {company[cmp_idx]}")
                conn.send(bytes(f"{states[st_idx]} {company[cmp_idx]}\n", "utf-8"))
                time.sleep(30)
            conn.close()
        except socket.error as e:
            print(f"Exception occurred: {e}")
            continue
finally:
    s.close()
