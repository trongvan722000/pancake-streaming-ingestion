import requests
import time 

for i in range(10):
    requests.post("http://localhost:9091/echo", data = f"test su kien ne {i}")
    time.sleep(1)