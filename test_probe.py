import requests




container_ip=""
res= requests.get(f"http://{container_ip}:5000/lastscan")
print(res)