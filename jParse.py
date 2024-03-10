import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        addr=data["result"][k]["addresses"]
        status = data["result"][k]["status"]
        category=data["result"][k]["category"]
        print(data["result"][k]["id"], end='')
        for a in addr:
            print (",",a,end='')
        print (",",status,end='')
        print (",",category,end='')
        print("")
