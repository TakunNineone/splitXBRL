temp= {'context':[{'id':1,'text':'odin'},{'id':2,'text':'dva'}]}

for index,xx in enumerate(temp['context']):
    if xx['id']==1:
        temp['context'].pop(index)
print(temp)