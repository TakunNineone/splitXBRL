import numpy,xmltodict,pickle,os
import numpy as np,datetime
from multiprocessing.pool import ThreadPool
from bisect import bisect_left


class splitXBRL():
    def __init__(self):
        self.id_to_reestr=[]
        self.value_ok=[]
        self.var_list= {}

    def binary_search(self,arr, low, high, x):
        if high >= low:
            mid = (high + low) // 2
            if arr[mid] == x:
                return mid
            elif arr[mid] > x:
                return self.binary_search(arr, low, mid - 1, x)
            else:
                return self.binary_search(arr, mid + 1, high, x)

        else:
            return -1



    def check_varible(self,varible):
        # print('проверяю переменную',varible, datetime.datetime.now())
        if type(self.instance['xbrli:xbrl'][varible]) == list:
            self.var_list[varible] = [xx for j,xx in enumerate(self.instance['xbrli:xbrl'][varible]) if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(xx['@contextRef'].encode('utf-8').hex(), 16))!=-1]

        else:
            if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(self.instance['xbrli:xbrl'][varible]['@contextRef'].encode('utf-8').hex(), 16))!=-1:
                self.var_list[varible] = [self.instance['xbrli:xbrl'][varible]]


    def readXBRL(self,path):

        if os.path.isfile('instance.pkl'):
            print('читаю pickle',datetime.datetime.now())
            with open('instance.pkl', 'rb') as file:
                self.instance = pickle.load(file)
            print('прочитал pickle', datetime.datetime.now())
        else:
            print('читаю файл',datetime.datetime.now())
            with open(path, encoding='utf-8') as xml_file:
                self.instance = xmltodict.parse(xml_file.read())
            print('прочитал файл',datetime.datetime.now())
            with open('instance.pkl', 'wb') as file:
                pickle.dump(self.instance, file)

        context_list=[]
        context_reestr_list=[]



        print('ищу нужные контексты',datetime.datetime.now())
        self.contexts=self.instance['xbrli:xbrl']['xbrli:context']
        for idx,context in enumerate(self.contexts):
            #if 'xbrli:scenario' not in list(context.keys()):
            context_list.append(context)
        print(len(context_list))

        varibles = [varible for varible in self.instance['xbrli:xbrl'].keys() if '@' not in varible and varible not in ('link:schemaRef', 'xbrli:context', 'xbrli:unit')]
        varibles = list(set(varibles))
        with open('varibles.txt','w') as f:
           f.write('\n'.join(varibles))

        self.cnt_ids=[]

        for xx in context_list:
            self.cnt_ids.append(xx['@id'])
        self.cnt_ids=[int(xx.encode('utf-8').hex(), 16) for xx in self.cnt_ids]
        self.cnt_ids.sort()

        with ThreadPool(processes=100) as pool:
            pool.map(self.check_varible, varibles)

        list_to_save=[]


        for xx in list(self.var_list.keys()):
            for yy in self.var_list[xx]:
                if xx in ('purcb-dic:Kod_Okato3','purcb-dic:INN_Prof_uch','nfo-dic:OGRN_OGRNIP','purcb-dic:AdresPocht_Prof_uch'):
                    list_to_save.append(f"{xx};{yy['@contextRef']};{yy['#text']}")
                #print(f"{xx} - {self.instance['xbrli:xbrl'][xx][yy]['@contextRef']} - {self.instance['xbrli:xbrl'][xx][yy]['#text']}")

        print(len(list_to_save))

        with open('log_11-10-2023.txt','w') as f:
            f.writelines([xx+'\n' for xx in list_to_save])



if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_1027700186062_ep_SSDNEMED_10rd_sr_m_20230831.xml')
    #ss.readXBRL('XBRL_5077746740121_ep_nso_bki_q_y_15rd_20230331.xml')

    None