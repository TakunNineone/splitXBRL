import numpy,xmltodict,pickle,os,codecs,pandas as pd,io
import numpy as np,datetime


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

    def save_large_dataframe_to_excel(self, df, excel_file_path, max_rows_per_sheet=1048575,ep=None):
        if ep!=None:
            df.insert(0, "entrypoint",ep)
        num_chunks = len(df) // max_rows_per_sheet + 1
        print('частей',num_chunks)
        for i in range(num_chunks):
            path = excel_file_path.split('.xslx')[0] + f'_{i}' + '.xlsx'
            with pd.ExcelWriter(path) as writer:
                print('сохраняю книгу', i)
                start_row = i * max_rows_per_sheet
                end_row = (i + 1) * max_rows_per_sheet
                chunk_df = df[start_row:end_row]
                chunk_df.to_excel(writer, sheet_name=f'Sheet', index=False)
                print(f'Сохранено в {path}')

    def check_varible(self,varible):
        # print('проверяю переменную',varible, datetime.datetime.now())
        if type(self.instance['xbrli:xbrl'][varible]) == list:
            self.var_list[varible] = [xx for j,xx in enumerate(self.instance['xbrli:xbrl'][varible]) if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(xx['@contextRef'].encode('utf-8').hex(), 16))!=-1]

        else:
            if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(self.instance['xbrli:xbrl'][varible]['@contextRef'].encode('utf-8').hex(), 16))!=-1:
                self.var_list[varible] = [self.instance['xbrli:xbrl'][varible]]

    def make_hex_context(self,context):
        taxiss = []
        axiss = []
        id=context['@id']
        period = context['xbrli:period']['xbrli:instant'] if 'xbrli:instant' in context['xbrli:period'].keys() else \
        context['xbrli:period']['xbrli:startDate'] + '-' + context['xbrli:period']['xbrli:endDate']
        if 'xbrli:scenario' in context.keys():
            if 'xbrldi:typedMember' in context['xbrli:scenario'].keys():
                if type(context['xbrli:scenario']['xbrldi:typedMember']) == list:
                    for tt in context['xbrli:scenario']['xbrldi:typedMember']:
                        taxis = tt['@dimension'] + '#' + tt[[tk for tk in tt.keys() if tk != '@dimension'][0]]
                        taxiss.append(taxis)
                else:
                    taxis = context['xbrli:scenario']['xbrldi:typedMember']['@dimension'] + '#' + \
                            context['xbrli:scenario']['xbrldi:typedMember'][
                                [tk for tk in context['xbrli:scenario']['xbrldi:typedMember'].keys() if
                                 tk != '@dimension'][0]]
                    taxiss.append(taxis)
            else:
                taxiss.append('null')

            if 'xbrldi:explicitMember' in context['xbrli:scenario'].keys():
                if type(context['xbrli:scenario']['xbrldi:explicitMember']) == list:
                    for tt in context['xbrli:scenario']['xbrldi:explicitMember']:
                        axis = tt['@dimension'] + '#' + tt['#text']
                        axiss.append(axis)
                else:
                    axis = context['xbrli:scenario']['xbrldi:explicitMember']['@dimension'] + '#' + \
                           context['xbrli:scenario']['xbrldi:explicitMember']['#text']
                    axiss.append(axis)
            else:
                axiss.append('null')
        else:
            axiss.append('null')
            taxiss.append('null')

        taxiss.sort()
        axiss.sort()

        result=period+'|'+';'.join(axiss)+'|'+';'.join(taxiss)
        hex = int(result.encode('utf-8').hex(), 16)
        return [hex,id,result]

    def readXBRL(self,path):
        if os.path.isfile('instance.pkl'):
            print('читаю pickle',datetime.datetime.now())
            with open('instance.pkl', 'rb') as file:
                self.instance = pickle.load(file)
            print('прочитал pickle', datetime.datetime.now())
        else:
            print('читаю файл',datetime.datetime.now())
            with io.open(path,'rb',buffering=2<<16) as xml_file:
                self.instance = xmltodict.parse(xml_file.read())
            print('прочитал файл',datetime.datetime.now())
            with open('instance.pkl', 'wb') as file:
                pickle.dump(self.instance, file)

        context_list= []
        context_to_excel = []



        if os.path.isfile('context_list.pkl'):
            print('читаю pickle листы',datetime.datetime.now())
            # with open('context_list.pkl', 'rb') as file:
            #     context_list = pickle.load(file)
            with open('context_to_excel.pkl', 'rb') as file:
                context_to_excel = pickle.load(file)
        else:
            print('ищу нужные контексты', datetime.datetime.now())
            self.contexts=self.instance['xbrli:xbrl']['xbrli:context']
            print(len(self.contexts))
            for idx,context in enumerate(self.contexts):
                print("", end=f"\rPercentComplete: {round((idx + 1) / len(self.contexts) * 100, 2)}%")
                try:
                    id,cntxt,values=self.make_hex_context(context)
                    # context_list.append([id,cntxt])
                    context_to_excel.append([cntxt,values.split('|')[0],values.split('|')[1],values.split('|')[2]])
                except:
                    print('ERROR!!!!!',context)

            # with open('context_list.pkl', 'wb') as file:
            #     pickle.dump(context_list, file)
            with open('context_to_excel.pkl', 'wb') as file:
                pickle.dump(context_to_excel, file)

        columns = ['id', 'period', 'axis', 'taxis']
        df = pd.DataFrame(data=context_to_excel, columns=columns)
        df.to_csv('contexts_.csv', sep='|', encoding='utf-8')
        # self.save_large_dataframe_to_excel(df,'contexts')


        # context_list.sort() #sorted(context_list, key=lambda x: x[0])
        #
        # context_list_id=[xx[1] for xx in context_list]
        # context_list_cntx=[xx[0] for xx in context_list]
        #
        # print('\n')
        #
        # res_list=[]
        # res_dict={}
        #
        # for indx,xx in enumerate(context_list_cntx):
        #     # print("", end=f"\rPercentComplete: {round((indx + 1) / len(context_list_cntx) * 100, 2)}%")
        #     res = self.binary_search(context_list_cntx, 0, len(context_list_cntx) - 1,int(xx))
        #     text=codecs.decode(bytes("%0.2X" % xx, encoding='utf-8'), "hex").decode('utf-8')
        #     res_list.append(text)
        #
        #     if res_dict.get(res)!=None:
        #         values=codecs.decode(bytes("%0.2X" % xx, encoding='utf-8'), "hex").decode('utf-8')
        #         print(context_list_id[res],context_list_id[indx])
        #
        #     else:
        #         res_dict[res]='1'
        # print('закончил',datetime.datetime.now())




if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_1187700022465_ep_nso_npf_q_30d_reestr_0420258_20230630.xml')
    None