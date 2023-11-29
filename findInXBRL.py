import numpy,xmltodict
import numpy as np,datetime
from multiprocessing.pool import ThreadPool
from bisect import bisect_left


class splitXBRL():
    def __init__(self):
        self.id_to_reestr=[]
        self.taxis_list_to_save_reestr='dim-int:ID_KontragentaTaxis'
        self.value_ok=[]

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
    def var_value(self, vv):
        key = [kk for kk in vv.keys() if '@' not in kk][0]
        var = vv[key]
        if var:
            self.id_to_reestr.append(var)

    def generator_value(self,iterable):
        iterator = iter(iterable)
        for xx in iterator:
            value=''
            osi=xx['xbrli:scenario']
            if len(osi)>1:
                tm=xx['xbrli:scenario']['xbrldi:typedMember']
                if type(tm)==list:
                    for dd in tm:
                        key = [kk for kk in dd.keys() if '@' not in kk][0]
                        value=dd[key]
                        if self.binary_search(self.id_to_reestr, 0, len(self.id_to_reestr) - 1,int(value.encode('utf-8').hex(), 16)) != -1:
                            yield value
                else:
                    key = [kk for kk in tm.keys() if '@' not in kk][0]
                    value = tm[key]
                    if self.binary_search(self.id_to_reestr, 0, len(self.id_to_reestr) - 1,int(value.encode('utf-8').hex(), 16)) != -1:
                        yield value




    def readXBRL(self,path):
        print('читаю файл',datetime.datetime.now())
        with open(path, encoding='utf-8') as xml_file:
            self.instance = xmltodict.parse(xml_file.read())
        print('прочитал файл',datetime.datetime.now())
        context_list=[]
        context_reestr_list=[]

        print('удаляю лишнее', datetime.datetime.now())
        self.instance['xbrli:xbrl']['xbrli:context'] = [i for j, i in
                                                        enumerate(self.instance['xbrli:xbrl']['xbrli:context']) if
                                                        'xbrli:scenario' in i.keys()]
        # self.instance['xbrli:xbrl']['xbrli:context'] = [i for j, i in
        #                                                 enumerate(self.instance['xbrli:xbrl']['xbrli:context']) if
        #                                                 'xbrldi:typedMember' in i['xbrli:scenario'].keys()]

        print('ищу нужные контексты',datetime.datetime.now())
        self.contexts=self.instance['xbrli:xbrl']['xbrli:context']
        for idx,context in enumerate(self.contexts):
            if len(context['xbrli:scenario'])>1:
                context_list.append(context)

            else:
                if 'xbrldi:typedMember' in context['xbrli:scenario'].keys():
                    dim=context['xbrli:scenario']['xbrldi:typedMember']
                    if type(dim)!=list:
                        if dim['@dimension']=='dim-int:ID_KontragentaTaxis':
                            # temp_=self.var_value(dim)
                            key = [kk for kk in dim.keys() if '@' not in kk][0]
                            var = dim[key]
                            if var:
                                self.id_to_reestr.append(var)
                            context_reestr_list.append(context)
                    else:
                        for dd in context['xbrli:scenario']['xbrldi:typedMember']:
                            if dd['@dimension']=='dim-int:ID_KontragentaTaxis':
                                key = [kk for kk in dd.keys() if '@' not in kk][0]
                                var = dd[key]
                                if var:
                                    self.id_to_reestr.append(var)
                                context_reestr_list.append(context)
        None


        self.id_to_reestr = list(set(self.id_to_reestr))
        r_f=list(set(self.id_to_reestr))
        self.id_to_reestr = [int(xx.encode('utf-8').hex(), 16) for xx in self.id_to_reestr]
        self.id_to_reestr.sort()

        print('ищу нужные контексты в реестре', datetime.datetime.now())
        find_context=[context_ for context_ in self.generator_value(context_list)]


        r_f.sort()

        find_context = list(set(find_context))
        find_context.sort()

        with open('r_f.txt','w') as f:
            for xx in r_f:
                f.write(xx+'\n')

        with open('find_context.txt','w') as f:
            for xx in find_context:
                f.write(xx+'\n')

        r_f=[xx for indx,xx in enumerate(r_f) if xx not in find_context]
        for xx in r_f:
            print(xx)

        # for idx,xx in enumerate(r_f):
        #     for idx2,yy in enumerate(find_context):
        #         if xx==yy:
        #             del r_f[idx]
        #             break
        #
        # for xx in r_f:
        #     print(xx)



if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_5077746690511_ep_SSDNEMED_10rd_sr_q_20230630.xml')
    #ss.readXBRL('XBRL_5077746740121_ep_nso_bki_q_y_15rd_20230331.xml')

    None