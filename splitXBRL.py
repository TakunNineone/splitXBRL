import numpy,xmltodict
import numpy as np,datetime
from multiprocessing.pool import ThreadPool
from bisect import bisect_left


class splitXBRL():
    def __init__(self):
        self.id_to_reestr=[]
        self.var_list_to_save=[
                               'bki-dic:DataZaprKredOtch','bki-dic:OsnAnKredIstEnumerator','bki-dic:DataAnKredIst',
                               'bki-dic:PolnNaim', 'bki-dic:OGRN', 'bki-dic:Surname', 'bki-dic:Name',
                               'bki-dic:MiddleName', 'bki-dic:OGRNIP',
                               'bki-dic:SeriyaDokUdostLichnost', 'bki-dic:NomDokUdostLichnost',
                               'bki-dic:DataRozhdeniya', 'bki-dic:INN', 'bki-dic:TIN',
                               'bki-dic:KodStranyRegistrEnumerator',
                               'bki-dic:TipOrg_IstochnFormKredIstEnumerator',
                               'bki-dic:TipOrg_PolzKredIst_LiczoZaprKredOtchEnumerator',
                               'bki-dic:IstochnFormKredIstEnumerator',
                               'bki-dic:PolzKredIst_LiczoZaprKredOtchEnumerator',
                               'bki-dic:SubKredIst_PopechOpekInojZakPredstFLSubKredIstEnumerator'
                               ]

        self.taxis_list_to_save=['dim-int:ID_PolzKredIstTaxis','dim-int:ID_SubKredIstTaxis']
        self.taxis_list_to_save_reestr='dim-int:ID_KontragentaTaxis'

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
        var = vv[key] if vv['@dimension'] in self.taxis_list_to_save else None
        if var:
            self.id_to_reestr.append(var)

    def generator_value(self,iterable):
        iterator = iter(iterable)
        for xx in iterator:
            key=[kk for kk in xx['xbrli:scenario']['xbrldi:typedMember'].keys() if '@' not in kk][0]
            value=xx['xbrli:scenario']['xbrldi:typedMember'].get(key)
            if self.binary_search(self.id_to_reestr,0,len(self.id_to_reestr)-1,int(value.encode('utf-8').hex(), 16))!=-1:
                yield xx


    def check_varible(self,varible):
        print('проверяю переменную',varible, datetime.datetime.now())
        if type(self.instance['xbrli:xbrl'][varible]) == list:
            self.instance['xbrli:xbrl'][varible] = [xx for j,xx in enumerate(self.instance['xbrli:xbrl'][varible]) if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(xx['@contextRef'].encode('utf-8').hex(), 16))!=-1]
        else:
            self.instance['xbrli:xbrl'][varible] = self.instance['xbrli:xbrl'][varible] if self.binary_search(self.cnt_ids,0,len(self.cnt_ids)-1,int(self.instance['xbrli:xbrl'][varible]['@contextRef'].encode('utf-8').hex(), 16))!=-1 else None

    def readXBRL(self,path):
        print('читаю файл',datetime.datetime.now())
        with open(path, encoding='utf-8') as xml_file:
            self.instance = xmltodict.parse(xml_file.read())
        print('прочитал файл',datetime.datetime.now())
        varibles=[varible for varible in self.instance['xbrli:xbrl'].keys() if '@' not in varible and varible not in ('link:schemaRef','xbrli:context','xbrli:unit')]
        context_list=[]
        context_reestr_list=[]

        print('удаляю лишнее', datetime.datetime.now())
        self.instance['xbrli:xbrl']['xbrli:context'] = [i for j, i in
                                                        enumerate(self.instance['xbrli:xbrl']['xbrli:context']) if
                                                        'xbrli:scenario' in i.keys()]
        self.instance['xbrli:xbrl']['xbrli:context'] = [i for j, i in
                                                        enumerate(self.instance['xbrli:xbrl']['xbrli:context']) if
                                                        'xbrldi:typedMember' in i['xbrli:scenario'].keys()]


        print('ищу нужные контексты',datetime.datetime.now())
        self.contexts=self.instance['xbrli:xbrl']['xbrli:context']
        for idx,context in enumerate(self.contexts):
            if type(context['xbrli:scenario']['xbrldi:typedMember'])==list:
                dim=[xx['@dimension'] for xx in context['xbrli:scenario']['xbrldi:typedMember']]
                if numpy.isin(self.taxis_list_to_save, dim).all():
                    temp_=[self.var_value(vv) for vv in context['xbrli:scenario']['xbrldi:typedMember']]
                    context_list.append(context)
            else:
                dim=context['xbrli:scenario']['xbrldi:typedMember'].get('@dimension')
                if dim==self.taxis_list_to_save_reestr:
                    context_reestr_list.append(context)


        self.id_to_reestr = list(set(self.id_to_reestr))
        self.id_to_reestr = [int(xx.encode('utf-8').hex(), 16) for xx in self.id_to_reestr]
        self.id_to_reestr.sort()

        print('ищу нужные контексты в реестре', datetime.datetime.now())
        context_reestr_list=[context_ for context_ in self.generator_value(context_reestr_list)]

        # context_reestr_list=[context for idx,context in enumerate(context_reestr_list) if context['xbrli:scenario']['xbrldi:typedMember'].get([kk for kk in context['xbrli:scenario']['xbrldi:typedMember'].keys() if '@' not in kk][0]) in self.id_to_reestr]

        res_cnt=context_list+context_reestr_list
        self.cnt_ids=[]
        for xx in res_cnt:
            self.cnt_ids.append(xx['@id'])
        self.cnt_ids=[int(xx.encode('utf-8').hex(), 16) for xx in self.cnt_ids]
        self.cnt_ids.sort()

        for varible in varibles:
            if varible not in self.var_list_to_save:
                del self.instance['xbrli:xbrl'][varible]

        varibles=[xx for xx in varibles if xx in self.var_list_to_save]


        with ThreadPool(processes=20) as pool:
            pool.map(self.check_varible, varibles)


        self.instance['xbrli:xbrl']['xbrli:context'] = res_cnt
        xml=xmltodict.unparse(self.instance,pretty=True)
        print(type(xml))

        with open('test-split.xml','w',encoding='utf-8') as f:
            f.write(xml)


if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_5077746740121_ep_nso_bki_q_y_15rd_20230331.xml')
    #ss.readXBRL('XBRL_5077746740121_ep_nso_bki_q_y_15rd_20230331.xml')

    None