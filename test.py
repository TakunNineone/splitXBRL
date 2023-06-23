import numpy,xmltodict,json,xml.dom.minidom as minidom,xml.etree.ElementTree as ET,operator
import numpy as np,datetime


class splitXBRL():
    def __init__(self):
        self.context_data= {}
        self.varible_data= {}
        self.data_list_reverse= {}
        self.axis_synt_list=[]
        self.taxis_list=[]
        self.var_list=[]
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
        self.taxis_list_to_save_reestr=['dim-int:ID_KontragentaTaxis']
        self.cnt_to_save=[]
        self.varible_to_save=[]
        self.var_to_save=[]

    def generator_1(self,iterable):
        iterator = iter(iterable)
        varible_value,context_index,context_name=None,None,None
        for xx in iterator:
            if 'taxis' in self.context_data[xx].keys():
                if numpy.isin(self.taxis_list_to_save, list(self.context_data[xx]['taxis'].keys())).all():
                    for tax_keys in self.context_data[xx]['taxis'].keys():
                        if tax_keys in self.taxis_list_to_save:
                            varible_value=self.context_data[xx]['taxis'][tax_keys]
                            context_index=self.context_data[xx]['index']
                            context_name=xx
            yield (varible_value,context_index,context_name)
        yield (varible_value, context_index, context_name)

    def generator_2(self,iterable,cnt_id):
        iterator = iter(iterable)
        context_index, context_name = None, None
        for xx in iterator:
            if numpy.isin(self.taxis_list_to_save_reestr, xx).all():
                if xx in self.taxis_list_to_save_reestr and self.context_data[cnt_id]['taxis'][xx] in self.varible_to_save:
                    context_index = self.context_data[cnt_id]['index']
                    context_name = cnt_id
            yield (context_index,context_name)
        yield (context_index,context_name)

    def generator_reestr(self,iterable):
        iterator = iter(iterable)
        context_index, context_name = None,None
        for xx in iterator:
            if 'taxis' in self.context_data[xx].keys():
                for context_index_temp, context_name_temp in self.generator_2(self.context_data[xx]['taxis'].keys(), xx):
                    context_index=context_index_temp
                    context_name=context_name_temp
            yield context_index, context_name
        yield context_index, context_name


    def generator_4(selfself,iterable,contextRef):
        iterator = iter(iterable)
        res=False
        for xx in iterator:
            if xx==contextRef:
                res=True
            yield res
        yield res


    def generator_3(self,iterable,varible):
        iterator = iter(iterable)
        for xx in iterator:
            for res in self.generator_4(self.var_to_save,xx['@contextRef']):
                if res==True:
                    break
            if res==False:
                self.instance['xbrli:xbrl'][varible].remove(xx)
            yield (xx)
        yield (xx)



    def uniq(self, lst):
        last = object()
        for item in lst:
            if item == last:
                continue
            yield item
            last = item

    def list_to_dict(self,dict):
        taxis_list = {}
        for item in dict:
            keys = item.keys()
            for key in keys:
                taxis_list[key] = item[key]
        return taxis_list


    def sort_and_deduplicate(self, l):
        return list(self.uniq(sorted(l, reverse=True)))

    def readXBRL(self,path):
        print('читаю файл',datetime.datetime.now())
        with open(path, encoding='utf-8') as xml_file:
            self.instance = xmltodict.parse(xml_file.read())
        print('прочитал файл',datetime.datetime.now())
        varibles=[varible for varible in self.instance['xbrli:xbrl'].keys() if '@' not in varible and varible not in ('link:schemaRef','xbrli:context','xbrli:unit')]

        print('ищу нужные контексты',datetime.datetime.now())
        for idx,context in enumerate(self.instance['xbrli:xbrl']['xbrli:context']):
            self.context_data[context['@id']]={'index':idx}
            if 'xbrli:scenario' in context.keys():
                if 'xbrldi:typedMember' in context['xbrli:scenario'].keys():
                    self.context_data[context['@id']]['taxis']={}
                    if type(context['xbrli:scenario']['xbrldi:typedMember']) == list:
                        for tx in context['xbrli:scenario']['xbrldi:typedMember']:
                            for td_key in tx.keys():
                                if '@' not in td_key:
                                    self.context_data[context['@id']]['taxis'][tx['@dimension']]=tx[td_key]
                    else:
                        for td_key in context['xbrli:scenario']['xbrldi:typedMember'].keys():
                            if '@' not in td_key:
                                self.context_data[context['@id']]['taxis'][context['xbrli:scenario']['xbrldi:typedMember']['@dimension']]=context['xbrli:scenario']['xbrldi:typedMember'][td_key]
        print('нашел нужные контексты',datetime.datetime.now())



        print('проверка сохраняемых контекстов',datetime.datetime.now())
        for varible_value, context_index, context_name in self.generator_1(self.context_data.keys()):
            if varible_value: self.varible_to_save.append(varible_value)
            if context_index: self.cnt_to_save.append(context_index)
            if context_name: self.var_to_save.append(context_name)
        print('проверка сохраняемых контекстов закончена',datetime.datetime.now())


        print('проверка сохраняемых контекстов для реестра',datetime.datetime.now())
        for context_index, context_name in self.generator_reestr(self.context_data):
            if context_index: self.cnt_to_save.append(context_index)
            if context_name: self.var_to_save.append(context_name)
        print('проверка сохраняемых контекстов для реестра закончена',datetime.datetime.now())

        print('оставляю уникальные значения', datetime.datetime.now())
        self.cnt_to_save = list(set(self.cnt_to_save))
        print('завершено', datetime.datetime.now())

        for varible in varibles:
            if varible not in self.var_list_to_save:
                print('удалил', varible)
                del self.instance['xbrli:xbrl'][varible]
                continue
            if type(self.instance['xbrli:xbrl'][varible]) != list:
                print(varible, 'проверка')
                if self.instance['xbrli:xbrl'][varible]['@contextRef'] in self.var_to_save:
                    del self.instance['xbrli:xbrl'][varible]
                    print(varible, 'удалил')
                else:
                    print(varible, 'оставил')
            else:
                print('проверяю',varible)
                for var in self.generator_3(self.instance['xbrli:xbrl'][varible],varible):
                    None

        self.instance['xbrli:xbrl']['xbrli:context'] = [i for j, i in enumerate(self.instance['xbrli:xbrl']['xbrli:context']) if j in self.cnt_to_save]

        print('сохраняю файл')
        xml=xmltodict.unparse(self.instance,pretty=True)
        print(type(xml))

        with open('test.xml','w',encoding='utf-8') as f:
            f.write(xml)
        print('закончил')




if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_5077746740121_ep_nso_bki_q_y_15rd_20230331.xml')

    None