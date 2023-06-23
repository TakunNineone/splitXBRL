import numpy,xmltodict,json,xml.dom.minidom as minidom,xml.etree.ElementTree as ET,operator
import numpy as np,datetime


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
        self.taxis_list_to_save_reestr=['dim-int:ID_KontragentaTaxis']

    def readXBRL(self,path):
        print('читаю файл',datetime.datetime.now())
        with open(path, encoding='utf-8') as xml_file:
            self.instance = xmltodict.parse(xml_file.read())
        print('прочитал файл',datetime.datetime.now())
        varibles=[varible for varible in self.instance['xbrli:xbrl'].keys() if '@' not in varible and varible not in ('link:schemaRef','xbrli:context','xbrli:unit')]
        context_list=[]
        context_reestr_list=[]

        print('ищу нужные контексты',datetime.datetime.now())
        self.contexts=self.instance['xbrli:xbrl']['xbrli:context']
        for idx,context in enumerate(self.contexts):
            if 'xbrli:scenario' in context.keys():
                if 'xbrldi:typedMember' in context['xbrli:scenario'].keys():
                    if type(context['xbrli:scenario']['xbrldi:typedMember'])==list:
                        dim=[xx['@dimension'] for xx in context['xbrli:scenario']['xbrldi:typedMember']]
                        if numpy.isin(self.taxis_list_to_save, dim).all():
                            for vv in context['xbrli:scenario']['xbrldi:typedMember']:
                                if vv['@dimension'] in self.taxis_list_to_save:
                                    var=[vv[xx] for xx in vv.keys() if '@' not in xx]
                            self.id_to_reestr = self.id_to_reestr + var
                            context_list.append(self.contexts[idx])



        # for xx in context_list:
        #     print(xx)







if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_1057747734934_ep_nso_bki_q_y_15rd_20230331.xml')

    None