import numpy,xmltodict,json,xml.dom.minidom as minidom,xml.etree.ElementTree as ET,operator
import numpy as np


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
        with open(path, encoding='utf-8') as xml_file:
            instance = xmltodict.parse(xml_file.read())
        xml_root=instance['xbrli:xbrl']
        contexts=xml_root['xbrli:context']
        varibles=[varible for varible in xml_root.keys() if '@' not in varible and varible not in ('link:schemaRef','xbrli:context','xbrli:unit')]
        #print(instance['xbrli:xbrl']['bki-dic:DataVstupVSiluDogObUstupPravTreb'])

        for idx,context in enumerate(contexts):
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


        for cnt_id in self.context_data.keys():
            if 'taxis' in self.context_data[cnt_id].keys():
                if numpy.isin(self.taxis_list_to_save,list(self.context_data[cnt_id]['taxis'].keys())).all():
                    for tax_keys in self.context_data[cnt_id]['taxis'].keys():
                        if tax_keys in self.taxis_list_to_save:
                            self.varible_to_save.append(self.context_data[cnt_id]['taxis'][tax_keys])
                            self.cnt_to_save.append(self.context_data[cnt_id]['index'])
                            self.var_to_save.append(cnt_id)
                else:
                    None
        for cnt_id in self.context_data.keys():
            if 'taxis' in self.context_data[cnt_id].keys():
                if numpy.isin(self.taxis_list_to_save_reestr,list(self.context_data[cnt_id]['taxis'].keys())).all():
                    for tax_keys in self.context_data[cnt_id]['taxis'].keys():
                        if tax_keys in self.taxis_list_to_save_reestr and self.context_data[cnt_id]['taxis'][tax_keys] in self.varible_to_save:
                            self.cnt_to_save.append(self.context_data[cnt_id]['index'])
                            self.var_to_save.append(cnt_id)
                else:
                    None

        self.cnt_to_save=list(set(self.cnt_to_save))



        for varible in varibles:
            if varible not in self.var_list_to_save:
                print('удалил', varible)
                del instance['xbrli:xbrl'][varible]
                continue
            if type(instance['xbrli:xbrl'][varible]) == list:
                print(varible,len(instance['xbrli:xbrl'][varible]))
                instance['xbrli:xbrl'][varible]=[i for j,i in enumerate(instance['xbrli:xbrl'][varible]) if i['@contextRef'] in self.var_to_save]
                print(varible,len(instance['xbrli:xbrl'][varible]))
            else:
                print(varible, 'проверка')
                if instance['xbrli:xbrl'][varible]['@contextRef'] in self.var_to_save:
                    del instance['xbrli:xbrl'][varible]
                    print(varible, 'удалил')
                else:
                    print(varible,'оставил')

        print(len(self.cnt_to_save))
        print(len(instance['xbrli:xbrl']['xbrli:context']))
        instance['xbrli:xbrl']['xbrli:context']=[i for j,i in enumerate(instance['xbrli:xbrl']['xbrli:context']) if j in self.cnt_to_save]
        print(len(instance['xbrli:xbrl']['xbrli:context']))




        xml=xmltodict.unparse(instance,pretty=True)
        print(type(xml))

        with open('XBRL_1057746710713_ep_nso_bki_q_y_15rd_20230331-split.xml','w',encoding='utf-8') as f:
            f.write(xml)


if __name__ == "__main__":
    ss=splitXBRL()
    ss.readXBRL('XBRL_1057746710713_ep_nso_bki_q_y_15rd_20230331.xml')


    None