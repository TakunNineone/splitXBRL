import datetime
import xml.sax

import pandas as pd


class XbrlHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.result = []
        self.current_context={'id':'','instant':'','period_start':'','period_end':'','axis':[],'taxis':[]}
        self.in_context = False
        self.in_instant = False
        self.in_startdate = False
        self.in_enddate = False
        self.in_axis = False
        self.in_taxis = False

    def startElement(self, name,attrs):
        if name == 'xbrli:context':
            self.current_context['id'] = attrs['id']
            self.in_context = True

        if name == 'xbrli:instant' and self.in_context:
            self.in_instant = True

        if name == 'xbrli:startDate' and self.in_context:
            self.in_startdate = True
        if name == 'xbrli:endDate' and self.in_context:
            self.in_enddate = True

        if name =='xbrldi:explicitMember' and self.in_context:
            self.in_axis = True
            if 'axis' not in self.current_context.keys():
                self.current_context['axis']=[attrs['dimension']+'#']
            else:
                self.current_context['axis'].append(attrs['dimension']+'#')

        if name =='xbrldi:typedMember' and self.in_context:
            self.in_taxis = True
            if 'taxis' not in self.current_context.keys():
                self.current_context['taxis']=[attrs['dimension']+'#']
            else:
                self.current_context['taxis'].append(attrs['dimension']+'#')




    def characters(self, content):
        if self.in_instant:
            if 'period_start' not in self.current_context:
                self.current_context['period_start'] = content.strip()
                self.current_context['period_end'] = ''
            else:
                self.current_context['period_start'] += content.strip()
                self.current_context['period_end'] = ''

        if self.in_startdate:
            if 'period_start' not in self.current_context:
                self.current_context['period_start'] = content.strip()
            else:
                self.current_context['period_start'] += content.strip()

        if self.in_enddate:
            if 'period_end' not in self.current_context:
                self.current_context['period_end'] = content.strip()
            else:
                self.current_context['period_end'] += content.strip()



        if self.in_axis:
            self.current_context['axis'][-1]+=content.strip()

        if self.in_taxis:
            self.current_context['taxis'][-1]+=content.strip()

    def endElement(self, name):
        if name == 'xbrli:context':
            self.result.append(self.current_context)
            self.current_context = {}
            self.in_context = False
            self.in_startdate = False
            self.in_instant = False
            self.in_enddate = False
        elif name == 'xbrli:instant':
            self.in_instant = False
        elif name == 'xbrli:endDate':
            self.in_enddate = False
        elif name == 'xbrli:startDate':
            self.in_startdate = False
        elif name == 'xbrldi:explicitMember':
            self.in_axis = False
        elif name == 'xbrldi:typedMember':
            self.in_taxis = False

def parse_xbrl_file(file_path):
    columns=['id','instant','period_start','period_end','axis','taxis']
    handler = XbrlHandler()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    parser.parse(file_path)
    res=pd.DataFrame(data=handler.result,columns=columns)
    return res

# Путь к XML файлу
file_path = 'XBRL_1027700186062_ep_SSDNEMED_10rd_sr_m_20230831.xml'

# Парсинг XML и получение результата
start_=datetime.datetime.now()
print('START:',start_)
result = parse_xbrl_file(file_path)
end_=datetime.datetime.now()
print('END:',end_)
duration = end_ - start_                         # For build-in functions
duration_in_s = duration.total_seconds()
minutes = divmod(duration_in_s, 60)
print('TOTAL TIME MIN:',minutes)
# print(result)
print('LENGTH CONTEXTS:',len(result))
None
