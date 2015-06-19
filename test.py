
import re
import itertools
import json
from phpserialize import *
from collections import OrderedDict

a='"O:8:""stdClass"":2:{s:7:""message"";s:61:""Searched within last 24 hours.  No results found for this Id."";s:6:""status"";i:404;}"'

a='"O:8:""stdClass"":6:{s:10:""likelihood"";d:0.90000000000000002220446049250313080847263336181640625;s:11:""contactInfo"";O:8:""stdClass"":3:{s:10:""familyName"";s:7:""Simmons"";s:9:""givenName"";s:5:""Debra"";s:8:""fullName"";s:13:""Debra Simmons"";}s:14:""socialProfiles"";a:1:{i:0;O:8:""stdClass"":6:{s:8:""typeName"";s:8:""Facebook"";s:6:""typeId"";s:8:""facebook"";s:2:""id"";s:7:""2533753"";s:4:""type"";s:8:""facebook"";s:3:""url"";s:37:""https://www.facebook.com/debrasimmons"";s:8:""username"";s:12:""debrasimmons"";}}s:6:""photos"";a:1:{i:0;O:8:""stdClass"":5:{s:9:""isPrimary"";b:1;s:8:""typeName"";s:8:""Facebook"";s:6:""typeId"";s:8:""facebook"";s:4:""type"";s:8:""facebook"";s:3:""url"";s:142:""https://d2ojpxxtu63wzl.cloudfront.net/static/4c813a7634ac054878fca2c7cca99f8a_0464c9003e5fe4fae294a8e9d2ede80778a66b45463360dae76655e4f2c2000d"";}}s:6:""status"";i:200;s:12:""demographics"";O:8:""stdClass"":1:{s:6:""gender"";s:6:""Female"";}}"'
a = a.replace('""',"'").replace('"',"").replace("'",'"')
a = re.sub(r'O:([0-9]*):"stdClass"', 'a', a)
a = loads(a,array_hook=OrderedDict)
a = json.dumps(a)





