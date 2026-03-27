import zipfile
import xml.etree.ElementTree as ET
import re

def col2num(col):
    num = 0
    for c in col:
        num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num

with zipfile.ZipFile('Arena_Costings_TM.xlsx', 'r') as z:
    shared_strings = []
    if 'xl/sharedStrings.xml' in z.namelist():
        root = ET.fromstring(z.read('xl/sharedStrings.xml'))
        ns = {'ns': root.tag.split('}')[0] + '}'} if '}' in root.tag else {'ns': ''}
        ns_prefix = ns['ns']
        for si in root.findall(f'{ns_prefix}si'):
            t = si.find(f'{ns_prefix}t')
            if t is not None and t.text is not None:
                shared_strings.append(t.text)
            else:
                texts = [node.text for node in si.iter(f'{ns_prefix}t') if node.text]
                shared_strings.append(''.join(texts))
                
    sheets = [n for n in z.namelist() if n.startswith('xl/worksheets/sheet')]
    sheets.sort()
    for name in sheets[:2]: # First two sheets
        print(f"--- {name} ---")
        root = ET.fromstring(z.read(name))
        ns = {'ns': root.tag.split('}')[0] + '}'} if '}' in root.tag else {'ns': ''}
        ns_prefix = ns['ns']
        for row in root.iter(f'{ns_prefix}row'):
            row_num = row.attrib.get('r', '')
            cells = row.findall(f'{ns_prefix}c')
            if not cells: continue
            
            row_data = []
            for c in cells:
                r_attr = c.attrib.get('r', '')
                v = c.find(f'{ns_prefix}v')
                if v is not None and v.text is not None:
                    val = v.text
                    if c.attrib.get('t') == 's':
                        val = shared_strings[int(val)]
                    row_data.append(f"{r_attr}: {val.strip()[:50]}") # limit length
            if row_data:
                print(row_num, "\t".join(row_data))
                if int(row_num) > 50:
                    print("... stopping at row 50")
                    break
