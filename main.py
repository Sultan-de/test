from xml.dom import minidom
import xml.etree.ElementTree as ET
import psycopg2
import datetime
import os
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="akmaganbetov")
cur = conn.cursor()
directory = 'files'
for filename in os.listdir(directory):
    print(filename)

    tree = ET.parse('files/' + filename)
    root = tree.getroot()
    print(root.attrib)

    id_file = root.attrib.get('ИдФайл')
    file_version = root.attrib.get('ВерсФорм')
    inf_type = root.attrib.get('ТипИнф')
    doc_cnt = root.attrib.get('КолДок')
    cur.execute( f"SELECT id_file FROM file_info where id_file = \'{id_file}\'")
    fetch = cur.fetchone()
    if fetch == None:
        cur.execute(f'insert into file_info (id_file, version, type, doc_cnt) values(\'{id_file}\',\'{file_version}\',\'{inf_type}\',{doc_cnt})')
        conn.commit()
    print(id_file,file_version,inf_type,doc_cnt)
    for child in root:
        if child.tag == 'Документ':
            print(child.tag, child.attrib)
            id_doc = child.attrib.get('ИдДок')
            create_date = datetime.datetime.strptime(child.attrib.get('ДатаСост'),'%d.%m.%Y').date()
            sub_type = child.attrib.get('ВидСуб')
            print(id_doc, create_date, sub_type)
            cur.execute(f"SELECT id_doc FROM document where id_doc = \'{id_doc}\'")
            fetch = cur.fetchone()
            if fetch == None:
                cur.execute(
                    f'insert into document (id_doc, date_created, sub_type, id_file) values(\'{id_doc}\',\'{create_date}\',\'{sub_type}\',\'{id_file}\')')
                conn.commit()
            for child1 in child:
                # print(child1.tag)
                # print(child1.attrib)
                if child1.tag == 'СвЮЛ':
                    iin_ul_sv = child1.attrib.get('ИННЮЛ')
                    naim_org_sv = child1.attrib.get('НаимОрг').replace('"','').replace("'",'')
                    print(iin_ul_sv, naim_org_sv)
                    cur.execute(f"SELECT id_doc FROM sv_ul where id_doc = \'{id_doc}\'")
                    fetch = cur.fetchone()
                    if fetch == None:
                        cur.execute(
                            f'insert into sv_ul (id_doc, iin_ul, org_name) values(\'{id_doc}\',\'{iin_ul_sv}\',\'{naim_org_sv}\')')
                        conn.commit()
                if child1.tag == 'СвПредПод':
                    pp_type = child1.attrib.get('ВидПП')
                    naim_org_pred = child1.attrib.get('НаимОрг')
                    iin_ul_pred = child1.attrib.get('ИННЮЛ')
                    cat_sub = child1.attrib.get('КатСуб')
                    srok_pod = datetime.datetime.strptime(child1.attrib.get('СрокПод'), '%d.%m.%Y').date()
                    prin_date = datetime.datetime.strptime(child1.attrib.get('ДатаПрин'), '%d.%m.%Y').date()
                    print(pp_type, naim_org_pred,iin_ul_pred,cat_sub,srok_pod,prin_date)
                    for child2 in child1:
                        #print(child2.tag, child2.attrib)
                        if child2.tag == 'ФормПод':
                            kod_form = child2.attrib.get('КодФорм')
                            naim_form = child2.attrib.get('НаимФорм')
                            print(kod_form, naim_form)
                        elif child2.tag == 'ВидПод':
                            kod_vid = child2.attrib.get('КодВид')
                            naim_vid = child2.attrib.get('НаимВид')
                            print(kod_vid, naim_vid)
                        elif child2.tag == 'РазмПод':
                            rasm_pod = child2.attrib.get('РазмПод')
                            ed_pod = child2.attrib.get('ЕдПод')
                            print(rasm_pod, ed_pod)
                        elif child2.tag == 'ИнфНаруш':
                            inf_narush = child2.attrib.get('ИнфНаруш')
                            inf_nacel = child2.attrib.get('ИнфНецел')
                            print(inf_narush,inf_nacel)
                        cur.execute(f"SELECT id_doc FROM sv_pred_pod where id_doc = \'{id_doc}\'")
                        fetch = cur.fetchone()
                    if fetch == None:
                        cur.execute(
                            f'insert into sv_pred_pod (id_doc, pp_type, org_name, iin_ul, sub_cat, srok_pod, date_prin, form_pod_code, vid_pod_code,rasm_pod, ed_ism_pod, inf_narush_id, inf_necel_id) '
                            f'values(\'{id_doc}\',\'{pp_type}\',\'{naim_org_pred}\',\'{iin_ul_pred}\',{cat_sub},\'{srok_pod}\',\'{prin_date}\',{kod_form},{kod_vid},{rasm_pod},{ed_pod},{inf_narush},{inf_nacel} )')
                        conn.commit()


