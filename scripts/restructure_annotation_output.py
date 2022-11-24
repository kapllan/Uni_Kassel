import pandas as pd
from bs4 import BeautifulSoup
import json as js
from pprint import pprint
import re
from pathlib import Path
import os
from traceback import print_exc

translator = {
  "r_3" : "zbmfe9vmcn0(e_1|e_2)",
  "e_2" : "Premise",
  "f_20" : "A_Support",
  "f_7" : "C_Relevanz",
  "f_8" : "D_Zirkelschluss",
  "r_5" : "p3f65o55khi(e_4|e_1)",
  "r_11" : "idepthoebpi(e_2|e_2)",
  "e_4" : "MajorClaim",
  "f_18" : "F_Uebergeneralisierung",
  "f_9" : "E_LogischerFehler",
  "e_1" : "Claim"
}

def create_lookup_table(path_to_directory,removable_parts,prefix='a'):
    lookup_table = dict()
    for file in Path(path_to_directory).glob('**/*'):
        filepath = str(file)
        name_to_match = filepath.split('/')[-1]
        name_to_match = name_to_match.strip(removable_parts)
        if prefix is not None:
            if name_to_match.startswith('_'):
                lookup_table[prefix+name_to_match]=file
            elif bool(re.search('\d',name_to_match[0])):
                lookup_table[prefix+'.'+name_to_match]=file
            lookup_table[prefix+name_to_match]=file
        else:
            lookup_table[name_to_match]=file
    
    return lookup_table


def remove_unnecessary_substrings(text):
    text = text.strip()
    text = re.sub(' +', ' ', text)
    text = re.sub('\xa0', ' ',text)
    return text


def get_html_content(path_to_raw_html):
    
    html_content = open(path_to_raw_html,'r', encoding='utf8').read()    
    html_content = BeautifulSoup(html_content, features="lxml")
    html_content = str(html_content.text)
    html_content = re.sub('\n','',html_content)
    html_content = re.sub('a_gNso7y_Y6Q_RCTfWQFZ.s1XM50-473.txt','', html_content)
    html_content = remove_unnecessary_substrings(html_content)

    return html_content


def get_anotations(path_to_annotation_file):
    with open(path_to_annotation_file,"r") as f:
        annotated_data = js.load(f)

    return annotated_data


def get_annotated_parts(html_content, annotated_data):

    entities = annotated_data['entities']

    annotated_parts = []
    for ent in entities:
        classId = ent['classId']
        for e in ent['offsets']:
            start = e['start']
            original_text = remove_unnecessary_substrings(e['text'])
            for x in re.finditer(original_text, html_content):
                span_start = x.span()[0]
                span_end = x.span()[1]
                extracted_text = html_content[span_start:span_end]
                item = dict()
                item['classId']=classId
                item['tag']=translator[classId]
                item['span_start']=span_start
                item['span_end']=span_end
                item['original_text']=original_text
                item['extracted_text']=extracted_text
                if extracted_text==original_text:
                    annotated_parts.append(item)
                else:
                    print('Wait! There is a missmatch!')
                    print(original_text)
                    print('----------------')
                    print(extracted_text)
                    print('################################')

    return annotated_parts


def get_not_annotated_part(html_content, annotated_parts):
    unannotated_parts = list()
    current_iteration = list()
    for i, char in enumerate(html_content):
        tagged = False
        for span in annotated_parts:
            if i in range(span['span_start'], span['span_end']+1):
                tagged = True
        
        if tagged==False:
            current_iteration.append(i)
        else:
            if len(current_iteration)>0:
                span_start = current_iteration[0]
                span_end = current_iteration[-1]
                extracted_text = html_content[span_start:span_end]
                original_text = extracted_text
                item = dict()
                item['classId']='untagged'
                item['tag']='untagged'
                item['span_start']=span_start
                item['span_end']=span_end
                item['original_text']=original_text
                item['extracted_text']=extracted_text
                unannotated_parts.append(item)
                current_iteration=list()
    
    return unannotated_parts


def restructure(html_content, annotated_data):

    annotated_parts = get_annotated_parts(html_content=html_content, annotated_data=annotated_data)
    unannotated_parts = get_not_annotated_part(html_content=html_content, annotated_parts=annotated_parts)

    final_result = annotated_parts + unannotated_parts
    final_result = sorted(final_result, key=lambda x: x['span_start'])

    final_result = pd.DataFrame(final_result)

    return final_result



if __name__ == "__main__":
    path_to_corpus_as_html = '/Users/test/Documents/GitHub/Uni_Kassel/data/1_original_tagtog_data/Argumentative_Essays/plain.html/pool'
    path_to_ann = "/Users/test/Documents/GitHub/Uni_Kassel/data/1_original_tagtog_data/Argumentative_Essays/ann.json/members/FH01/pool"
    look_up_table_html = create_lookup_table(path_to_corpus_as_html,'.txt.plain.html',prefix='a')
    look_up_table_ann = create_lookup_table(path_to_ann,'.txt.ann.json')

    print(sorted(list(look_up_table_html.keys()))[:10])
    print(sorted(list(look_up_table_ann.keys()))[:10])

    for key, path_to_raw_html in look_up_table_html.items():
        try:
            if key in look_up_table_ann.keys():
                path_to_annotation_file = look_up_table_ann[key]
                output_directory = '/'.join(str(path_to_annotation_file).split('/')[:-1])
                #output_directory = 'output'

                annotated_data = get_anotations(path_to_annotation_file=path_to_annotation_file)
                html_content = get_html_content(path_to_raw_html=path_to_raw_html)

                final_result = restructure(html_content, annotated_data)


                #if os.path.isdir(output_directory):
                final_result.to_excel(output_directory+'/'+key+'.xlsx', index=False)
                '''else:
                    os.mkdir(output_directory)
                    final_result.to_excel(output_directory+'/'+key+'.xlsx', index=False)'''
                print('Successful!')
            else:
                print('No match for :', key)
        except Exception as e:
            print(e)




        



    
    
