import json
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import time 

def has_class_but_no_id(tag):
    return tag.has_attr('class') and not tag.has_attr('id')

def parcourir_json(data, connection, indentation=0, parent_key=''):
    if isinstance(data, list):
        for i, item in enumerate(data):
            new_key = parent_key + '[{}]'.format(i) 
            parcourir_json(item, connection, indentation, new_key) 
    else:
        soup = BeautifulSoup(data['detail'], 'html.parser')
        soup = soup.find_all(has_class_but_no_id)
        detail = {}
        tags = {}

        for tag in soup: 
            if tag.has_attr('class'):
                class_attr = tag['class'][0]
                if class_attr in tags:
                    count = tags[class_attr] + 1
                    tags[class_attr] = count
                    class_attr += str(count)
                    detail[class_attr] = str(tag.text)
                else:
                    tags[class_attr] = 0
                    detail[tag['class'][0]] = str(tag.text)
            
        obj = {
            'id_communaute': data['id_communaute'],
            'id_consistoire': data['id_consistoire'],
            'id_region': data['id_region'],
            'id_ville_h': data['id_ville_h'],
            'nom': data['nom'],
            'adresse': data['adresse'],
            'code_postal': data['code_postal'],
            'ville': data['ville'],
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'tel': data['tel'],
            'email': data['email'],
            'consistoriale': data['consistoriale'],
            'associee': data['associee'],
            'autre': data['autre'],
            'site': data['site'],
            'image': data['image']
        }
        
        obj.update(detail)
        rows.append(obj)

def connectDatabase():
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                                database='local',
                                                user='samuel',
                                                password='samuel',
                                                port=10005)
        return connection
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)
    


def findIdPost(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name,'contact-des-synagogu'))
    return cursor.fetchone()

def findIdPostMembre(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name,'dirigeants'))
    return cursor.fetchone()

def findCommunaute(connection, id):
    cursor = connection.cursor(buffered=True)
    select = "SELECT post_title FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.ID = %s"
    cursor.execute(select, (id,))
    return cursor.fetchone()

def findIdSyna(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name,'synagogue'))
    return cursor.fetchone()

def createAndReturnIdMember(connection, name, actualTime, text):
    queryMembrePost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"

    membreContent = {
        "post_author": 1,
        "post_date": actualTime,
        "post_date_gmt": actualTime,
        "post_content": "",
        "post_title": name,
        "post_excerpt": "",
        "to_ping": "",
        "pinged": "",
        "post_name": text,
        "post_modified": actualTime,
        "post_modified_gmt": actualTime,
        "post_content_filtered": "",
        "guid": "http://consistoire.local/?p=11",
        "post_type": "dirigeants"
    }
    cursor.execute(queryMembrePost, membreContent)
    connection.commit()
    return cursor.lastrowid

def findIdRegion(id):
    regions = [
    'Région parisienne',
    'Alpes provence',
    'Bourgogne franche-comté',
    'Bretagne - pays de loire',
    'Centre ouest',
    'Champagne ardennes',
    'Côte d\'azur',
    'Languedoc',
    'Lorraine',
    'Nord',
    'Normandie',
    'Pays de la garonne',
    'Rhône-alpes - centre',
    'Sud ouest',
    'Dom tom',
    'Bas-rhin',
    'Haut-rhin',
    'Moselle'
    ]   
    return regions[id - 1]

def transform_value(value):
        if value == '0':
            return 'no'
        elif value == '1':
            return 'yes'
        else:
            return value

def countSynasByVille(rows):
    # Dictionary to keep track of counts
    counts = {}

    # Iterate through each item in the data
    for item in rows:
        # Get the value associated with the 'ville' key
        ville = item.get("ville")
        
        # If the value exists, increment the count
        if ville:
            counts[ville] = counts.get(ville, 0) + 1
    return counts

def insertData(connection, row, countsByVille):
    actualTime = time.strftime('%Y-%m-%d %H:%M:%S')

    # plsuieurs synagogues dans la ville
    if(countsByVille[row['ville']]) > 1:
        queryContactSynaPost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
        
        postContent = {
            "post_author": 1,
            "post_date": actualTime,
            "post_date_gmt": actualTime,
            "post_content": "",
            "post_title": row['nom'],
            "post_excerpt": "",
            "to_ping": "",
            "pinged": "",
            "post_name": row['nom'].lower().replace(" ", "-"),
            "post_modified": actualTime,
            "post_modified_gmt": actualTime,
            "post_content_filtered": "",
            "guid": "http://consistoire.local/?p=11",
            "post_type": "contact-des-synagogu"
        }

        cursor = connection.cursor(buffered=True)
        cursor.execute(queryContactSynaPost, postContent)
        connection.commit()

        try:
            id = cursor.lastrowid
            isDirigeant = False
            arrayIdsMembers = []

            for entry in row:
                if entry == 'id_region':
                    meta_key = 'region'
                    meta_value = findIdRegion(int(row[entry]))
                elif entry == 'id_communaute':
                    titleCommunaute = findCommunaute(connection, row[entry])
                    if titleCommunaute:
                        meta_key = 'nom-complet'
                        meta_value = titleCommunaute[0]
                    else:
                        continue
                elif entry == 'historique':
                    meta_key = 'historique'
                    meta_value = row[entry]
                elif entry in ['id_consistoire', 'id_ville_h']:
                    continue
                elif 'nom-prenom' in entry:
                    continue
                elif entry == 'ville':
                    meta_key = 'ville'
                    meta_value = row[entry].capitalize()
                elif 'fonction' in entry:
                    isDirigeant = True
                    number = re.findall(r'\d+', entry)
                    number = '' if not number else int(number[0])
                    text = row['nom-prenom'+str(number)].lower()
                    if ',' in text:
                        pattern = re.compile(r'\b[a-zA-ZÀ-ÿ\s\.\-]+(?=:)', re.UNICODE)
                        names = pattern.findall(text)
                        for name in names:
                            idPostMembre = findIdPostMembre(connection, name)
                            if not idPostMembre:
                            #     membreContent = {
                            #     "post_author": 1,
                            #     "post_date": actualTime,
                            #     "post_date_gmt": actualTime,
                            #     "post_content": "",
                            #     "post_title": name,
                            #     "post_excerpt": "",
                            #     "to_ping": "",
                            #     "pinged": "",
                            #     "post_name": name,
                            #     "post_modified": actualTime,
                            #     "post_modified_gmt": actualTime,
                            #     "post_content_filtered": "",
                            #     "guid": "http://consistoire.local/?p=11",
                            #     "post_type": "dirigeants"
                            # }
                                lastRowId = createAndReturnIdMember(connection, name ,actualTime)
                                arrayIdsMembers.append(lastRowId)
                            else:
                                arrayIdsMembers.append(idPostMembre[0])
                            query_postmetamember = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
                            meta_key = 'status'
                            meta_value = transform_value(row[entry])
                            cursor.execute(query_postmetamember, (idPostMembre[0], meta_key, meta_value))
                            connection.commit()
                    else:
                        if len(text) > 200:
                            text = text[:200]
                        idPostMembre = findIdPostMembre(connection, text)
                        if not idPostMembre:
                            membreContent = {
                                "post_author": 1,
                                "post_date": actualTime,
                                "post_date_gmt": actualTime,
                                "post_content": "",
                                "post_title": row['nom-prenom'+str(number)],
                                "post_excerpt": "",
                                "to_ping": "",
                                "pinged": "",
                                "post_name": text,
                                "post_modified": actualTime,
                                "post_modified_gmt": actualTime,
                                "post_content_filtered": "",
                                "guid": "http://consistoire.local/?p=11",
                                "post_type": "dirigeants"
                            }
                            lastRowId = createAndReturnIdMember(connection, name ,actualTime,text)
                            arrayIdsMembers.append(lastRowId)
                            # idPostMembre = findIdPostMembre(connection, row['nom-prenom'+str(number)])
                        else:
                            arrayIdsMembers.append(idPostMembre[0])
                        query_postmetamember = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
                        meta_key = 'status'
                        meta_value = transform_value(row[entry])
                        cursor.execute(query_postmetamember, (idPostMembre[0], meta_key, meta_value))
                        connection.commit()
                else:
                    meta_key = re.sub(r'\d+', '', entry)
                    meta_value = transform_value(row[entry])
                
                cursor.execute("INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)", (id, meta_key, meta_value))
                connection.commit()

            if isDirigeant:
                result = ";".join([f"i:{i};s:{len(str(value))}:\"{value}\"" for i, value in enumerate(arrayIdsMembers)])
                metaDirigeants = {
                    'meta_key': 'dirigeants',
                    'meta_value': f"a:{len(arrayIdsMembers)}:{{{result}}}"
                }
                idSyna = findIdSyna(connection, row['ville'].capitalize())
                if idSyna:
                    sqlLikeBetweenDirAndContact = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
                    cursor.execute(sqlLikeBetweenDirAndContact, (idSyna[0], metaDirigeants['meta_key'], metaDirigeants['meta_value']))
                    connection.commit()
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
    else:
        idSyna = findIdSyna(connection, row['ville'].capitalize())
        for entry in row:
            if entry == 'historique' and idSyna:
                cursor = connection.cursor(buffered=True)
                sqlLikeDescriptionPrincipale = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
                cursor.execute(sqlLikeDescriptionPrincipale, (idSyna[0], 'description-principale',row[entry]))
                connection.commit()
                # cursor.execute("UPDATE users SET name = %s, age = %s WHERE id = %s", (new_name, findIdSyna[0]))


# Lire le fichier JSON
url = 'http://www.consistoire.org/getJson?f=_communaute'
response = requests.get(url)
# Parcourir et structurer le fichier JSON
rows = []
connection = connectDatabase()
parcourir_json(response.json(), connection)
countsByVille = countSynasByVille(rows)
for row in rows:
    insertData(connection, row, countsByVille)

# excel_file_path = 'questions.xlsx'

# # Load the Excel file
# xls = pd.ExcelFile(excel_file_path)

# # Dictionary to store each worksheet as a key with data as values
# data = {}

# Iterate through each sheet in the Excel file
# for sheet_name in xls.sheet_names:
#     # Read data from the sheet
#     sheet_data = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    
    # Convert the data to JSON format (as a list of dictionaries)
    # sheet_json = sheet_data.to_dict(orient='records')
    
    # # Store the data in the dictionary
    # data[sheet_name] = sheet_json

# Write the dictionary to a JSON file
# with open('output.json', 'w') as json_file:
#     json.dump(data, json_file, indent=4)