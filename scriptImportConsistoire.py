import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import time 

# GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'root' WITH GRANT OPTION;
# consistoire comprend X synagogues
# exemple troyes est rattaché à un consistoire regional et en mee temps vu que c'est la seule synagogue c'est un consistoire de ville

def has_class_but_no_id(tag):
    return tag.has_attr('class') and not tag.has_attr('id')

def parcourir_json(data, connection, indentation=0, parent_key=''):
    # if isinstance(data, dict):
    #     for key, value in data.items():
    #         new_key = parent_key + '.' + key if parent_key else key
    #         parcourir_json(value, indentation, new_key)
    if isinstance(data, list):
        for i, item in enumerate(data):
            new_key = parent_key + '[{}]'.format(i)
            parcourir_json(item, connection, indentation, new_key)
    else:
        # if isinstance(data, dict):
        #  print(data)
        #  for key, value in data.items():
        #      new_key = parent_key + '.' + key if parent_key else key
        #      parcourir_json(value, indentation, new_key)

        soup = BeautifulSoup(data['detail'], 'html.parser')
        soup = soup.find_all(has_class_but_no_id)
        detail = {}
        # value = tag.get_text().strip()
        # tags[class_attr] = value
        tags = {}

        for tag in soup: 
            if tag.has_attr('class'):
                class_attr = tag['class'][0]
                if class_attr in tags:
                    count = tags[class_attr] + 1
                    tags[class_attr] = count
                    class_attr += str(count)
                    # print(class_attr)
                    # tag['class']=class_attr
                    detail[class_attr] = str(tag.text)
                else:
                    tags[class_attr] = 0
                    detail[tag['class'][0]] = str(tag.text)
            
        obj = {'id_communaute': str(data['id_communaute']),'id_consistoire': str(data['id_consistoire']),
                      'id_region': str(data['id_region']), 'id_ville_h': str(data['id_ville_h']) ,'nom': str(data['nom']) ,
                      'adresse': str(data['adresse']), 'code_postal': str(data['code_postal']),  'ville': str(data['ville']),
                        'latitude': str(data['latitude']), 'longitude': str(data['longitude']),  'tel': str(data['tel']),
                        'email': str(data['email']), 'consistoriale': str(data['consistoriale']),  'associee': str(data['associee']),
                        'autre': str(data['autre']), 'site': str(data['site']),  'image': str(data['image'])
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
        # connectionDev = mysql.connector.connect(host='80.119.208.4',
        #                                      database='consistoirefr',
        #                                      user='wp_brrvv',
        #                                      password='64nL@_X5B@1*d?H&',
        #                                      port=8443)
        return connection
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)
    


def findIdPost(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s"
    cursor.execute(select, (name,))
    return cursor.fetchone()

def findCommunaute(connection, id):
    cursor = connection.cursor(buffered=True)
    select = "SELECT post_title FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.ID = %s"
    print(id)
    cursor.execute(select, (id,))
    return cursor.fetchone()

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
    print(id)
    return regions[id - 1]

def transform_value(value):
        if value == '0':
            return 'no'
        elif value == '1':
            return 'yes'
        else:
            return value

def insertData(connection, row):
    actualTime = time.strftime('%Y-%m-%d %H:%M:%S')
    data = {
        'login': 'toto',
        'pass': 'sam',
        'email': 'sam@sam.fr',
        'date': actualTime,
        'status': 0
    }

     #  "id_consistoire": "1", A quoi ça correspond?
        #  "id_ville_h": "13", A quoi ça correspond?
    arrayDatas = {
        "nom": row['nom'],
        "adresse": row['adresse'],
        "ville": row['ville'],
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "tel": row['tel'],
        "email": row['email'],
        "consistoriale": row['consistoriale'],
        "associee": row['associee'],
        "autre": row['autre'],
        "site": row['site'],
        "image": row['image']
    }

    id = findIdPost(connection, row['nom'])
    if id:
        for entry in row:
            if entry == 'id_region':
                meta = {
                    'post_id': id[0],
                    'meta_key': entry,
                    'meta_value': findIdRegion(int(row[entry]))
                }
            elif entry == 'id_communaute':
                titleCommunaute = findCommunaute(connection, row[entry])
                if titleCommunaute:
                    meta = {
                        'post_id': id[0],
                        'meta_key': 'titre',
                        'meta_value': titleCommunaute[0]
                    }
                else:
                    continue
            elif entry in ['id_consistoire', 'id_ville_h']:
                continue
            else:
                meta = {
                    'post_id': id[0],
                    'meta_key': re.sub(r'\d+', '', entry),
                    'meta_value': transform_value(row[entry])
                }
            sql_query3 = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)"
            cursor = connection.cursor(buffered=True)
            cursor.execute(sql_query3, meta)
            connection.commit()
            print("inserted successfully")

# Lire le fichier JSON
with open('file.json') as file:
    json_data = json.load(file)
# Parcourir et structurer le fichier JSON
rows = []
# print(json_data)
connection = connectDatabase()
parcourir_json(json_data, connection)

# insertData(connection, rows)
for row in rows:
    print(row)
    insertData(connection, row)
# Créer un DataFrame à partir des données structurées
# df = pd.DataFrame(rows)

# # Écrire le DataFrame dans un fichier Excel
# df.to_excel('result.xlsx', index=False)





 # sql_query = "INSERT INTO wp_users (user_login, user_pass, user_email, user_registered, user_status) VALUES (%(login)s, %(pass)s, %(email)s, %(date)s, %(status)s)"
        # sql_query = "INSERT INTO J6e0wfWFh_posts (user_login, user_pass, user_email, user_registered, user_status) VALUES (%(login)s, %(pass)s, %(email)s, %(date)s, %(status)s)"
        # sql_query2 = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_status, comment_status, ping_status, post_password, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, post_parent, guid, menu_order, post_type, post_mime_type, comment_count) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_status)s, %(comment_status)s, %(ping_status)s, %(post_password)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(post_parent)s, %(guid)s, %(menu_order)s, %(post_type)s, %(post_mime_type)s, %(comment_count)s)"
        # postContent = {
        #     "post_author": 1,
        #     "post_date": actualTime,
        #     "post_date_gmt": actualTime,
        #     "post_content": "",
        #     "post_title": "Samtest",
        #     "post_excerpt": "",
        #     "post_status": "publish",
        #     "comment_status": "closed",
        #     "ping_status": "closed",
        #     "post_password": "",
        #     "post_name": "kit-par-defaut",
        #     "to_ping": "",
        #     "pinged": "",
        #     "post_modified": actualTime,
        #     "post_modified_gmt": actualTime,
        #     "post_content_filtered": "",
        #     "post_parent": 0,
        #     "guid": "http://consistoire.local/?p=11",
        #     "menu_order": 0,
        #     "post_type": "elementor_library",
        #     "post_mime_type": "",
        #     "comment_count": 0
        # }