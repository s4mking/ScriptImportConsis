# GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'root' WITH GRANT OPTION;
# consistoire comprend X synagogues
# on affiche pas l'étiquette vu que y a1 seule syna troyes
# on affiche pour marseille vu que multi synas
# chaque ville est un conssitoire de ville avec exception Paris qui est considéré comme un consistoire de ville
# Troyes
# 9 en multi consistoires
# exemple troyes est rattaché à un consistoire regional et en mee temps vu que c'est la seule synagogue c'est un consistoire de ville
# Consistoire de régions = fichier conssitoires transmis par Denis

# Consistoire de ville = Liste des villes (avec synagogue unique ou multiples) avec une exception pour Paris identifié par ID ville 13 dans le fichier communautés = une seule ville  

# Liste des consistoire de ville à compléter car multi synagogues

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

def insertData(connection, row):
    actualTime = time.strftime('%Y-%m-%d %H:%M:%S')

   
    data = {
        'login': 'toto',
        'pass': 'sam',
        'email': 'sam@sam.fr',
        'date': actualTime,
        'status': 0
    }

    queryContactSynaPost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s,  %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
    postContent = {
            "post_author": 1,
            "post_date": actualTime,
            "post_date_gmt": actualTime,
            "post_content": "",
            "post_title": row['nom'],
            "post_excerpt": "",
             "to_ping": "",
            "pinged": "",
            "post_name":  row['nom'].lower().replace(" ", "-"),
            "post_modified": actualTime,
            "post_modified_gmt": actualTime,
            "post_content_filtered": "",
            "guid": "http://consistoire.local/?p=11",
            "post_type": "contact-des-synagogu",
        }
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryContactSynaPost, postContent)
    connection.commit()


    id = findIdPost(connection, row['nom'])
    if id:
        isDirigeant = False
        arrayIdsMembers = []
        for entry in row:
            if entry == 'id_region':
                meta = {
                    'post_id': id[0],
                    'meta_key': 'region',
                    'meta_value': findIdRegion(int(row[entry]))
                }
            elif entry == 'id_communaute':
                titleCommunaute = findCommunaute(connection, row[entry])
                if titleCommunaute:
                    meta = {
                        'post_id': id[0],
                        'meta_key': 'nom-complet',
                        'meta_value': titleCommunaute[0]
                    }
                else:
                    continue
            elif entry == 'historique':
                meta = {
                        'post_id': id[0],
                        'meta_key': 'historique',
                        'meta_value': row[entry]
                    }
                
                
            elif entry in ['id_consistoire', 'id_ville_h']:
                continue
            elif  'nom-prenom' in entry:
                continue
            elif  entry ==  'ville':
                 meta = {
                        'post_id': id[0],
                        'meta_key': 'ville',
                        'meta_value': row[entry].capitalize()
                    }
            elif  'fonction' in entry:
                isDirigeant = True
                number  = re.findall(r'\d+', entry)
                number = '' if not number else int(number[0])
                queryMembrePost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s,  %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
                text = row['nom-prenom'+str(number)].lower().replace(" ", "-")
                if len(text) > 200:
                    text = text[:200]
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
                        "post_type": "dirigeants",
                    }
                cursor = connection.cursor(buffered=True)
                cursor.execute(queryMembrePost, membreContent)
                connection.commit()
                arrayIdsMembers.append(cursor.lastrowid)
                idPostMembre = findIdPostMembre(connection, row['nom-prenom'+str(number)])
                query_postmetamember = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)"
                meta = {
                    'post_id': idPostMembre[0],
                    'meta_key': 'status',
                    'meta_value': transform_value(row[entry])
                }

                cursor = connection.cursor(buffered=True)
                cursor.execute(query_postmetamember, meta)
                connection.commit()
            
            else:
                meta = {
                    'post_id': id[0],
                    'meta_key': re.sub(r'\d+', '', entry),
                    'meta_value': transform_value(row[entry])
                }
        if isDirigeant:
            result = "a:{}:{{".format(len(arrayIdsMembers))
            result += ";".join([f"i:{i};s:{len(str(value))}:\"{value}\"" for i, value in enumerate(arrayIdsMembers)])
            result += "}"
            metaDirigeants = {
                'post_id': idPostMembre[0],
                'meta_key': 'dirigeants',
                'meta_value': result
            }
            sqlLikeBetweenDirAndContact = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)"
            cursor = connection.cursor(buffered=True)
            cursor.execute(sqlLikeBetweenDirAndContact, metaDirigeants)
            connection.commit()
        sql_query3 = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)"
        cursor = connection.cursor(buffered=True)
        cursor.execute(sql_query3, meta)
        connection.commit()

            # metaConsistoire = {
            #     'post_id': id[0],
            #     'meta_key': 'consistoire',
            #     'meta_value': 'Consistoire de ville'
            # }
            # sql_consistoire = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)"
            # cursor = connection.cursor(buffered=True)
            # cursor.execute(sql_consistoire, metaConsistoire)
            # print("inserted successfully")

# Lire le fichier JSON
url = 'http://www.consistoire.org/getJson?f=_communaute'
response = requests.get(url)
# Parcourir et structurer le fichier JSON
rows = []
# print(json_data)
connection = connectDatabase()
parcourir_json(response.json(), connection)

# insertData(connection, rows)
for row in rows:
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
    #     arrayDatas = {
    #     "nom": row['nom'],
    #     "adresse-complete": row['adresse'],
    #     "ville": row['ville'],
    #     "latitude": row['latitude'],
    #     "longitude": row['longitude'],
    #     "numero-de-telephone": row['tel'],
    #     "email": row['email'],
    #     "consistoriale": row['consistoriale'],
    #     "associee": row['associee'],
    #     "autre": row['autre'],
    #     "site": row['site'],
    #     "image": row['image']
    # }