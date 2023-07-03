import json
import requests
import re
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import time

# How to run this script 
# python3 -m venv script
# source script/bin/activate
# pip install openpyxl 
# pip install mysql-connector-python 
# pip install beautifulsoup4
# pip install re
# python3 scriptImportConsistoire.py

def has_class_but_no_id(tag):
    return tag.has_attr("class") and not tag.has_attr("id")


def parcourir_json(data, connection, indentation=0, parent_key=""):
    if isinstance(data, list):
        for i, item in enumerate(data):
            new_key = parent_key + "[{}]".format(i)
            parcourir_json(item, connection, indentation, new_key)
    else:
        soup = BeautifulSoup(data["detail"], "html.parser")
        soup = soup.find_all(has_class_but_no_id)
        detail = {}
        tags = {}

        for tag in soup:
            if tag.has_attr("class"):
                class_attr = tag["class"][0]
                if class_attr in tags:
                    count = tags[class_attr] + 1
                    tags[class_attr] = count
                    class_attr += str(count)
                    detail[class_attr] = str(tag.text)
                else:
                    tags[class_attr] = 0
                    detail[tag["class"][0]] = str(tag.text)

        obj = {
            "id_communaute": data["id_communaute"],
            "id_consistoire": data["id_consistoire"],
            "id_region": data["id_region"],
            "id_ville_h": data["id_ville_h"],
            "nom": data["nom"],
            "adresse": data["adresse"],
            "code_postal": data["code_postal"],
            "ville": data["ville"],
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "tel": data["tel"],
            "email": data["email"],
            "consistoriale": data["consistoriale"],
            "associee": data["associee"],
            "autre": data["autre"],
            "site": data["site"],
            "image": data["image"],
        }

        obj.update(detail)
        rows.append(obj)


def connectDatabase():
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            database="local",
            user="samuel",
            password="samuel",
            port=10005,
        )
        return connection
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)


def findIdPost(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name, "contact-des-synagogu"))
    return cursor.fetchone()


def findIdPostMembre(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name, "dirigeants"))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def findCommunaute(connection, id):
    cursor = connection.cursor(buffered=True)
    select = "SELECT post_title FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.ID = %s"
    cursor.execute(select, (id,))
    return cursor.fetchone()
    


def findIdSynaByCpAndAdress(connection, row):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_postmeta.meta_value LIKE %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, ('%'+row['adresse']+'%', "synagogue"))
    result = cursor.fetchone()
    return result[0] if result is not None else None

def findIdSynaByVille(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name, "synagogue"))
    result = cursor.fetchone()
    return result[0] if result is not None else None

def getLastIdAddOne(connection):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id from J6e0wfWFh_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(select)
    return cursor.fetchone()[0]+1

def createAndReturnIdMember(connection, name, actualTime, text):
    queryMembrePost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
    lastId = getLastIdAddOne(connection)
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
        "guid": "http://consistoire.local/?post_type=dirigeants&#038;p="+str(lastId),
        "post_type": "dirigeants",
    }
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryMembrePost, membreContent)
    connection.commit()
    return cursor.lastrowid


def createPostMeta(connection, entry, meta_key, id):
    cursor = connection.cursor(buffered=True)
    query_postmetamember = "INSERT INTO J6e0wfWFh_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
    meta_value = transformValue(entry)
    cursor.execute(
        query_postmetamember,
        (id, meta_key, meta_value),
    )
    connection.commit()

def findcontactSynaAndReturnId(connection, actualTime):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_postmeta.meta_value LIKE %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, ('%'+row['adresse']+'%', "contact-des-synagogu"))
    result = cursor.fetchone()
    return result[0] if result is not None else None

def createPostContactSynaAndReturnId(connection, actualTime):
    queryContactSynaPost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
    lastId = getLastIdAddOne(connection)
    postContent = {
        "post_author": 1,
        "post_date": actualTime,
        "post_date_gmt": actualTime,
        "post_content": "",
        "post_title": row["nom"],
        "post_excerpt": "",
        "to_ping": "",
        "pinged": "",
        "post_name": row["nom"].lower().replace(" ", "-"),
        "post_modified": actualTime,
        "post_modified_gmt": actualTime,
        "post_content_filtered": "",
        "guid": "http://consistoire.local/?post_type=contact-des-synagogu&#038;p="+str(lastId),
        "post_type": "contact-des-synagogu",
    }

    cursor = connection.cursor(buffered=True)
    cursor.execute(queryContactSynaPost, postContent)
    connection.commit()
    return cursor.lastrowid


def findIdRegion(id):
    regions = [
        "Région parisienne",
        "Alpes provence",
        "Bourgogne franche-comté",
        "Bretagne - pays de loire",
        "Centre ouest",
        "Champagne ardennes",
        "Côte d'azur",
        "Languedoc",
        "Lorraine",
        "Nord",
        "Normandie",
        "Pays de la garonne",
        "Rhône-alpes - centre",
        "Sud ouest",
        "Dom tom",
        "Bas-rhin",
        "Haut-rhin",
        "Moselle",
    ]
    return regions[id - 1]


def transformValue(value):
    if value == "0":
        return "no"
    elif value == "1":
        return "yes"
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
    actualTime = time.strftime("%Y-%m-%d %H:%M:%S")
    multiSynas=False
    # Plusieurs synagogues dans la ville 
    if (countsByVille[row["ville"]]) > 1:
        idContactSyna = findcontactSynaAndReturnId(connection, row)
        if not idContactSyna:
            idContactSyna = createPostContactSynaAndReturnId(connection, actualTime)
        multiSynas = True

    # 1 syna = 1 ville
    else:
        #Si adrresse et code postal similaire alors on ne rentre pas
        idContactSyna = findIdSynaByVille(connection, row["ville"].capitalize())
    #la ville correspondante n'est pas trouvé on sort
        if not idContactSyna:
            return
    try:
        isDirigeant = False
        arrayIdsMembers = []

        for entry in row:
            # print(entry)
            # print(row[entry])
            if entry == "id_region":
                meta_key = "region"
                meta_value = findIdRegion(int(row[entry]))
            elif entry == "id_communaute":
                titleCommunaute = findCommunaute(connection, row[entry])
                if titleCommunaute:
                    meta_key = "nom-complet"
                    meta_value = titleCommunaute[0]
                else:
                    continue
            #Dans ma compréhension si on a 1 seul Syna alors c'estdescription-princiaple sinon c'est le detail non?
            elif entry == "historique":
                meta_key = "description-principale" if multiSynas else "detail"
                meta_value = row[entry]
            elif entry == "adresse":
                meta_key = "adresse-complete"
                adresseComplete = row[entry] + ' '+ row['code_postal']
                meta_value = adresseComplete
            elif entry == "tel":
                meta_key = "numero-de-telephone"
                meta_value = row[entry]
            elif entry == "nom":
                meta_key = "nom-complete"
                meta_value = row[entry]
            elif entry in ["id_consistoire", "id_ville_h", "code_postal"]:
                continue
            elif "nom-prenom" in entry:
                meta_key = "nom-complet-"
                meta_value = row[entry]
            elif entry == "ville":
                meta_key = "ville"
                meta_value = row[entry].capitalize()
            elif "fonction" in entry:
                isDirigeant = True
                number = re.findall(r"\d+", entry)
                number = "" if not number else int(number[0])
                text = row["nom-prenom" + str(number)].lower()
                if "," in text:
                    # pattern = re.compile(r"\b[a-zA-ZÀ-ÿ\s\.\-]+(?=:)", re.UNICODE)
                    # names = pattern.findall(text)
                    names = text.split(", ")
                    for name in names:
                        idPostMembre = findIdPostMembre(connection, name)
                        if not idPostMembre:
                            lastRowId = createAndReturnIdMember(
                                connection, name, actualTime, name
                            )
                            arrayIdsMembers.append(lastRowId)
                        else:
                            arrayIdsMembers.append(idPostMembre)
                            createPostMeta(
                                connection, row[entry], "status", idPostMembre
                            )
                else:
                    if len(text) > 200:
                        text = text[:200]
                    idPostMembre = findIdPostMembre(connection, text)
                    if not idPostMembre:
                        idPostMembre = createAndReturnIdMember(
                            connection, text, actualTime, text
                        )
                        arrayIdsMembers.append(idPostMembre)
                    #     # idPostMembre = findIdPostMembre(connection, row['nom-prenom'+str(number)])
                    else:
                        arrayIdsMembers.append(idPostMembre)
                    createPostMeta(
                        connection, row[entry], "status", idPostMembre
                    )
            else:
                meta_key = re.sub(r"\d+", "", entry)
                meta_value = transformValue(row[entry])
            createPostMeta(connection, meta_value, meta_key, idContactSyna)
        if isDirigeant:
            result = ";".join(
                [
                    f'i:{i};s:{len(str(value))}:"{value}"'
                    for i, value in enumerate(arrayIdsMembers)
                ]
            )
            metaDirigeants = {
                "meta_key": "dirigeants",
                "meta_value": f"a:{len(arrayIdsMembers)}:{{{result}}}",
            }
            idSyna = findIdSynaByCpAndAdress(connection, row)
            if idSyna:
                createPostMeta(
                    connection,
                    metaDirigeants["meta_value"],
                    metaDirigeants["meta_key"],
                    idSyna
                )
    except Exception as e:
        print(f"Error inserting data: {str(e)}")


# Lire le fichier JSON
url = "http://www.consistoire.org/getJson?f=_communaute"
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
