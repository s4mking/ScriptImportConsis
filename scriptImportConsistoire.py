import json
import requests
import re
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import time


def has_class_but_no_id(tag):
    return tag.has_attr("class") and not tag.has_attr("id")


def parcourirJsonCommunautes(data, connection, indentation=0, parent_key=""):
    if isinstance(data, list):
        for i, item in enumerate(data):
            new_key = parent_key + "[{}]".format(i)
            parcourirJsonCommunautes(item, connection, indentation, new_key)
    else:
        soupGlobal = BeautifulSoup(data["detail"], "html.parser")
        soup = soupGlobal.find_all(has_class_but_no_id)
        detail = {}
        tags = {}

        div_start_tag = soupGlobal.find('div', style='text-align:justify;')
        div_end_tag = soupGlobal.find('div', style='clear:both;')

        if div_start_tag and div_end_tag:
            extracted_text = ''
            current_tag = div_start_tag.find_next()
            while current_tag != div_end_tag:
                if current_tag.name == 'p':
                    extracted_text += current_tag.get_text() + ' '
                current_tag = current_tag.find_next()
            detail['historique'] = extracted_text.strip()

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
            "detailGlobal": data["detail"]
        }

        obj.update(detail)
        rowsCommunautes.append(obj)


def parcourirJsonConsistoire(data, connection, indentation=0, parent_key=""):
    if isinstance(data, list):
        for i, item in enumerate(data):
            new_key = parent_key + "[{}]".format(i)
            parcourirJsonConsistoire(item, connection, indentation, new_key)
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

        soup = BeautifulSoup(data["detail"], "html.parser")
        divs = soup.find_all("div")
        last_span_end = data["detail"].find(str(divs[-1])) + len(str(divs[-1]))
        text_after_last_span = data["detail"][last_span_end:].strip()
        obj = {
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
            "regional": data["regional"],
            "membres": text_after_last_span,
        }

        obj.update(detail)
        rowsConsistoires.append(obj)


def connectDatabase():
    try:
        local = {
            "host": "127.0.0.1",
            "database": "local",
            "user": "samuel",
            "password": "samuel",
            "port": 10005,
        }

        dev = {
            "host": "localhost",
            "database": "consistoirefr",
            "user": "wp_brrvv",
            "password": "64nL@_X5B@1*d?H&",
            "port": 3306,
        }

        connection = mysql.connector.connect(
            host=dev["host"],
            database=dev["database"],
            user=dev["user"],
            password=dev["password"],
            port=dev["port"],
        )

        return connection
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)


def findIdPostByType(connection, name, type):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, (name, type))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def findIfSameMetaNameWithSamePostId(connection, postId, metaname):
    cursor = connection.cursor(buffered=True)
    select = "SELECT meta_id FROM J6e0wfWFh_postmeta WHERE J6e0wfWFh_postmeta.post_id = %s AND J6e0wfWFh_postmeta.meta_key = %s "
    cursor.execute(select, (postId, metaname))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def findPostById(connection, id):
    cursor = connection.cursor(buffered=True)
    select = "SELECT post_title FROM J6e0wfWFh_posts WHERE J6e0wfWFh_posts.ID = %s"
    cursor.execute(select, (id,))
    return cursor.fetchone()


def findIdSynaByCpAndAdress(connection, row):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_postmeta.meta_value LIKE %s AND J6e0wfWFh_postmeta.meta_key = 'adresse-complete' AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(select, ("%" + row["adresse"] + "%", "synagogue"))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def selectEveryConsistoires(connection):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_posts.post_type = %s AND J6e0wfWFh_postmeta.meta_value LIKE %s"
    cursor.execute(select, ("synagogue", "Consistoire régional"))
    result = cursor.fetchall()
    return result


def findIdConsistoireRégionalByVille(connection, name):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_posts.post_title = %s AND J6e0wfWFh_posts.post_type = %s AND J6e0wfWFh_postmeta.meta_value LIKE %s"
    cursor.execute(select, (name, "synagogue", "%Consistoire régional%"))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def getLastIdAddOne(connection):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id from J6e0wfWFh_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(select)
    return cursor.fetchone()[0] + 1


def selectEverySynas(connection):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_posts.post_type = %s AND J6e0wfWFh_postmeta.meta_value NOT LIKE %s"
    cursor.execute(select, ("synagogue", "Consistoire régional"))
    result = cursor.fetchall()
    return result


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
        "guid": "http://consistoire.local/?post_type=dirigeants&#038;p=" + str(lastId),
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


def updatePostMeta(connection, entry, meta_key, id):
    cursor = connection.cursor(buffered=True)
    query_postmetamember = "UPDATE J6e0wfWFh_postmeta SET meta_value = %s  WHERE post_id = %s AND meta_key = %s"
    meta_value = transformValue(entry)
    cursor.execute(
        query_postmetamember,
        (meta_value, id, meta_key),
    )
    connection.commit()


def createOrUpdatePostMeta(connection, entry, meta_key, id):
    if findIfSameMetaNameWithSamePostId(connection, id, meta_key) is None:
        createPostMeta(connection, entry, meta_key, id)
    else:
        updatePostMeta(connection, entry, meta_key, id)


def findcontactSynaAndReturnId(connection):
    cursor = connection.cursor(buffered=True)
    select = "SELECT id FROM J6e0wfWFh_posts INNER JOIN J6e0wfWFh_postmeta ON J6e0wfWFh_postmeta.post_id = J6e0wfWFh_posts.id WHERE J6e0wfWFh_postmeta.meta_value LIKE %s AND J6e0wfWFh_posts.post_type = %s "
    cursor.execute(
        select,
        (
            "%" + communaute["adresse"] + " " + communaute["code_postal"] + "%",
            "contact-des-synagogu",
        ),
    )
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
        "post_title": communaute["nom"] if communaute["nom"] else 'synagogue',
        "post_excerpt": "",
        "to_ping": "",
        "pinged": "",
        "post_name": communaute["nom"].lower().replace(" ", "-") if communaute["nom"] else 'synagogue',
        "post_modified": actualTime,
        "post_modified_gmt": actualTime,
        "post_content_filtered": "",
        "guid": "http://consistoire.local/?post_type=contact-des-synagogu&#038;p="
        + str(lastId),
        "post_type": "contact-des-synagogu",
    }
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryContactSynaPost, postContent)
    connection.commit()
    return cursor.lastrowid


def createSynaAndReturnId(connection, actualTime, nomConsistoire):
    queryContactSynaPost = "INSERT INTO J6e0wfWFh_posts (post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, guid, post_type) VALUES (%(post_author)s, %(post_date)s, %(post_date_gmt)s, %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, %(post_modified_gmt)s, %(post_content_filtered)s, %(guid)s, %(post_type)s)"
    lastId = getLastIdAddOne(connection)
    postContent = {
        "post_author": 1,
        "post_date": actualTime,
        "post_date_gmt": actualTime,
        "post_content": "",
        "post_title": nomConsistoire,
        "post_excerpt": "",
        "to_ping": "",
        "pinged": "",
        "post_name": nomConsistoire.lower().replace(" ", "-"),
        "post_modified": actualTime,
        "post_modified_gmt": actualTime,
        "post_content_filtered": "",
        "guid": "http://consistoire.local/?post_type=synagogues&#038;p=" + str(lastId),
        "post_type": "synagogue",
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
        "Autre",
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


def updateConsistoireForSynas(connection):
    consistoires = selectEveryConsistoires(connection)
    synas = selectEverySynas(connection)
    for syna in synas:
        if findIfSameMetaNameWithSamePostId(connection, syna[0], "consistoire") is None:
            createPostMeta(connection, "Consistoire de ville", "consistoire", syna[0])
        else:
            updatePostMeta(connection, "Consistoire de ville", "consistoire", syna[0])

    for consistoire in consistoires:
        if findIfSameMetaNameWithSamePostId(connection, syna[0], "consistoire") is None:
            createPostMeta(
                connection, "Consistoire régionale", "consistoire", consistoire[0]
            )
        else:
            updatePostMeta(
                connection, "Consistoire régional", "consistoire", consistoire[0]
            )


def updateContactSynasForConsistoire(connection, listVilles):
    listConsistoireById = {
        1541: "CONSISTOIRE DU BAS-RHIN",
        1505: "CONSISTOIRE JUIF REGIONAL AUVERGNE RHONE-ALPES",
        1493: "CONSISTOIRE REGIONAL DU PAYS DE LA GARONNE",
        1468: "NICE - ACIN",
        1610: "CONSISTOIRE DE BOURGOGNE FRANCHE COMTÉ - DIJON",
        1: "CONSISTOIRE PARIS",
        1438: "CONSISTOIRE MARSEILLE",
        1558: "CONSISTOIRE REGIONAL DE LA MOSELLE"
    }
    for consistoire in listVilles:
        if consistoire != "0":

            idConsistoire = findIdConsistoireRégionalByVille(
                connection, listConsistoireById[int(consistoire)]
            )
            if idConsistoire:
                result = ";".join(
                    [
                        f'i:{i};s:{len(str(value))}:"{value}"'
                        for i, value in enumerate(listVilles[consistoire])
                    ]
                )
                result += ";"
                metaDirigeants = {
                    "meta_key": "contact-des-synagogues-de-la-ville",
                    "meta_value": f"a:{len(listVilles[consistoire])}:{{{result}}}",
                }
                if (
                    findIfSameMetaNameWithSamePostId(
                        connection, idConsistoire, metaDirigeants["meta_key"]
                    )
                    is None
                ):
                    createPostMeta(
                        connection,
                        metaDirigeants["meta_value"],
                        metaDirigeants["meta_key"],
                        idConsistoire,
                    )
                else:
                    updatePostMeta(
                        connection,
                        metaDirigeants["meta_value"],
                        metaDirigeants["meta_key"],
                        idConsistoire,
                    )


def insertDataContact(connection, communaute, countsByVille):
    
    actualTime = time.strftime("%Y-%m-%d %H:%M:%S")
    multiSynas = False
    # Plusieurs synagogues dans la ville
    idContactSyna = findcontactSynaAndReturnId(connection)
    if not idContactSyna:
        idContactSyna = createPostContactSynaAndReturnId(connection, actualTime)
    multiSynas = True
    # 1 syna = 1 ville
    # Si adresse et code postal similaire alors on ne rentre pas
    idSyna = findIdPostByType(
        connection, communaute["ville"].capitalize(), "synagogue"
    )
    # la ville correspondante n'est pas trouvé on sort
    if not idSyna:
        return
    try:
        if communaute["id_consistoire"] not in communautesByConsistoire:
            communautesByConsistoire[communaute["id_consistoire"]] = []
        communautesByConsistoire[communaute["id_consistoire"]].append(idContactSyna)

        if (countsByVille[communaute["ville"]]) > 1 and int(communaute["id_ville_h"]) == 13:
            if communaute["ville"] not in communautesByVillesParis:
                communautesByVillesParis[communaute["ville"]] = []
            communautesByVillesParis[communaute["ville"]].append(idContactSyna)

        isDirigeant = False
        arrayIdsMembers = []
        for entry in communaute:
            if entry == "id_region":
                meta_key = "region"
                meta_value = findIdRegion(int(communaute[entry]))
            elif entry == "id_communaute":
                titleCommunaute = findPostById(connection, communaute[entry])
                if titleCommunaute:
                    meta_key = "nom-complet"
                    meta_value = titleCommunaute[0]
                else:
                    continue
            # Dans ma compréhension si on a 1 seul Syna alors c'estdescription-princiaple sinon c'est le detail non?
            elif entry == "historique":
                # meta_key = "description-principale" if multiSynas else "detail"
                meta_key = "description-principale"
                meta_value = communaute[entry]
                #meta_value = communaute['detailGlobal'] 
               # entry == "historiqueBetweenp"
               # entry == "historique"
            elif entry == "adresse":
                meta_key = "adresse-complete"
                adresseComplete = communaute[entry] + " " + communaute["code_postal"]
                meta_value = adresseComplete
            elif entry == "tel":
                meta_key = "numero-de-telephone"
                meta_value = communaute[entry]
            elif entry == "nom":
                meta_key = "nom-complet"
                meta_value = communaute[entry] if communaute[entry] else 'synagogue'
            elif entry in ["id_consistoire", "id_ville_h", "code_postal"]:
                continue
            elif "nom-prenom" in entry:
                continue
                meta_key = "nom-complet-"
                meta_value = communaute[entry]
            elif entry == "ville":
                meta_key = "ville"
                if int(communaute["id_ville_h"]) == 13:
                    meta_value = "Paris"
                else:
                    meta_value = communaute[entry].capitalize()
            elif "fonction" in entry:
                isDirigeant = True
                number = re.findall(r"\d+", entry)
                number = "" if not number else int(number[0])
                text = communaute["nom-prenom" + str(number)].lower()
                if "," in text:
                    # pattern = re.compile(r"\b[a-zA-ZÀ-ÿ\s\.\-]+(?=:)", re.UNICODE)
                    # names = pattern.findall(text)
                    names = text.split(", ")
                    for name in names:
                         # Define a regex pattern to match email addresses
                        email_pattern = r'\S*@\S*'

                        # Define a regex pattern to match numbers
                        number_pattern = r'\b\d+\b'

                        # Combine the patterns using the OR (|) operator
                        combined_pattern = f'{email_pattern}|{number_pattern}'

                        # Use re.sub() to replace matched patterns with an empty string
                        cleaned_name = re.sub(combined_pattern, '', name)
                        idPostMembre = findIdPostByType(connection, cleaned_name, "dirigeants")
                        if not idPostMembre:
                            idPostMembre = createAndReturnIdMember(
                                connection, cleaned_name, actualTime, cleaned_name
                            )
                            createPostMeta(
                                connection, communaute[entry].replace('</div', ''), "status", idPostMembre
                            )
                            
                            arrayIdsMembers.append(idPostMembre)
                            
                            if findIfSameMetaNameWithSamePostId(connection, idPostMembre, "nom-complet-") is None:
                                createPostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)
                            else:
                                updatePostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)
                        else:
                            arrayIdsMembers.append(idPostMembre)
                        if findIfSameMetaNameWithSamePostId(connection, idPostMembre, "nom-complet-") is None:
                            createPostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)
                        else:
                            updatePostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)
                else:
                    if len(text) > 200:
                        text = text[:200]
                    idPostMembre = findIdPostByType(connection, text, "dirigeants")
                    if not idPostMembre:
                        idPostMembre = createAndReturnIdMember(
                            connection, text, actualTime, text
                        )
                        arrayIdsMembers.append(idPostMembre)
                    #     # idPostMembre = findIdPostByType(connection, communaute['nom-prenom'+str(number)])
                    else:
                        arrayIdsMembers.append(idPostMembre)

                    # createPostMeta(connection, communaute[entry], "status", idPostMembre)
                    idMeta = findIfSameMetaNameWithSamePostId(
                            connection, idPostMembre, meta_key
                        )
                    if (
                        idMeta is None
                    ):
                        createPostMeta(
                            connection, communaute[entry].replace('</div', ''), "status", idPostMembre
                        )
                    else:
                        updatePostMeta(
                            connection, communaute[entry].replace('</div', ''), "status", idMeta
                        )
                    if findIfSameMetaNameWithSamePostId(connection, idPostMembre, "nom-complet-") is None:
                            createPostMeta(connection, text, "nom-complet-", idPostMembre)
                    else:
                        updatePostMeta(connection, text, "nom-complet-", idPostMembre)
                    continue
            else:
                meta_key = re.sub(r"\d+", "", entry)
                meta_value = transformValue(communaute[entry])
            if (
                findIfSameMetaNameWithSamePostId(connection, idContactSyna, meta_key)
                is None
            ):
                createPostMeta(connection, meta_value, meta_key, idContactSyna)
            else:
                updatePostMeta(connection, meta_value, meta_key, idContactSyna)
            if (
                findIfSameMetaNameWithSamePostId(connection, idSyna, meta_key)
                is None
            ):
                createPostMeta(connection, meta_value, meta_key, idSyna)
            else:
                updatePostMeta(connection, meta_value, meta_key, idSyna)
        if isDirigeant:
            result = ";".join(
                [
                    f'i:{i};s:{len(str(value))}:"{value}"'
                    for i, value in enumerate(arrayIdsMembers)
                ]
            )
            result += ";"
            metaDirigeants = {
                "meta_key": "dirigeants",
                "meta_value": f"a:{len(arrayIdsMembers)}:{{{result}}}",
            }
            # if not communaute['adresse']:

            # idSyna = findIdSynaByCpAndAdress(connection, communaute)
            # if idSyna:
            if (
                findIfSameMetaNameWithSamePostId(
                    connection, idContactSyna, metaDirigeants["meta_key"]
                )
                is None
            ):
                createPostMeta(
                    connection,
                    metaDirigeants["meta_value"],
                    metaDirigeants["meta_key"],
                    idContactSyna,
                )
            else:
                updatePostMeta(
                    connection,
                    metaDirigeants["meta_value"],
                    metaDirigeants["meta_key"],
                    idContactSyna,
                )
    except Exception as e:
        print(f"Error inserting data: {str(e)}")


def insertDataConsistoires(connection, consistoire):
    actualTime = time.strftime("%Y-%m-%d %H:%M:%S")
    nomConsistoire = consistoire["nom"]
    if nomConsistoire == 'PARIS':
        nomConsistoire = 'CONSISTOIRE PARIS'
    if nomConsistoire == 'MARSEILLE':
        nomConsistoire = 'CONSISTOIRE MARSEILLE'
    idSyna = findIdConsistoireRégionalByVille(connection, nomConsistoire)
    if not idSyna:
        idSyna = createSynaAndReturnId(connection, actualTime, nomConsistoire)
        createPostMeta(connection, "Consistoire régional", "consistoire", idSyna)

    try:
        updatePostMeta(connection, "Consistoire régional", "consistoire", idSyna)
        isDirigeant = False
        arrayIdsMembers = []

        if findIfSameMetaNameWithSamePostId(connection, idSyna, "titre") is None:
                            createPostMeta(connection, nomConsistoire, "titre", idSyna)
        else:
            updatePostMeta(connection, nomConsistoire, "titre", idSyna)
        if findIfSameMetaNameWithSamePostId(connection, idSyna, "image-principale") is None:
                            createPostMeta(connection, '207', "image-principale", idSyna)
        for entry in consistoire:
            if entry == "id_region":
                meta_key = "region"
                meta_value = findIdRegion(int(consistoire[entry]))
            elif entry == "historique":
                # meta_key = "description-principale" if multiSynas else "detail"
                meta_key = "description-principale"
                meta_value = consistoire[entry]
            elif entry == "adresse":
                meta_key = "adresse-complete"
                adresseComplete = consistoire[entry] + " " + consistoire["code_postal"]
                meta_value = adresseComplete
            elif entry == "tel":
                meta_key = "numero-de-telephone"
                meta_value = consistoire[entry]
            elif entry == "nom":
                meta_key = "nom-complet"
                meta_value = nomConsistoire
            elif entry in ["id_consistoire", "id_ville_h", "code_postal"]:
                continue
            elif entry == "ville":
                meta_key = "ville"
                meta_value = consistoire[entry].capitalize()
            elif "membres" in entry:
                text = consistoire[entry]
                isDirigeant = True
                roles_and_names = text.split("<br>")

                # Création d'un dictionnaire pour stocker les rôles et les noms
                roles_and_names_dict = {}

                for role_and_name in roles_and_names:
                    # Séparation du rôle et du nom
                    if not role_and_name:
                        continue
                    role, names = role_and_name.split(":")
                    # Suppression des espaces blancs au début et à la fin
                    role = role.strip()
                    # Conversion des noms en une liste de noms, en supprimant les espaces blancs au début et à la fin de chaque nom
                    names = [name.strip() for name in names.split(",")]
                    # Ajout du rôle et des noms au dictionnaire
                    roles_and_names_dict[role] = names

                for role, names in roles_and_names_dict.items():
                    for name in names:
                        # Define a regex pattern to match email addresses
                        email_pattern = r'\S*@\S*'

                        # Define a regex pattern to match numbers
                        number_pattern = r'\b\d+\b'

                        # Combine the patterns using the OR (|) operator
                        combined_pattern = f'{email_pattern}|{number_pattern}'

                        # Use re.sub() to replace matched patterns with an empty string
                        cleaned_name = re.sub(combined_pattern, '', name)

                        idPostMembre = findIdPostByType(connection, cleaned_name, "dirigeants")
                        if not idPostMembre:
                            idPostMembre = createAndReturnIdMember(
                                connection, cleaned_name, actualTime, cleaned_name
                            )
                            createPostMeta(connection, role.replace('</div', ''), "status", idPostMembre)
                            
                            arrayIdsMembers.append(idPostMembre)
                        else:
                            arrayIdsMembers.append(idPostMembre)
                        
                        if findIfSameMetaNameWithSamePostId(connection, idPostMembre, "nom-complet-") is None:
                            createPostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)
                        else:
                            updatePostMeta(connection, cleaned_name, "nom-complet-", idPostMembre)

            else:
                meta_key = re.sub(r"\d+", "", entry)
                meta_value = transformValue(consistoire[entry])
            if findIfSameMetaNameWithSamePostId(connection, idSyna, meta_key) is None:
                createPostMeta(connection, meta_value, meta_key, idSyna)
            else:
                updatePostMeta(connection, meta_value, meta_key, idSyna)
        if isDirigeant:
            result = ";".join(
                [
                    f'i:{i};s:{len(str(value))}:"{value}"'
                    for i, value in enumerate(arrayIdsMembers)
                ]
            )
            result += ";"
            metaDirigeants = {
                "meta_key": "dirigeants",
                "meta_value": f"a:{len(arrayIdsMembers)}:{{{result}}}",
            }
            if (
                findIfSameMetaNameWithSamePostId(
                    connection, idSyna, metaDirigeants["meta_key"]
                )
                is None
            ):
                createPostMeta(
                    connection,
                    metaDirigeants["meta_value"],
                    metaDirigeants["meta_key"],
                    idSyna,
                )
            else:
                updatePostMeta(
                    connection,
                    metaDirigeants["meta_value"],
                    metaDirigeants["meta_key"],
                    idSyna,
                )
    except Exception as e:
        print(f"Error inserting data: {str(e)}")

def updateParisSynasTownsMulti(connection, communautesByVillesParis):
    for communaute in communautesByVillesParis:
        if communaute != 'PARIS':
            idSyna = findIdPostByType(
                    connection, communaute,"synagogue"
                )
            if (
                idSyna is not None
            ):
                result = ";".join(
                    [
                        f'i:{i};s:{len(str(value))}:"{value}"'
                        for i, value in enumerate(communautesByVillesParis[communaute])
                    ]
                )
                result += ";"
                metaDirigeants = {
                    "meta_key": "contact-des-synagogues-de-la-ville",
                    "meta_value": f"a:{len(communautesByVillesParis[communaute])}:{{{result}}}",
                }
                if (
                    findIfSameMetaNameWithSamePostId(
                        connection, idSyna, metaDirigeants["meta_key"]
                    )
                    is None
                ):
                    createPostMeta(
                        connection,
                        metaDirigeants["meta_value"],
                        metaDirigeants["meta_key"],
                        idSyna,
                    )
                else:
                    updatePostMeta(
                        connection,
                        metaDirigeants["meta_value"],
                        metaDirigeants["meta_key"],
                        idSyna,
                    )
            

connection = connectDatabase()

 
urlConsistoires = "http://www.consistoire.org/getJson?f=_consistoire"
responseConsistoires = requests.get(urlConsistoires)


# Parcourir et structurer le fichier JSON
rowsConsistoires = []
parcourirJsonConsistoire(responseConsistoires.json(), connection)
for consistoire in rowsConsistoires:
    insertDataConsistoires(connection, consistoire)


# Lire le fichier JSON
urlCommunautes = "http://www.consistoire.org/getJson?f=_communaute"
responseCommunautes = requests.get(urlCommunautes)
# Parcourir et structurer le fichier JSON
rowsCommunautes = []
parcourirJsonCommunautes(responseCommunautes.json(), connection)

countsByVille = countSynasByVille(rowsCommunautes)
communautesByConsistoire = {}
communautesByVillesParis = {}
for communaute in rowsCommunautes:
    insertDataContact(connection, communaute, countsByVille)
updateConsistoireForSynas(connection)
updateContactSynasForConsistoire(connection, communautesByConsistoire)
updateParisSynasTownsMulti(connection, communautesByVillesParis)