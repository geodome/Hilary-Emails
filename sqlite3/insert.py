"""
This program retrieves the emails from the SQLite3 repository and inserts it into the Elasticsearch server

@author Donaldson Tan
@python >= 3
@since  6 March 2016
"""

import sqlite3
import json
import requests

# First setup data fields for both SQLite3 and Elasticsearch
sqlfields = [  "DocNumber", "MetadataSubject", "MetadataTo", "MetadataFrom", "MetadataDateSent",
            "MetadataDateReleased", "MetadataPdfLink", "MetadataCaseNumber", "MetadataDocumentClass",
            "ExtractedSubject", "ExtractedTo", "ExtractedFrom", "ExtractedCc", "ExtractedDateSent",
            "ExtractedCaseNumber", "ExtractedDocNumber", "ExtractedDateReleased",
            "ExtractedReleaseInPartOrFull", "ExtractedBodyText", "RawText"]

# First retrieve the emails from the SQLite3 repository
conn =  sqlite3.connect("database.sqlite")
cursor = conn.cursor()
query = "SELECT " + ", ".join(fields) + " FROM Emails"
emails = cursor.execute(query)

# Then insert email data into Elasticsearch
id = 1
for email in emails:
    data = dict(zip(fields, email))
    r = requests.put("http://localhost:9200/hillary/email/"+str(id), data=json.dumps(data))
    id += 1
