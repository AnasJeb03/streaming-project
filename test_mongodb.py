from pymongo import MongoClient
from datetime import datetime

# Connexion à MongoDB
client = MongoClient('mongodb://admin:admin123@localhost:27017/')

# Sélectionner la base de données
db = client['invoices_db']

# Sélectionner la collection
collection = db['invoices']

# Insérer un document de test
test_invoice = {
    "InvoiceNo": "TEST001",
    "CustomerID": 99999,
    "InvoiceDate": datetime.now(),
    "Country": "France",
    "Items": [
        {
            "StockCode": "TEST01",
            "Description": "Test Product",
            "Quantity": 1,
            "UnitPrice": 10.0,
            "TotalPrice": 10.0
        }
    ],
    "InvoiceTotal": 10.0,
    "inserted_at": datetime.now()
}

# Insérer
result = collection.insert_one(test_invoice)
print(f"✅ Document inséré avec l'ID : {result.inserted_id}")

# Lire
found_invoice = collection.find_one({"InvoiceNo": "TEST001"})
print(f"✅ Document trouvé : {found_invoice}")

# Compter les documents
count = collection.count_documents({})
print(f"✅ Nombre total de factures : {count}")

# Fermer la connexion
client.close()
print("✅ Connexion MongoDB fermée")