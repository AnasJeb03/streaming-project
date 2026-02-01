// Initialize MongoDB database and collection
db = db.getSiblingDB('invoices_db');

// Create invoices collection
db.createCollection('invoices');

// Create indexes for better query performance
db.invoices.createIndex({ "InvoiceNo": 1 }, { unique: true });
db.invoices.createIndex({ "CustomerID": 1 });
db.invoices.createIndex({ "Country": 1 });
db.invoices.createIndex({ "InvoiceDate": 1 });

print('✅ MongoDB initialized successfully!');
print('✅ Collection "invoices" created');
print('✅ Indexes created');