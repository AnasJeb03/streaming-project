import requests
import json
import os
import time
from pathlib import Path

class InvoiceClient:
    def __init__(self, api_url="http://localhost:8000"):
        self.api_url = api_url
        self.invoices_endpoint = f"{api_url}/invoices"
        self.stats_endpoint = f"{api_url}/stats"
    
    def check_api_health(self):
        """Vérifie que l'API est accessible"""
        try:
            response = requests.get(self.api_url, timeout=5)
            if response.status_code == 200:
                print("✅ API is reachable and healthy")
                print(f"   Response: {response.json()}")
                return True
            else:
                print(f"❌ API returned status code: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot reach API: {str(e)}")
            return False
    
    def send_invoice(self, invoice_data):
        """Envoie une seule invoice à l'API"""
        try:
            response = requests.post(
                self.invoices_endpoint,
                json=invoice_data,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                return True, result
            else:
                return False, f"Error {response.status_code}: {response.text}"
        
        except requests.exceptions.RequestException as e:
            return False, str(e)
    
    def get_stats(self):
        """Récupère les statistiques de l'API"""
        try:
            response = requests.get(self.stats_endpoint, timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except:
            return None
    
    def send_invoices_from_directory(self, directory_path, limit=None, delay=0.5, batch_size=10):
        """
        Envoie plusieurs invoices depuis un dossier
        
        Args:
            directory_path: Chemin vers le dossier contenant les JSON
            limit: Nombre maximum d'invoices à envoyer (None = tous)
            delay: Délai entre chaque envoi en secondes
            batch_size: Afficher les stats tous les X invoices
        """
        # Trouver tous les fichiers JSON
        invoice_dir = Path(directory_path)
        json_files = list(invoice_dir.glob("*.json"))
        
        if not json_files:
            print(f"❌ No JSON files found in {directory_path}")
            return
        
        total_files = len(json_files)
        files_to_process = json_files[:limit] if limit else json_files
        
        print(f"\n{'='*80}")
        print(f"📤 Starting to send invoices")
        print(f"{'='*80}")
        print(f"Total files found: {total_files}")
        print(f"Files to process: {len(files_to_process)}")
        print(f"Delay between sends: {delay}s")
        print(f"{'='*80}\n")
        
        success_count = 0
        error_count = 0
        
        start_time = time.time()
        
        for idx, json_file in enumerate(files_to_process, 1):
            try:
                # Lire le fichier JSON
                with open(json_file, 'r', encoding='utf-8') as f:
                    invoice_data = json.load(f)
                
                # Envoyer l'invoice
                success, result = self.send_invoice(invoice_data)
                
                if success:
                    success_count += 1
                    invoice_no = result.get('invoice_no', 'Unknown')
                    total = result.get('total', 0)
                    print(f"✅ [{idx}/{len(files_to_process)}] Invoice {invoice_no} sent successfully (Total: ${total:.2f})")
                else:
                    error_count += 1
                    print(f"❌ [{idx}/{len(files_to_process)}] Failed: {result}")
                
                # Afficher les stats tous les batch_size invoices
                if idx % batch_size == 0:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    print(f"\n📊 Progress: {idx}/{len(files_to_process)} | Success: {success_count} | Errors: {error_count} | Rate: {rate:.2f} inv/s\n")
                
                # Attendre avant le prochain envoi
                if idx < len(files_to_process):
                    time.sleep(delay)
            
            except Exception as e:
                error_count += 1
                print(f"❌ [{idx}/{len(files_to_process)}] Error reading {json_file.name}: {str(e)}")
        
        # Résumé final
        elapsed_time = time.time() - start_time
        print(f"\n{'='*80}")
        print(f"📊 FINAL SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Successfully sent: {success_count}")
        print(f"❌ Errors: {error_count}")
        print(f"⏱️  Total time: {elapsed_time:.2f}s")
        print(f"📈 Average rate: {success_count/elapsed_time:.2f} invoices/second")
        
        # Récupérer les stats de l'API
        stats = self.get_stats()
        if stats:
            print(f"\n📊 API Statistics:")
            print(f"   Total invoices sent: {stats.get('total_invoices_sent', 0)}")
            print(f"   Total revenue: ${stats.get('total_revenue', 0):.2f}")
            print(f"   Kafka errors: {stats.get('kafka_errors', 0)}")
        
        print(f"{'='*80}\n")


def main():
    """Fonction principale"""
    # Configuration
    API_URL = "http://localhost:8000"
    INVOICES_DIR = "data/json/invoices"
    
    # Options de test
    LIMIT = 100  # Nombre d'invoices à envoyer (None pour tout envoyer)
    DELAY = 0.2  # Délai entre chaque envoi (secondes)
    BATCH_SIZE = 10  # Afficher les stats tous les X invoices
    
    print("\n🚀 Invoice Streaming Client")
    print("="*80)
    
    # Créer le client
    client = InvoiceClient(api_url=API_URL)
    
    # Vérifier que l'API est accessible
    if not client.check_api_health():
        print("\n❌ Please start the API first with: py -3.13 -m uvicorn api.main:app --reload")
        return
    
    print("\n")
    
    # Demander confirmation
    if LIMIT:
        response = input(f"📤 Ready to send {LIMIT} invoices. Continue? (y/n): ")
    else:
        response = input(f"📤 Ready to send ALL invoices. Continue? (y/n): ")
    
    if response.lower() != 'y':
        print("❌ Cancelled by user")
        return
    
    # Envoyer les invoices
    client.send_invoices_from_directory(
        directory_path=INVOICES_DIR,
        limit=LIMIT,
        delay=DELAY,
        batch_size=BATCH_SIZE
    )


if __name__ == "__main__":
    main()