from sodapy import Socrata
from pymongo import MongoClient
import time

def download_2024_crime_data_robust():
    """
    Robust version: Download all 2024 data with resume capability and auto-retry
    """
    
    essential_fields = [
        'cmplnt_num', 'cmplnt_fr_dt', 'cmplnt_fr_tm',
        'boro_nm', 'latitude', 'longitude', 'ofns_desc', 'law_cat_cd'
    ]
    
    # Connect to MongoDB
    print("Connecting to MongoDB...")
    client = MongoClient('mongodb://localhost:27017/')
    db = client['nyc_crime']
    collection = db['complaints_2024']
    
    # Check existing data
    existing_count = collection.count_documents({})
    print(f"✓ Connected. Current records: {existing_count:,}")
    
    if existing_count > 0:
        response = input(f"\nContinue from {existing_count:,} records? (yes/no): ")
        if response.lower() != 'yes':
            print("Exiting...")
            client.close()
            return
        offset = existing_count
    else:
        offset = 0
    
    # Connect to API
    print("\nConnecting to NYC Open Data API...")
    client_api = Socrata("data.cityofnewyork.us", None, timeout=30)
    
    select_clause = ",".join(essential_fields)
    where_clause = (
        "latitude IS NOT NULL AND "
        "longitude IS NOT NULL AND "
        "cmplnt_fr_dt >= '2024-01-01T00:00:00.000' AND "
        "cmplnt_fr_dt <= '2024-12-31T23:59:59.999'"
    )
    
    batch_size = 5000  # Smaller batch size for stability
    total_imported = existing_count
    max_retries = 5
    
    print(f"\n🚀 Starting download...")
    print(f"   Starting from: {offset:,}")
    print(f"   Batch size: {batch_size}")
    print(f"   Max retries per batch: {max_retries}\n")
    
    while True:
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                print(f"Fetching {offset:,} to {offset + batch_size:,}...", end=" ")
                
                results = client_api.get(
                    "qgea-i56i",
                    select=select_clause,
                    where=where_clause,
                    limit=batch_size,
                    offset=offset,
                    order="cmplnt_fr_dt"
                )
                
                if not results:
                    print("✓ No more data.")
                    success = True
                    break
                
                if results:
                    collection.insert_many(results)
                    total_imported += len(results)
                    offset += len(results)
                    print(f"✓ {len(results)} records. Total: {total_imported:,}")
                    success = True
                    
                    if len(results) < batch_size:
                        print("\n✓ Reached end of data.")
                        break
                
                # Brief delay to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                retry_count += 1
                print(f"\n⚠️  Error (attempt {retry_count}/{max_retries}): {str(e)[:80]}")
                
                if retry_count < max_retries:
                    wait_time = retry_count * 5
                    print(f"   Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"\n❌ Failed after {max_retries} attempts.")
                    print(f"   Progress saved: {total_imported:,} records")
                    print(f"   Run script again to resume from offset {offset:,}")
                    break
        
        if not success:
            break
            
        if not results or len(results) < batch_size:
            break
    
    # Create indexes
    if total_imported > existing_count:
        print("\n📊 Creating/updating indexes...")
        collection.create_index([("cmplnt_fr_dt", 1)])
        collection.create_index([("boro_nm", 1)])
        collection.create_index([("ofns_desc", 1)])
        collection.create_index([("latitude", 1), ("longitude", 1)])
        print("✓ Indexes updated.")
    
    client.close()
    client_api.close()
    
    print(f"\n{'='*60}")
    print(f"✅ Download session completed!")
    print(f"   Total records in DB: {total_imported:,}")
    print(f"   New records added: {total_imported - existing_count:,}")
    print(f"{'='*60}")

if __name__ == "__main__":
    print("="*60)
    print("NYC Crime Data Downloader - 2024 (Robust Version)")
    print("="*60)
    print("\n✨ Features:")
    print("   • Auto-retry on errors (5 attempts)")
    print("   • Resume from interruption")
    print("   • Smaller batches (5,000 records)")
    print("   • Progress saved continuously")
    print("\n⚠️  This will download ALL 2024 data")
    print("   Estimated: 300K-400K records")
    print("   Time: 30-60 minutes")
    
    confirm = input("\n👉 Start download? (yes/no): ")
    
    if confirm.lower() == 'yes':
        download_2024_crime_data_robust()
    else:
        print("Cancelled.")