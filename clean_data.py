from pymongo import MongoClient

def clean_2024_data():
    """
    Clean 2024 crime data - Fixed version (handles string coordinates)
    """
    
    # Connect to database
    client = MongoClient('mongodb://localhost:27017/')
    db = client['nyc_crime']
    source_collection = db['complaints_2024']
    clean_collection = db['complaints_2024_clean']
    
    print("="*70)
    print("DATA CLEANING - NYC Crime 2024 (Fixed Version)")
    print("="*70)
    
    original_count = source_collection.count_documents({})
    print(f"\n📊 Original records: {original_count:,}")
    
    # Clear target collection
    clean_collection.delete_many({})
    print("✓ Cleared target collection")
    
    print("\n🧹 Cleaning steps:")
    
    # Cleaning rules
    print("\n1️⃣  Processing all records...")
    
    seen_ids = set()
    records_to_insert = []
    
    skipped_missing = 0
    skipped_invalid_coords = 0
    skipped_duplicate = 0
    processed = 0
    
    for record in source_collection.find():
        processed += 1
        
        if processed % 50000 == 0:
            print(f"   Processing: {processed:,} / {original_count:,}")
        
        # Check required fields
        cmplnt_id = record.get('cmplnt_num')
        cmplnt_dt = record.get('cmplnt_fr_dt')
        boro = record.get('boro_nm')
        lat_str = record.get('latitude')
        lon_str = record.get('longitude')
        ofns = record.get('ofns_desc')
        law_cat = record.get('law_cat_cd')
        
        # Skip records with missing key fields
        if not all([cmplnt_id, cmplnt_dt, boro, lat_str, lon_str, ofns, law_cat]):
            skipped_missing += 1
            continue
        
        # Skip records with null borough
        if boro == "(null)" or boro == "":
            skipped_missing += 1
            continue
        
        # Skip duplicate IDs
        if cmplnt_id in seen_ids:
            skipped_duplicate += 1
            continue
        
        # Convert and validate coordinates
        try:
            lat = float(lat_str)
            lon = float(lon_str)
            
            # Validate NYC coordinate range
            if not (40.4 <= lat <= 41.0 and -74.4 <= lon <= -73.6):
                skipped_invalid_coords += 1
                continue
                
        except (ValueError, TypeError):
            skipped_invalid_coords += 1
            continue
        
        # Record processed ID
        seen_ids.add(cmplnt_id)
        
        # Create cleaned record
        cleaned_record = {
            'cmplnt_num': cmplnt_id,
            'cmplnt_fr_dt': cmplnt_dt,
            'cmplnt_fr_tm': record.get('cmplnt_fr_tm', '00:00:00'),
            'boro_nm': boro,
            'latitude': lat,  # Store as float
            'longitude': lon,  # Store as float
            'ofns_desc': ofns,
            'law_cat_cd': law_cat
        }
        
        records_to_insert.append(cleaned_record)
        
        # Batch insert
        if len(records_to_insert) >= 5000:
            clean_collection.insert_many(records_to_insert)
            records_to_insert = []
    
    # Insert remaining records
    if records_to_insert:
        clean_collection.insert_many(records_to_insert)
    
    final_count = clean_collection.count_documents({})
    
    print(f"\n   ✓ Processing complete: {processed:,} records")
    
    # 2. Create indexes
    print("\n2️⃣  Creating indexes...")
    clean_collection.create_index([("cmplnt_num", 1)], unique=True)
    clean_collection.create_index([("cmplnt_fr_dt", 1)])
    clean_collection.create_index([("boro_nm", 1)])
    clean_collection.create_index([("ofns_desc", 1)])
    clean_collection.create_index([("law_cat_cd", 1)])
    clean_collection.create_index([("latitude", 1), ("longitude", 1)])
    print("   ✓ Indexes created")
    
    # 3. Summary statistics
    print("\n" + "="*70)
    print("📊 CLEANING SUMMARY")
    print("="*70)
    print(f"\nOriginal records:           {original_count:>10,}")
    print(f"Cleaned records:            {final_count:>10,}")
    print(f"\nRemoved:")
    print(f"  - Missing fields:         {skipped_missing:>10,}")
    print(f"  - Invalid coordinates:    {skipped_invalid_coords:>10,}")
    print(f"  - Duplicates:             {skipped_duplicate:>10,}")
    print(f"  - Total removed:          {original_count - final_count:>10,} ({(original_count - final_count)/original_count*100:.1f}%)")
    print(f"\nRetention rate:             {final_count/original_count*100:>10.1f}%")
    
    # 4. Cleaned data distribution
    print("\n" + "="*70)
    print("📈 CLEANED DATA DISTRIBUTION")
    print("="*70)
    
    print("\n🏙️  By Borough:")
    pipeline = [
        {"$group": {"_id": "$boro_nm", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in clean_collection.aggregate(pipeline):
        pct = (doc['count'] / final_count * 100)
        print(f"  {doc['_id']:20s}: {doc['count']:>8,} ({pct:>5.1f}%)")
    
    print("\n⚖️  By Crime Level:")
    pipeline = [
        {"$group": {"_id": "$law_cat_cd", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in clean_collection.aggregate(pipeline):
        pct = (doc['count'] / final_count * 100)
        print(f"  {doc['_id']:20s}: {doc['count']:>8,} ({pct:>5.1f}%)")
    
    print("\n🚨 Top 10 Crime Types:")
    pipeline = [
        {"$group": {"_id": "$ofns_desc", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    for i, doc in enumerate(clean_collection.aggregate(pipeline), 1):
        pct = (doc['count'] / final_count * 100)
        crime_type = doc['_id'][:40]  # Truncate long names
        print(f"  {i:2d}. {crime_type:40s}: {doc['count']:>6,} ({pct:>4.1f}%)")
    
    # 5. Validate coordinates
    print("\n🗺️  Coordinate Validation:")
    sample_coords = list(clean_collection.find().limit(3))
    for i, record in enumerate(sample_coords, 1):
        print(f"  {i}. lat={record['latitude']:.6f}, lon={record['longitude']:.6f} (type: float)")
    
    print("\n" + "="*70)
    print("✅ Data cleaning complete!")
    print("   Clean data saved to: complaints_2024_clean")
    print("="*70)
    
    client.close()

if __name__ == "__main__":
    print("This will create a new clean dataset: complaints_2024_clean")
    print("- Removes records with missing fields")
    print("- Validates and converts coordinates to float")
    print("- Removes duplicates")
    print("- Filters invalid NYC coordinates")
    
    confirm = input("\n👉 Start cleaning? (yes/no): ")
    
    if confirm.lower() == 'yes':
        clean_2024_data()
    else:
        print("Cancelled.")