from pymongo import MongoClient
import pandas as pd

# Connect to database
client = MongoClient('mongodb://localhost:27017/')
db = client['nyc_crime']
collection = db['complaints_2024']

print("="*70)
print("DATA QUALITY ASSESSMENT - 2024 NYC Crime Data")
print("="*70)

total = collection.count_documents({})
print(f"\n📊 Total Records: {total:,}")

# 1. Check missing values for each field
print("\n" + "="*70)
print("🔍 MISSING VALUES CHECK")
print("="*70)

fields = ['cmplnt_num', 'cmplnt_fr_dt', 'cmplnt_fr_tm', 'boro_nm', 
          'latitude', 'longitude', 'ofns_desc', 'law_cat_cd']

issues = []

for field in fields:
    null_count = collection.count_documents({field: None})
    empty_count = collection.count_documents({field: ""})
    missing = null_count + empty_count
    pct = (missing / total * 100) if total > 0 else 0
    
    status = "✓" if missing == 0 else "⚠️"
    print(f"{status} {field:20s}: {missing:>8,} missing ({pct:>5.1f}%)")
    
    if missing > 0:
        issues.append(field)

# 2. Check for anomalies
print("\n" + "="*70)
print("🔍 ANOMALY CHECK")
print("="*70)

# Check coordinate range (NYC approximately: lat 40.5-40.9, lon -74.3--73.7)
invalid_coords = collection.count_documents({
    "$or": [
        {"latitude": {"$lt": "40.4"}},
        {"latitude": {"$gt": "41.0"}},
        {"longitude": {"$lt": "-74.4"}},
        {"longitude": {"$gt": "-73.6"}}
    ]
})

print(f"⚠️  Invalid coordinates: {invalid_coords:,} ({invalid_coords/total*100:.1f}%)")

# Check for duplicate complaint numbers
pipeline = [
    {"$group": {"_id": "$cmplnt_num", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]
duplicates = list(collection.aggregate(pipeline))
dup_count = len(duplicates)

print(f"⚠️  Duplicate complaint IDs: {dup_count:,}")

# 3. Data distribution check
print("\n" + "="*70)
print("📊 DATA DISTRIBUTION")
print("="*70)

# Borough distribution
print("\n🏙️  By Borough:")
pipeline = [
    {"$group": {"_id": "$boro_nm", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
for doc in collection.aggregate(pipeline):
    if doc['_id']:
        pct = (doc['count'] / total * 100)
        print(f"  {doc['_id']:20s}: {doc['count']:>8,} ({pct:>5.1f}%)")

# Crime level distribution
print("\n⚖️  By Crime Level:")
pipeline = [
    {"$group": {"_id": "$law_cat_cd", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
for doc in collection.aggregate(pipeline):
    if doc['_id']:
        pct = (doc['count'] / total * 100)
        print(f"  {doc['_id']:20s}: {doc['count']:>8,} ({pct:>5.1f}%)")

# 4. Date range check
print("\n📅 Date Range:")
earliest = collection.find_one(sort=[("cmplnt_fr_dt", 1)])
latest = collection.find_one(sort=[("cmplnt_fr_dt", -1)])
if earliest and latest:
    print(f"  Earliest: {earliest.get('cmplnt_fr_dt', 'N/A')}")
    print(f"  Latest:   {latest.get('cmplnt_fr_dt', 'N/A')}")

# 5. Cleaning recommendations
print("\n" + "="*70)
print("💡 CLEANING RECOMMENDATIONS")
print("="*70)

if issues:
    print("\n⚠️  Fields with missing values:")
    for field in issues:
        print(f"  • {field}")
    print("\n  Recommendation: Remove records with missing critical fields")
else:
    print("\n✓ No missing values in critical fields!")

if invalid_coords > 0:
    print(f"\n⚠️  {invalid_coords:,} records have invalid coordinates")
    print("  Recommendation: Remove or flag these records")

if dup_count > 0:
    print(f"\n⚠️  {dup_count:,} duplicate complaint IDs found")
    print("  Recommendation: Keep only first occurrence of each ID")

print("\n" + "="*70)
print("✅ Quality check complete!")
print("="*70)

client.close()