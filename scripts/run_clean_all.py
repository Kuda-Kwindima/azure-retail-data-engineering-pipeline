import subprocess

scripts = [
    "scripts/clean_products.py",
    "scripts/clean_aisles.py",
    "scripts/clean_departments.py",
    "scripts/clean_orders.py",
    "scripts/clean_order_products_prior.py",
    "scripts/clean_order_products_train.py"
]

all_success = True

for script in scripts:
    print(f"\n🚀 Running {script}...")
    
    result = subprocess.run(["python", script])

    if result.returncode != 0:
        print(f"❌ FAILED: {script}")
        all_success = False
        break
    else:
        print(f"✅ SUCCESS: {script}")

# Only validate if everything passed
if all_success:
    print("\n🔍 Running validation...")
    subprocess.run(["python", "scripts/validate_instacart_files.py"])
    print("🎯 Cleaning pipeline completed successfully")
else:
    print("\n⚠️ Pipeline stopped due to failure")