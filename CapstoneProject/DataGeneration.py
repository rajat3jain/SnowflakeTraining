import csv
import random
from faker import Faker
import datetime

fake = Faker()

NUM_RECORDS = 1000  # Number of records to generate

# Get the current timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Generate Patient Data
def generate_patient_data():
    patient_ids = set()
    primary_care_provider_ids = set()
    
    filename = f"patient_data_{timestamp}.csv"
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "patient_id", "first_name", "last_name", "date_of_birth", "gender", "race", "ethnicity",
            "address", "city", "state", "zip_code", "insurance_type", "primary_care_provider_id"
        ])
        for _ in range(NUM_RECORDS):
            patient_id = fake.uuid4()
            primary_care_provider_id = fake.uuid4()
            patient_ids.add(patient_id)
            primary_care_provider_ids.add(primary_care_provider_id)
            writer.writerow([
                patient_id, fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=20, maximum_age=100).strftime('%Y-%m-%d'),
                random.choice(["Male", "Female", "Other"]), random.choice(["White", "Black", "Asian", "Other"]),
                random.choice(["Hispanic", "Non-Hispanic"]), fake.address(), fake.city(), fake.state(),
                fake.zipcode(), random.choice(["Private", "Medicare", "Medicaid"]),
                primary_care_provider_id
            ])
    return patient_ids, primary_care_provider_ids


# Generate Medical Events Data
def generate_medical_events(patient_ids, provider_ids):
    filename = f"medical_events_{timestamp}.csv"
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "event_id", "patient_id", "event_date", "event_type", "diagnosis_code", "procedure_code",
            "medication_code", "provider_id", "facility_id", "notes"
        ])
        for _ in range(NUM_RECORDS):
            patient_id = random.choice(list(patient_ids))
            provider_id = random.choice(list(provider_ids))
            writer.writerow([
                fake.uuid4(), patient_id, fake.date_this_decade().strftime('%Y-%m-%d'), random.choice(["Emergency", "Routine"]),
                f"DX{random.randint(1, 100):03d}", f"PR{random.randint(1, 100):03d}", f"MED{random.randint(1, 100):03d}",
                provider_id, fake.uuid4(), fake.text(max_nb_chars=100)
            ])


# Generate Claims Data
def generate_claims_data(patient_ids, provider_ids):
    filename = f"claims_data_{timestamp}.csv"
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "claim_id", "patient_id", "service_date", "claim_date", "claim_amount", "claim_status",
            "provider_id", "diagnosis_codes", "procedure_codes"
        ])
        for _ in range(NUM_RECORDS):
            patient_id = random.choice(list(patient_ids))
            provider_id = random.choice(list(provider_ids))
            writer.writerow([
                fake.uuid4(), patient_id, fake.date_this_year().strftime('%Y-%m-%d'), fake.date_this_year().strftime('%Y-%m-%d'),
                round(random.uniform(100, 10000), 2), random.choice(["Approved", "Pending", "Rejected"]),
                provider_id, [f"DX{random.randint(1, 100):03d}" for _ in range(3)],
                [f"PR{random.randint(1, 100):03d}" for _ in range(3)]
            ])


# Generate Pharmacy Data
def generate_pharmacy_data(patient_ids):
    filename = f"pharmacy_data_{timestamp}.csv"
    medication_codes = [f"MED{str(i).zfill(3)}" for i in range(1, 101)]
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "prescription_id", "patient_id", "medication_code", "fill_date", "days_supply", "quantity", "pharmacy_id"
        ])
        for _ in range(NUM_RECORDS):
            patient_id = random.choice(list(patient_ids))
            writer.writerow([
                fake.uuid4(), patient_id, random.choice(medication_codes),
                fake.date_this_year().strftime('%Y-%m-%d'), random.randint(1, 90), round(random.uniform(1, 30), 2), fake.uuid4()
            ])


# Generate Provider Data
def generate_provider_data():
    filename = f"provider_data_{timestamp}.csv"
    specialties = ["Cardiology", "Neurology", "Orthopedics", "Pediatrics", "General"]
    provider_ids = set()
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "provider_id", "provider_name", "specialty", "npi_number", "facility_id"
        ])
        for _ in range(NUM_RECORDS):
            provider_id = fake.uuid4()
            provider_ids.add(provider_id)
            writer.writerow([
                provider_id, fake.name(), random.choice(specialties), fake.uuid4(), fake.uuid4()
            ])
    return provider_ids


# Main function to generate all data
def main():
    print("Generating data...")
    patient_ids, primary_care_provider_ids = generate_patient_data()
    provider_ids = generate_provider_data()
    generate_medical_events(patient_ids, provider_ids)
    generate_claims_data(patient_ids, provider_ids)
    generate_pharmacy_data(patient_ids)
    print("Data generation complete!")


if __name__ == "__main__":
    main()
