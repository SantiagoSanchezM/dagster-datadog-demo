# Seed data

Fabricated, deterministic CSVs loaded by `raw_customers` and `raw_orders` so records actually
flow through the pipeline end to end (rather than being regenerated randomly on every run).

- `customers.csv` - 500 fabricated customers (`customer_id`, `customer_name`, `region`, `signup_date`).
- `orders.csv` - 5,025 fabricated orders (`order_id`, `customer_id`, `order_amount`, `order_date`),
  deliberately messy:
  - 25 duplicate rows (upstream dupes)
  - ~2% negative `order_amount` (bad data)
  - `customer_id` values up to 520, while `customers.csv` only goes up to 500 (orphan foreign keys)

This messiness is what `cleaned_orders` dedupes/filters and what the `no_orphan_customers` /
`non_negative_revenue` asset checks catch (or don't).

Regenerate with the same seed (`Faker.seed(42)` / `random.seed(42)`) via:

```
docker run --rm -v "$(pwd)/demo_pipeline/seed_data:/out" demo-pipeline:latest python3 -c "
import random
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

REGIONS = ['NA', 'EMEA', 'APAC', 'LATAM']

customers = [
    {
        'customer_id': i,
        'customer_name': fake.name(),
        'region': random.choice(REGIONS),
        'signup_date': fake.date_between(start_date='-2y', end_date='today'),
    }
    for i in range(1, 501)
]
pd.DataFrame(customers).to_csv('/out/customers.csv', index=False)

orders = []
for i in range(1, 5001):
    amount = round(random.uniform(5, 500), 2)
    if random.random() < 0.02:
        amount = -amount
    orders.append({
        'order_id': i,
        'customer_id': random.randint(1, 520),
        'order_amount': amount,
        'order_date': fake.date_between(start_date='-1y', end_date='today'),
    })
orders += random.sample(orders, 25)
pd.DataFrame(orders).to_csv('/out/orders.csv', index=False)
"
```
