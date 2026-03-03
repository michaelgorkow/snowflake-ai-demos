import pandas as pd
import numpy as np
from datetime import date, timedelta


WAREHOUSE_LOCATIONS = {
    'EMEA': [
        ('Frankfurt', 'Germany'), ('Rotterdam', 'Netherlands'), ('London', 'UK'),
        ('Paris', 'France'), ('Milan', 'Italy'), ('Barcelona', 'Spain'),
        ('Warsaw', 'Poland'), ('Prague', 'Czech Republic'), ('Stockholm', 'Sweden'),
        ('Dubai', 'UAE'),
    ],
    'APAC': [
        ('Shanghai', 'China'), ('Tokyo', 'Japan'), ('Singapore', 'Singapore'),
        ('Sydney', 'Australia'), ('Mumbai', 'India'), ('Seoul', 'South Korea'),
        ('Bangkok', 'Thailand'), ('Kuala Lumpur', 'Malaysia'),
    ],
    'Americas': [
        ('Chicago', 'USA'), ('Los Angeles', 'USA'), ('Houston', 'USA'),
        ('Toronto', 'Canada'), ('Mexico City', 'Mexico'), ('Sao Paulo', 'Brazil'),
        ('Miami', 'USA'),
    ],
}

WAREHOUSE_TYPES = ['Ambient', 'Cold Storage', 'Hazmat']

TYPE_BASELINES = {
    'Ambient': (0.50, 0.70),
    'Cold Storage': (0.70, 0.85),
    'Hazmat': (0.40, 0.60),
}


FORECAST_HORIZON_DAYS = 90
DRIFT_GRACE_DAYS = 30


def setup(session, schema='WAREHOUSE_OPTIMIZATION'):
    database = 'AI_DEMOS'
    session.sql(f'CREATE SCHEMA IF NOT EXISTS {database}.{schema}').collect()
    session.sql(f'CREATE SCHEMA IF NOT EXISTS {database}.{schema}_FEATURE_STORE').collect()
    session.sql(f'CREATE SCHEMA IF NOT EXISTS {database}.{schema}_MODEL_REGISTRY').collect()
    session.use_schema(schema)

    today = date.today()
    start_date = (today - timedelta(days=730)).isoformat()
    end_date = (today - timedelta(days=FORECAST_HORIZON_DAYS)).isoformat()
    generate_warehouse_data(session, schema, n_warehouses=25, start_date=start_date, end_date=end_date)


def generate_warehouse_data(session, schema, n_warehouses=25, start_date=None,
                            end_date=None, mode='overwrite'):
    database = 'AI_DEMOS'
    rng = np.random.RandomState(42 if mode == 'overwrite' else 99)

    today = date.today()
    if start_date is None:
        start_date = (today - timedelta(days=730)).isoformat()
    if end_date is None:
        end_date = (today - timedelta(days=FORECAST_HORIZON_DAYS)).isoformat()

    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)

    if mode == 'append':
        existing_wh = session.table(f'{database}.{schema}.WAREHOUSES').to_pandas()
        warehouses_df = existing_wh
        existing_attrs = session.table(f'{database}.{schema}.WAREHOUSE_ATTRIBUTES').to_pandas()
        n_warehouses = len(warehouses_df)
    else:
        warehouses_df = _generate_warehouses(n_warehouses, rng, start_dt)
        existing_attrs = None

    if mode != 'append':
        attributes_df = _generate_attributes(warehouses_df, rng, start_dt, end_dt)
    else:
        attributes_df = existing_attrs

    is_drift = (mode == 'append')
    operations_df = _generate_operations(
        warehouses_df, rng, start_dt, end_dt, drift=is_drift
    )

    if mode == 'overwrite':
        session.write_pandas(warehouses_df, 'WAREHOUSES', database=database, schema=schema,
                             auto_create_table=True, overwrite=True)
        session.write_pandas(attributes_df, 'WAREHOUSE_ATTRIBUTES', database=database, schema=schema,
                             auto_create_table=True, overwrite=True)
        session.write_pandas(operations_df, 'WAREHOUSE_OPERATIONS', database=database, schema=schema,
                             auto_create_table=True, overwrite=True)
    elif mode == 'append':
        session.write_pandas(operations_df, 'WAREHOUSE_OPERATIONS', database=database, schema=schema,
                             auto_create_table=True, overwrite=False)

    return warehouses_df, attributes_df, operations_df


def _generate_warehouses(n_warehouses, rng, start_dt):
    regions = []
    cities = []
    countries = []
    wh_types = []
    names = []
    ids = []
    opening_dates = []

    region_counts = {'EMEA': 10, 'APAC': 8, 'Americas': 7}
    idx = 0
    for region, count in region_counts.items():
        locs = WAREHOUSE_LOCATIONS[region]
        for i in range(count):
            idx += 1
            loc = locs[i % len(locs)]
            ids.append(f'WH_{idx:03d}')
            names.append(f'{loc[0]} Warehouse {idx}')
            cities.append(loc[0])
            countries.append(loc[1])
            regions.append(region)
            wh_type = WAREHOUSE_TYPES[rng.randint(0, len(WAREHOUSE_TYPES))]
            wh_types.append(wh_type)
            days_before = rng.randint(0, 365)
            opening_dates.append((start_dt - pd.Timedelta(days=days_before + 365)).date())

    return pd.DataFrame({
        'WAREHOUSE_ID': ids,
        'WAREHOUSE_NAME': names,
        'CITY': cities,
        'COUNTRY': countries,
        'REGION': regions,
        'WAREHOUSE_TYPE': wh_types,
        'OPENING_DATE': opening_dates,
    })


def _generate_attributes(warehouses_df, rng, start_dt, end_dt):
    rows = []
    total_days = (end_dt - start_dt).days

    for _, wh in warehouses_df.iterrows():
        wh_id = wh['WAREHOUSE_ID']
        wh_type = wh['WAREHOUSE_TYPE']

        if wh_type == 'Cold Storage':
            base_cap = rng.randint(5000, 15000)
            base_docks = rng.randint(5, 12)
            base_staff = rng.randint(30, 80)
        elif wh_type == 'Hazmat':
            base_cap = rng.randint(3000, 10000)
            base_docks = rng.randint(5, 10)
            base_staff = rng.randint(20, 60)
        else:
            base_cap = rng.randint(10000, 50000)
            base_docks = rng.randint(8, 30)
            base_staff = rng.randint(40, 200)

        rows.append({
            'WAREHOUSE_ID': wh_id,
            'EFFECTIVE_DATE': start_dt.date(),
            'TOTAL_CAPACITY_SQM': int(base_cap),
            'NUM_LOADING_DOCKS': int(base_docks),
            'STAFFING_LEVEL': int(base_staff),
        })

        n_changes = rng.randint(1, 4)
        change_days = sorted(rng.choice(range(60, total_days - 30), size=n_changes, replace=False))

        cap = base_cap
        docks = base_docks
        staff = base_staff
        for cd in change_days:
            change_type = rng.choice(['capacity', 'docks', 'staffing', 'mixed'])
            if change_type == 'capacity' or change_type == 'mixed':
                cap = int(cap * (1 + rng.uniform(0.10, 0.30)))
            if change_type == 'docks' or change_type == 'mixed':
                docks = int(docks + rng.randint(2, 6))
            if change_type == 'staffing' or change_type == 'mixed':
                staff = int(staff * (1 + rng.uniform(-0.20, 0.20)))
                staff = max(staff, 10)

            rows.append({
                'WAREHOUSE_ID': wh_id,
                'EFFECTIVE_DATE': (start_dt + pd.Timedelta(days=int(cd))).date(),
                'TOTAL_CAPACITY_SQM': int(cap),
                'NUM_LOADING_DOCKS': int(docks),
                'STAFFING_LEVEL': int(staff),
            })

    return pd.DataFrame(rows)


def _generate_operations(warehouses_df, rng, start_dt, end_dt, drift=False):
    dates = pd.date_range(start=start_dt, end=end_dt, freq='D')
    all_rows = []

    n_days = len(dates)
    t = np.arange(n_days, dtype=float)
    day_of_week = np.array([d.dayofweek for d in dates])
    day_of_year = np.array([d.dayofyear for d in dates])
    drift_ramp = np.clip(t / DRIFT_GRACE_DAYS, 0, 1) if drift else np.zeros(n_days)

    for _, wh in warehouses_df.iterrows():
        wh_id = wh['WAREHOUSE_ID']
        wh_type = wh['WAREHOUSE_TYPE']
        region = wh['REGION']

        base_inbound = rng.uniform(30, 70)
        inbound = (
            base_inbound
            + t / 365.0 * rng.uniform(2, 5)
            + np.where(day_of_week >= 5, -8, 4)
            + 6 * np.sin(2 * np.pi * (day_of_year - 60) / 365.0)
            + rng.normal(0, 2, n_days)
        )
        if drift:
            inbound += drift_ramp * rng.uniform(25, 50)
        inbound = np.clip(inbound, 5, 250).astype(int)

        base_outbound = rng.uniform(30, 70)
        outbound = (
            base_outbound
            + t / 365.0 * rng.uniform(2, 5)
            + np.where(day_of_week >= 5, -6, 3)
            + 5 * np.sin(2 * np.pi * (day_of_year - 90) / 365.0)
            + rng.normal(0, 2, n_days)
        )
        if drift:
            outbound -= drift_ramp * rng.uniform(15, 30)
        outbound = np.clip(outbound, 5, 250).astype(int)

        temp = np.array([_seasonal_temp(d, region, rng) for d in dates])
        if drift:
            temp += drift_ramp * rng.uniform(4, 8)

        base_clients = rng.randint(10, 40)
        clients = (
            base_clients
            + t / 365.0 * rng.uniform(1, 3)
            + rng.normal(0, 1, n_days)
        )
        if drift:
            clients += drift_ramp * rng.uniform(15, 30)
        clients = np.clip(clients, 3, 120).astype(int)

        baseline_low, baseline_high = TYPE_BASELINES.get(wh_type, (0.50, 0.70))
        base_util = rng.uniform(baseline_low, baseline_high)

        w_inbound = rng.uniform(0.002, 0.004)
        w_outbound = rng.uniform(-0.003, -0.001)
        w_temp = rng.uniform(0.0005, 0.0015)
        w_clients = rng.uniform(0.002, 0.005)

        utilization_raw = (
            base_util
            + w_inbound * (inbound - base_inbound)
            + w_outbound * (outbound - base_outbound)
            + w_temp * (temp - temp[:30].mean())
            + w_clients * (clients - base_clients)
            + np.where(day_of_week >= 5, -0.03, 0.01)
            + rng.normal(0, 0.015, n_days)
        )

        if not drift:
            n_anomalies = rng.randint(3, 6)
            for _ in range(n_anomalies):
                anomaly_start = rng.randint(0, max(1, n_days - 20))
                anomaly_duration = rng.randint(5, 21)
                anomaly_magnitude = rng.uniform(0.10, 0.15)
                anomaly_end = min(anomaly_start + anomaly_duration, n_days)
                utilization_raw[anomaly_start:anomaly_end] += anomaly_magnitude

        utilization_raw = np.clip(utilization_raw, 0.10, 0.98)

        for i, d in enumerate(dates):
            d_date = d.date()

            all_rows.append({
                'WAREHOUSE_ID': wh_id,
                'DATE': d_date,
                'UTILIZATION_PCT': round(float(utilization_raw[i]), 4),
                'INBOUND_SHIPMENTS': int(inbound[i]),
                'OUTBOUND_SHIPMENTS': int(outbound[i]),
                'NUM_ACTIVE_CLIENTS': int(clients[i]),
                'TEMPERATURE_C': round(float(temp[i]), 1),
            })

    return pd.DataFrame(all_rows)


def _seasonal_temp(d, region, rng):
    doy = d.dayofyear
    if region == 'EMEA':
        base = 10 + 15 * np.sin(2 * np.pi * (doy - 80) / 365.0)
    elif region == 'APAC':
        base = 22 + 8 * np.sin(2 * np.pi * (doy - 80) / 365.0)
    else:
        base = 15 + 12 * np.sin(2 * np.pi * (doy - 80) / 365.0)
    return base + rng.normal(0, 2)

