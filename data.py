import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()
import os

fake = Faker()

# ── Mappings ───────────────────────────────────────────────────────
# Weights match bulk_rides.json distribution for consistent KPIs

VEHICLE_TYPE_MAPPING = [
    {'vehicle_type_id': 1, 'vehicle_type': 'UberX',        'description': 'Standard',    'base_rate': 1.50, 'per_mile': 1.20, 'per_minute': 0.22, 'weight': 45},
    {'vehicle_type_id': 2, 'vehicle_type': 'UberXL',       'description': 'Extra Large', 'base_rate': 2.50, 'per_mile': 1.75, 'per_minute': 0.32, 'weight': 20},
    {'vehicle_type_id': 3, 'vehicle_type': 'UberPOOL',     'description': 'Shared Ride', 'base_rate': 1.00, 'per_mile': 0.90, 'per_minute': 0.18, 'weight': 20},
    {'vehicle_type_id': 4, 'vehicle_type': 'Uber Comfort', 'description': 'Comfortable', 'base_rate': 2.00, 'per_mile': 1.50, 'per_minute': 0.28, 'weight': 10},
    {'vehicle_type_id': 5, 'vehicle_type': 'Uber Black',   'description': 'Premium',     'base_rate': 5.00, 'per_mile': 3.50, 'per_minute': 0.55, 'weight': 5},
]

PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'Credit Card',    'is_card': True,  'requires_auth': True,  'weight': 38},
    {'payment_method_id': 2, 'payment_method': 'Debit Card',     'is_card': True,  'requires_auth': True,  'weight': 28},
    {'payment_method_id': 3, 'payment_method': 'Digital Wallet', 'is_card': False, 'requires_auth': False, 'weight': 25},
    {'payment_method_id': 4, 'payment_method': 'Cash',           'is_card': False, 'requires_auth': False, 'weight': 9},
]

RIDE_STATUS_MAPPING = [
    {'ride_status_id': 1, 'ride_status': 'Completed', 'is_completed': True},
    {'ride_status_id': 2, 'ride_status': 'Cancelled',  'is_completed': False},
]

VEHICLE_MAKE_MAPPING = [
    {'vehicle_make_id': 1, 'vehicle_make': 'Toyota',    'weight': 28},
    {'vehicle_make_id': 2, 'vehicle_make': 'Honda',     'weight': 22},
    {'vehicle_make_id': 3, 'vehicle_make': 'Ford',      'weight': 18},
    {'vehicle_make_id': 4, 'vehicle_make': 'Chevrolet', 'weight': 14},
    {'vehicle_make_id': 5, 'vehicle_make': 'Nissan',    'weight': 10},
    {'vehicle_make_id': 6, 'vehicle_make': 'BMW',       'weight': 5},
    {'vehicle_make_id': 7, 'vehicle_make': 'Mercedes',  'weight': 3},
]

CITY_MAPPING = [
    {'city_id': 1,  'city': 'New York',       'state': 'NY', 'region': 'Northeast',  'weight': 12},
    {'city_id': 2,  'city': 'Los Angeles',    'state': 'CA', 'region': 'West',        'weight': 10},
    {'city_id': 3,  'city': 'Chicago',        'state': 'IL', 'region': 'Midwest',     'weight': 8},
    {'city_id': 4,  'city': 'Houston',        'state': 'TX', 'region': 'South',       'weight': 7},
    {'city_id': 5,  'city': 'Phoenix',        'state': 'AZ', 'region': 'Southwest',   'weight': 5},
    {'city_id': 6,  'city': 'Philadelphia',   'state': 'PA', 'region': 'Northeast',   'weight': 5},
    {'city_id': 7,  'city': 'San Antonio',    'state': 'TX', 'region': 'South',       'weight': 4},
    {'city_id': 8,  'city': 'San Diego',      'state': 'CA', 'region': 'West',        'weight': 4},
    {'city_id': 9,  'city': 'Dallas',         'state': 'TX', 'region': 'South',       'weight': 6},
    {'city_id': 10, 'city': 'San Jose',       'state': 'CA', 'region': 'West',        'weight': 4},
    {'city_id': 11, 'city': 'Birmingham',     'state': 'AL', 'region': 'South',       'weight': 2},
    {'city_id': 12, 'city': 'Anchorage',      'state': 'AK', 'region': 'West',        'weight': 1},
    {'city_id': 13, 'city': 'Little Rock',    'state': 'AR', 'region': 'South',       'weight': 1},
    {'city_id': 14, 'city': 'Denver',         'state': 'CO', 'region': 'West',        'weight': 4},
    {'city_id': 15, 'city': 'Hartford',       'state': 'CT', 'region': 'Northeast',   'weight': 2},
    {'city_id': 16, 'city': 'Wilmington',     'state': 'DE', 'region': 'Northeast',   'weight': 1},
    {'city_id': 17, 'city': 'Washington DC',  'state': 'DC', 'region': 'Northeast',   'weight': 5},
    {'city_id': 18, 'city': 'Jacksonville',   'state': 'FL', 'region': 'South',       'weight': 3},
    {'city_id': 19, 'city': 'Atlanta',        'state': 'GA', 'region': 'South',       'weight': 5},
    {'city_id': 20, 'city': 'Honolulu',       'state': 'HI', 'region': 'West',        'weight': 2},
    {'city_id': 21, 'city': 'Boise',          'state': 'ID', 'region': 'West',        'weight': 1},
    {'city_id': 22, 'city': 'Indianapolis',   'state': 'IN', 'region': 'Midwest',     'weight': 2},
    {'city_id': 23, 'city': 'Des Moines',     'state': 'IA', 'region': 'Midwest',     'weight': 1},
    {'city_id': 24, 'city': 'Wichita',        'state': 'KS', 'region': 'Midwest',     'weight': 1},
    {'city_id': 25, 'city': 'Louisville',     'state': 'KY', 'region': 'South',       'weight': 2},
    {'city_id': 26, 'city': 'New Orleans',    'state': 'LA', 'region': 'South',       'weight': 3},
    {'city_id': 27, 'city': 'Portland',       'state': 'ME', 'region': 'Northeast',   'weight': 1},
    {'city_id': 28, 'city': 'Baltimore',      'state': 'MD', 'region': 'Northeast',   'weight': 3},
    {'city_id': 29, 'city': 'Boston',         'state': 'MA', 'region': 'Northeast',   'weight': 5},
    {'city_id': 30, 'city': 'Detroit',        'state': 'MI', 'region': 'Midwest',     'weight': 3},
    {'city_id': 31, 'city': 'Minneapolis',    'state': 'MN', 'region': 'Midwest',     'weight': 3},
    {'city_id': 32, 'city': 'Jackson',        'state': 'MS', 'region': 'South',       'weight': 1},
    {'city_id': 33, 'city': 'Kansas City',    'state': 'MO', 'region': 'Midwest',     'weight': 2},
    {'city_id': 34, 'city': 'Billings',       'state': 'MT', 'region': 'West',        'weight': 1},
    {'city_id': 35, 'city': 'Omaha',          'state': 'NE', 'region': 'Midwest',     'weight': 1},
    {'city_id': 36, 'city': 'Las Vegas',      'state': 'NV', 'region': 'West',        'weight': 4},
    {'city_id': 37, 'city': 'Manchester',     'state': 'NH', 'region': 'Northeast',   'weight': 1},
    {'city_id': 38, 'city': 'Newark',         'state': 'NJ', 'region': 'Northeast',   'weight': 3},
    {'city_id': 39, 'city': 'Albuquerque',    'state': 'NM', 'region': 'Southwest',   'weight': 2},
    {'city_id': 40, 'city': 'Buffalo',        'state': 'NY', 'region': 'Northeast',   'weight': 2},
    {'city_id': 41, 'city': 'Charlotte',      'state': 'NC', 'region': 'South',       'weight': 3},
    {'city_id': 42, 'city': 'Fargo',          'state': 'ND', 'region': 'Midwest',     'weight': 1},
    {'city_id': 43, 'city': 'Columbus',       'state': 'OH', 'region': 'Midwest',     'weight': 3},
    {'city_id': 44, 'city': 'Oklahoma City',  'state': 'OK', 'region': 'South',       'weight': 2},
    {'city_id': 45, 'city': 'Portland',       'state': 'OR', 'region': 'West',        'weight': 3},
    {'city_id': 46, 'city': 'Pittsburgh',     'state': 'PA', 'region': 'Northeast',   'weight': 2},
    {'city_id': 47, 'city': 'Providence',     'state': 'RI', 'region': 'Northeast',   'weight': 1},
    {'city_id': 48, 'city': 'Columbia',       'state': 'SC', 'region': 'South',       'weight': 1},
    {'city_id': 49, 'city': 'Sioux Falls',    'state': 'SD', 'region': 'Midwest',     'weight': 1},
    {'city_id': 50, 'city': 'Nashville',      'state': 'TN', 'region': 'South',       'weight': 3},
    {'city_id': 51, 'city': 'Salt Lake City', 'state': 'UT', 'region': 'West',        'weight': 2},
    {'city_id': 52, 'city': 'Burlington',     'state': 'VT', 'region': 'Northeast',   'weight': 1},
    {'city_id': 53, 'city': 'Virginia Beach', 'state': 'VA', 'region': 'South',       'weight': 2},
    {'city_id': 54, 'city': 'Seattle',        'state': 'WA', 'region': 'West',        'weight': 5},
    {'city_id': 55, 'city': 'Charleston',     'state': 'WV', 'region': 'South',       'weight': 1},
    {'city_id': 56, 'city': 'Milwaukee',      'state': 'WI', 'region': 'Midwest',     'weight': 2},
    {'city_id': 57, 'city': 'Cheyenne',       'state': 'WY', 'region': 'West',        'weight': 1},
]

# Real bounding boxes (lat_min, lat_max, lon_min, lon_max)
CITY_BBOX = {
    1:  (40.4774, 40.9176, -74.2591, -73.7004),
    2:  (33.7037, 34.3373, -118.6682, -118.1553),
    3:  (41.6445, 42.0230, -87.9401, -87.5240),
    4:  (29.5239, 30.1108, -95.7835, -95.0146),
    5:  (33.2148, 33.9187, -112.3241, -111.9253),
    6:  (39.8670, 40.1379, -75.2803, -74.9558),
    7:  (29.2098, 29.6659, -98.8088, -98.2340),
    8:  (32.5343, 33.1139, -117.2817, -116.9057),
    9:  (32.6177, 33.0237, -97.0000, -96.5546),
    10: (37.1255, 37.4697, -122.0436, -121.5886),
    11: (33.4018, 33.6012, -86.9958, -86.6360),
    12: (61.0767, 61.3717, -150.1626, -149.5561),
    13: (34.6243, 34.8432, -92.4899, -92.1722),
    14: (39.6143, 39.9142, -105.1099, -104.5997),
    15: (41.7137, 41.8137, -72.7331, -72.6331),
    16: (39.6846, 39.7846, -75.5993, -75.4993),
    17: (38.7916, 38.9958, -77.1198, -76.9094),
    18: (30.1029, 30.5850, -82.0551, -81.3971),
    19: (33.6100, 33.8886, -84.5516, -84.2898),
    20: (21.2541, 21.3941, -157.9541, -157.8141),
    21: (43.5318, 43.7318, -116.3284, -116.1284),
    22: (39.6326, 39.9271, -86.3282, -85.9503),
    23: (41.5308, 41.6508, -93.6908, -93.5308),
    24: (37.6176, 37.7776, -97.4136, -97.2136),
    25: (38.0674, 38.3785, -85.9437, -85.4884),
    26: (29.8758, 30.0758, -90.1400, -89.9400),
    27: (43.6181, 43.7181, -70.3289, -70.2289),
    28: (39.1972, 39.3722, -76.7112, -76.5290),
    29: (42.2279, 42.3979, -71.1912, -70.9912),
    30: (42.2550, 42.4550, -83.2879, -82.9879),
    31: (44.8896, 45.0512, -93.3290, -93.1938),
    32: (32.2241, 32.3841, -90.2641, -90.1041),
    33: (38.8779, 39.3067, -94.7541, -94.3644),
    34: (45.7033, 45.8433, -108.5901, -108.4301),
    35: (41.1660, 41.3999, -96.2400, -95.8677),
    36: (36.0756, 36.3849, -115.3816, -115.0625),
    37: (42.9406, 43.0406, -71.5006, -71.4006),
    38: (40.6901, 40.7901, -74.2201, -74.1201),
    39: (34.9479, 35.2181, -106.8813, -106.4683),
    40: (42.8264, 42.9664, -78.9264, -78.7664),
    41: (35.0175, 35.3723, -81.0090, -80.6512),
    42: (46.8322, 46.9322, -96.8896, -96.7296),
    43: (39.8620, 40.1563, -83.2008, -82.7713),
    44: (35.3378, 35.6495, -97.7696, -97.2673),
    45: (45.4325, 45.6524, -122.8367, -122.4718),
    46: (40.3613, 40.5013, -80.0952, -79.9352),
    47: (41.7801, 41.8601, -71.4701, -71.3701),
    48: (33.9801, 34.1001, -81.1101, -80.9501),
    49: (43.4800, 43.6200, -96.8200, -96.6600),
    50: (35.9946, 36.4066, -87.0599, -86.5159),
    51: (40.6900, 40.8100, -111.9400, -111.8200),
    52: (44.4459, 44.5059, -73.2459, -73.1859),
    53: (36.6801, 36.9201, -76.1001, -75.9001),
    54: (47.4919, 47.7341, -122.4596, -122.2244),
    55: (38.2701, 38.4101, -81.7401, -81.5801),
    56: (42.9215, 43.1928, -88.0706, -87.8467),
    57: (41.0800, 41.2000, -104.8700, -104.7700),
}

CANCELLATION_REASON_MAPPING = [
    {'cancellation_reason_id': 1, 'cancellation_reason': 'Driver cancelled'},
    {'cancellation_reason_id': 2, 'cancellation_reason': 'Passenger cancelled'},
    {'cancellation_reason_id': 3, 'cancellation_reason': 'No show'},
    {'cancellation_reason_id': 4, 'cancellation_reason': None},
]

# ── Lookup helpers ─────────────────────────────────────────────────
VEHICLE_MAKES_LIST    = [m['vehicle_make']    for m in VEHICLE_MAKE_MAPPING]
VEHICLE_MAKE_WEIGHTS  = [m['weight']          for m in VEHICLE_MAKE_MAPPING]
VEHICLE_MAKE_ID_MAP   = {m['vehicle_make']:   m['vehicle_make_id']   for m in VEHICLE_MAKE_MAPPING}

VEHICLE_TYPES_LIST    = [t['vehicle_type']    for t in VEHICLE_TYPE_MAPPING]
VEHICLE_TYPE_WEIGHTS  = [t['weight']          for t in VEHICLE_TYPE_MAPPING]
VEHICLE_TYPE_ID_MAP   = {t['vehicle_type']:   t['vehicle_type_id']   for t in VEHICLE_TYPE_MAPPING}

PAYMENT_METHODS_LIST  = [p['payment_method']  for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_WEIGHTS= [p['weight']          for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p['payment_method']: p['payment_method_id'] for p in PAYMENT_METHOD_MAPPING}

RIDE_STATUS_ID_MAP    = {s['ride_status']:    s['ride_status_id']    for s in RIDE_STATUS_MAPPING}

CITY_LIST             = [c['city_id']         for c in CITY_MAPPING]
CITY_WEIGHTS          = [c['weight']          for c in CITY_MAPPING]
CITY_ID_MAP           = {c['city_id']:        c['city']              for c in CITY_MAPPING}

CANCELLATION_REASON_ID_MAP = {c['cancellation_reason']: c['cancellation_reason_id'] for c in CANCELLATION_REASON_MAPPING}

VEHICLE_MODELS = ['Camry', 'Accord', 'Civic', 'Corolla', 'Altima', 'Fusion', 'Malibu', 'Prius', 'Sonata', 'Elantra']


def _coords_for_city(city_id: int) -> tuple[float, float]:
    """Return a random (latitude, longitude) within the city's bounding box."""
    bb = CITY_BBOX[city_id]
    return round(random.uniform(bb[0], bb[1]), 6), round(random.uniform(bb[2], bb[3]), 6)


def generate_uber_ride_confirmation():

    # ── Timestamps ────────────────────────────────────────────────
    pickup_time = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    # ── Realistic distance — city-weighted short trips ─────────────
    distance = round(random.choices(
        [random.uniform(0.5, 2),
         random.uniform(2, 5),
         random.uniform(5, 10),
         random.uniform(10, 20),
         random.uniform(20, 40)],
        weights=[25, 40, 22, 10, 3],
        k=1
    )[0], 2)

    # Duration proportional to distance (city driving ~10-20 mph)
    speed_mph = random.uniform(10, 20)
    duration_minutes = max(4, int((distance / speed_mph) * 60 + random.randint(2, 8)))
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)
    booking_time = pickup_time - timedelta(minutes=random.randint(1, 5))

    # ── Realistic surge — mostly low, rare spikes ──────────────────
    surge_multiplier = round(random.choices(
        [random.uniform(1.0, 1.1),
         random.uniform(1.1, 1.3),
         random.uniform(1.3, 1.6),
         random.uniform(1.6, 2.0),
         random.uniform(2.0, 2.5)],
        weights=[50, 28, 13, 6, 3],
        k=1
    )[0], 2)

    # ── Vehicle type — weighted, UberX most common ─────────────────
    vehicle_type = random.choices(VEHICLE_TYPES_LIST, weights=VEHICLE_TYPE_WEIGHTS, k=1)[0]
    vt_data      = next(v for v in VEHICLE_TYPE_MAPPING if v['vehicle_type'] == vehicle_type)

    distance_fare = round(distance * vt_data['per_mile'], 2)
    time_fare     = round(duration_minutes * vt_data['per_minute'], 2)
    subtotal      = round((distance_fare + time_fare + vt_data['base_rate']) * surge_multiplier, 2)

    # ── Realistic tip — most rides no tip ─────────────────────────
    tip = round(random.choices(
        [0,
         random.uniform(1, 3),
         random.uniform(3, 6),
         random.uniform(6, 12)],
        weights=[50, 28, 15, 7],
        k=1
    )[0], 2)
    total_fare = round(subtotal + tip, 2)

    # ── City — weighted by demand ──────────────────────────────────
    pickup_city_id  = random.choices(CITY_LIST, weights=CITY_WEIGHTS, k=1)[0]
    dropoff_city_id = random.choices(CITY_LIST, weights=CITY_WEIGHTS, k=1)[0]
    pickup_latitude,  pickup_longitude  = _coords_for_city(pickup_city_id)
    dropoff_latitude, dropoff_longitude = _coords_for_city(dropoff_city_id)

    # ── Vehicle make — Toyota/Honda most common ────────────────────
    vehicle_make   = random.choices(VEHICLE_MAKES_LIST, weights=VEHICLE_MAKE_WEIGHTS, k=1)[0]

    # ── Payment — credit card most common ─────────────────────────
    payment_method = random.choices(PAYMENT_METHODS_LIST, weights=PAYMENT_METHOD_WEIGHTS, k=1)[0]

    # ── Cancellation — realistic ~7% rate ─────────────────────────
    is_cancelled   = random.random() < 0.07
    ride_status    = 'Cancelled' if is_cancelled else 'Completed'
    cancel_reason  = random.choice(['Driver cancelled', 'Passenger cancelled', 'No show']) if is_cancelled else None

    # ── Rating — skewed high (4.5-5.0 most common) ────────────────
    rating = None if is_cancelled else round(random.choices(
        [random.uniform(1.0, 2.9),
         random.uniform(3.0, 3.9),
         random.uniform(4.0, 4.4),
         random.uniform(4.5, 4.9),
         5.0],
        weights=[2, 5, 18, 45, 30],
        k=1
    )[0], 1)

    return {
        # Identifiers
        'ride_id':               str(uuid.uuid4()),
        'confirmation_number':   fake.bothify('??#-####-??##'),
        'passenger_id':          str(uuid.uuid4()),
        'driver_id':             str(uuid.uuid4()),
        'vehicle_id':            str(uuid.uuid4()),
        'pickup_location_id':    str(uuid.uuid4()),
        'dropoff_location_id':   str(uuid.uuid4()),
        # Foreign keys
        'vehicle_type_id':          VEHICLE_TYPE_ID_MAP[vehicle_type],
        'vehicle_make_id':          VEHICLE_MAKE_ID_MAP[vehicle_make],
        'payment_method_id':        PAYMENT_METHOD_ID_MAP[payment_method],
        'ride_status_id':           RIDE_STATUS_ID_MAP[ride_status],
        'pickup_city_id':           pickup_city_id,
        'dropoff_city_id':          dropoff_city_id,
        'cancellation_reason_id':   CANCELLATION_REASON_ID_MAP[cancel_reason],
        # Passenger
        'passenger_name':   fake.name(),
        'passenger_email':  fake.email(),
        'passenger_phone':  fake.phone_number(),
        # Driver
        'driver_name':      fake.name(),
        'driver_rating':    round(random.uniform(4.2, 5.0), 2),
        'driver_phone':     fake.phone_number(),
        'driver_license':   fake.bothify('??-???-#######'),
        # Vehicle
        'vehicle_model':    random.choice(VEHICLE_MODELS),
        'vehicle_color':    random.choice(['Black', 'White', 'Gray', 'Silver', 'Blue', 'Red']),
        'license_plate':    fake.bothify('???-####'),
        # Locations
        'pickup_address':    fake.address().replace('\n', ', '),
        'pickup_latitude':   pickup_latitude,
        'pickup_longitude':  pickup_longitude,
        'dropoff_address':   fake.address().replace('\n', ', '),
        'dropoff_latitude':  dropoff_latitude,
        'dropoff_longitude': dropoff_longitude,
        # Measures
        'distance_miles':    distance,
        'duration_minutes':  duration_minutes,
        'booking_timestamp': booking_time.isoformat(),
        'pickup_timestamp':  pickup_time.isoformat(),
        'dropoff_timestamp': dropoff_time.isoformat(),
        'base_fare':         vt_data['base_rate'],
        'distance_fare':     distance_fare,
        'time_fare':         time_fare,
        'surge_multiplier':  surge_multiplier,
        'subtotal':          subtotal,
        'tip_amount':        tip,
        'total_fare':        total_fare,
        'rating':            rating,
    }