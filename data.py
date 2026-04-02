import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os

fake = Faker()


VEHICLE_TYPE_MAPPING = [
    {'vehicle_type_id': 1, 'vehicle_type': 'UberX', 'description': 'Standard', 'base_rate': 2.50, 'per_mile': 1.75, 'per_minute': 0.35},
    {'vehicle_type_id': 2, 'vehicle_type': 'UberXL', 'description': 'Extra Large', 'base_rate': 3.50, 'per_mile': 2.25, 'per_minute': 0.45},
    {'vehicle_type_id': 3, 'vehicle_type': 'UberPOOL', 'description': 'Shared Ride', 'base_rate': 2.00, 'per_mile': 1.50, 'per_minute': 0.30},
    {'vehicle_type_id': 4, 'vehicle_type': 'Uber Comfort', 'description': 'Comfortable', 'base_rate': 3.00, 'per_mile': 2.00, 'per_minute': 0.40},
    {'vehicle_type_id': 5, 'vehicle_type': 'Uber Black', 'description': 'Premium', 'base_rate': 5.00, 'per_mile': 3.50, 'per_minute': 0.60}
]

PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'Credit Card', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 2, 'payment_method': 'Debit Card', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 3, 'payment_method': 'Digital Wallet', 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 4, 'payment_method': 'Cash', 'is_card': False, 'requires_auth': False}
]

RIDE_STATUS_MAPPING = [
    {'ride_status_id': 1, 'ride_status': 'Completed', 'is_completed': True},
    {'ride_status_id': 2, 'ride_status': 'Cancelled', 'is_completed': False}
]

VEHICLE_MAKE_MAPPING = [
    {'vehicle_make_id': 1, 'vehicle_make': 'Toyota'},
    {'vehicle_make_id': 2, 'vehicle_make': 'Honda'},
    {'vehicle_make_id': 3, 'vehicle_make': 'Ford'},
    {'vehicle_make_id': 4, 'vehicle_make': 'Chevrolet'},
    {'vehicle_make_id': 5, 'vehicle_make': 'Nissan'},
    {'vehicle_make_id': 6, 'vehicle_make': 'BMW'},
    {'vehicle_make_id': 7, 'vehicle_make': 'Mercedes'}
]

VEHICLE_MAKES_LIST = [m['vehicle_make'] for m in VEHICLE_MAKE_MAPPING]
VEHICLE_MAKE_ID_MAP = {m['vehicle_make']: m['vehicle_make_id'] for m in VEHICLE_MAKE_MAPPING}

VEHICLE_TYPES_LIST = [t['vehicle_type'] for t in VEHICLE_TYPE_MAPPING]
VEHICLE_TYPE_ID_MAP = {t['vehicle_type']: t['vehicle_type_id'] for t in VEHICLE_TYPE_MAPPING}

PAYMENT_METHODS_LIST = [p['payment_method'] for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p['payment_method']: p['payment_method_id'] for p in PAYMENT_METHOD_MAPPING}

RIDE_STATUSES_LIST = [s['ride_status'] for s in RIDE_STATUS_MAPPING]
RIDE_STATUS_ID_MAP = {s['ride_status']: s['ride_status_id'] for s in RIDE_STATUS_MAPPING}

CITY_MAPPING = [
    # Original 10
    {'city_id': 1,  'city': 'New York',       'state': 'NY', 'region': 'Northeast'},
    {'city_id': 2,  'city': 'Los Angeles',    'state': 'CA', 'region': 'West'},
    {'city_id': 3,  'city': 'Chicago',        'state': 'IL', 'region': 'Midwest'},
    {'city_id': 4,  'city': 'Houston',        'state': 'TX', 'region': 'South'},
    {'city_id': 5,  'city': 'Phoenix',        'state': 'AZ', 'region': 'Southwest'},
    {'city_id': 6,  'city': 'Philadelphia',   'state': 'PA', 'region': 'Northeast'},
    {'city_id': 7,  'city': 'San Antonio',    'state': 'TX', 'region': 'South'},
    {'city_id': 8,  'city': 'San Diego',      'state': 'CA', 'region': 'West'},
    {'city_id': 9,  'city': 'Dallas',         'state': 'TX', 'region': 'South'},
    {'city_id': 10, 'city': 'San Jose',       'state': 'CA', 'region': 'West'},
    # 30 new cities
    {'city_id': 11, 'city': 'Austin',         'state': 'TX', 'region': 'South'},
    {'city_id': 12, 'city': 'Jacksonville',   'state': 'FL', 'region': 'South'},
    {'city_id': 13, 'city': 'Fort Worth',     'state': 'TX', 'region': 'South'},
    {'city_id': 14, 'city': 'Columbus',       'state': 'OH', 'region': 'Midwest'},
    {'city_id': 15, 'city': 'Charlotte',      'state': 'NC', 'region': 'South'},
    {'city_id': 16, 'city': 'Indianapolis',   'state': 'IN', 'region': 'Midwest'},
    {'city_id': 17, 'city': 'San Francisco',  'state': 'CA', 'region': 'West'},
    {'city_id': 18, 'city': 'Seattle',        'state': 'WA', 'region': 'West'},
    {'city_id': 19, 'city': 'Denver',         'state': 'CO', 'region': 'West'},
    {'city_id': 20, 'city': 'Nashville',      'state': 'TN', 'region': 'South'},
    {'city_id': 21, 'city': 'Oklahoma City',  'state': 'OK', 'region': 'South'},
    {'city_id': 22, 'city': 'El Paso',        'state': 'TX', 'region': 'Southwest'},
    {'city_id': 23, 'city': 'Washington DC',  'state': 'DC', 'region': 'Northeast'},
    {'city_id': 24, 'city': 'Las Vegas',      'state': 'NV', 'region': 'West'},
    {'city_id': 25, 'city': 'Louisville',     'state': 'KY', 'region': 'South'},
    {'city_id': 26, 'city': 'Memphis',        'state': 'TN', 'region': 'South'},
    {'city_id': 27, 'city': 'Portland',       'state': 'OR', 'region': 'West'},
    {'city_id': 28, 'city': 'Baltimore',      'state': 'MD', 'region': 'Northeast'},
    {'city_id': 29, 'city': 'Milwaukee',      'state': 'WI', 'region': 'Midwest'},
    {'city_id': 30, 'city': 'Albuquerque',    'state': 'NM', 'region': 'Southwest'},
    {'city_id': 31, 'city': 'Tucson',         'state': 'AZ', 'region': 'Southwest'},
    {'city_id': 32, 'city': 'Fresno',         'state': 'CA', 'region': 'West'},
    {'city_id': 33, 'city': 'Sacramento',     'state': 'CA', 'region': 'West'},
    {'city_id': 34, 'city': 'Mesa',           'state': 'AZ', 'region': 'Southwest'},
    {'city_id': 35, 'city': 'Atlanta',        'state': 'GA', 'region': 'South'},
    {'city_id': 36, 'city': 'Kansas City',    'state': 'MO', 'region': 'Midwest'},
    {'city_id': 37, 'city': 'Omaha',          'state': 'NE', 'region': 'Midwest'},
    {'city_id': 38, 'city': 'Miami',          'state': 'FL', 'region': 'South'},
    {'city_id': 39, 'city': 'Minneapolis',    'state': 'MN', 'region': 'Midwest'},
    {'city_id': 40, 'city': 'Raleigh',        'state': 'NC', 'region': 'South'},
]

# Real bounding boxes (lat_min, lat_max, lon_min, lon_max) for each city
CITY_BBOX = {
    # Original 10
    'New York':       (40.4774, 40.9176, -74.2591, -73.7004),
    'Los Angeles':    (33.7037, 34.3373, -118.6682, -118.1553),
    'Chicago':        (41.6445, 42.0230, -87.9401, -87.5240),
    'Houston':        (29.5239, 30.1108, -95.7835, -95.0146),
    'Phoenix':        (33.2148, 33.9187, -112.3241, -111.9253),
    'Philadelphia':   (39.8670, 40.1379, -75.2803, -74.9558),
    'San Antonio':    (29.2098, 29.6659, -98.8088, -98.2340),
    'San Diego':      (32.5343, 33.1139, -117.2817, -116.9057),
    'Dallas':         (32.6177, 33.0237, -97.0000, -96.5546),
    'San Jose':       (37.1255, 37.4697, -122.0436, -121.5886),
    # 30 new cities
    'Austin':         (30.0986, 30.5168, -97.9382, -97.5614),
    'Jacksonville':   (30.1029, 30.5850, -82.0551, -81.3971),
    'Fort Worth':     (32.5695, 32.9935, -97.5467, -97.0472),
    'Columbus':       (39.8620, 40.1563, -83.2008, -82.7713),
    'Charlotte':      (35.0175, 35.3723, -81.0090, -80.6512),
    'Indianapolis':   (39.6326, 39.9271, -86.3282, -85.9503),
    'San Francisco':  (37.6879, 37.8324, -122.5270, -122.3482),
    'Seattle':        (47.4919, 47.7341, -122.4596, -122.2244),
    'Denver':         (39.6143, 39.9142, -105.1099, -104.5997),
    'Nashville':      (35.9946, 36.4066, -87.0599, -86.5159),
    'Oklahoma City':  (35.3378, 35.6495, -97.7696, -97.2673),
    'El Paso':        (31.6207, 31.9695, -106.6285, -106.1359),
    'Washington DC':  (38.7916, 38.9958, -77.1198, -76.9094),
    'Las Vegas':      (36.0756, 36.3849, -115.3816, -115.0625),
    'Louisville':     (38.0674, 38.3785, -85.9437, -85.4884),
    'Memphis':        (34.9944, 35.3278, -90.1730, -89.6544),
    'Portland':       (45.4325, 45.6524, -122.8367, -122.4718),
    'Baltimore':      (39.1972, 39.3722, -76.7112, -76.5290),
    'Milwaukee':      (42.9215, 43.1928, -88.0706, -87.8467),
    'Albuquerque':    (34.9479, 35.2181, -106.8813, -106.4683),
    'Tucson':         (32.0481, 32.4108, -111.0768, -110.7574),
    'Fresno':         (36.6540, 36.9296, -119.9721, -119.6527),
    'Sacramento':     (38.4344, 38.6854, -121.5625, -121.3624),
    'Mesa':           (33.3022, 33.5088, -111.9026, -111.5814),
    'Atlanta':        (33.6100, 33.8886, -84.5516, -84.2898),
    'Kansas City':    (38.8779, 39.3067, -94.7541, -94.3644),
    'Omaha':          (41.1660, 41.3999, -96.2400, -95.8677),
    'Miami':          (25.6097, 25.9088, -80.4397, -80.1441),
    'Minneapolis':    (44.8896, 45.0512, -93.3290, -93.1938),
    'Raleigh':        (35.7016, 35.9957, -78.7875, -78.5691),
}

CITY_LIST = [c['city'] for c in CITY_MAPPING]
CITY_ID_MAP = {c['city']: c['city_id'] for c in CITY_MAPPING}


def _coords_for_city(city: str) -> tuple[float, float]:
    """Return a random (latitude, longitude) that falls within the city's bounding box."""
    lat_min, lat_max, lon_min, lon_max = CITY_BBOX[city]
    lat = round(random.uniform(lat_min, lat_max), 6)
    lon = round(random.uniform(lon_min, lon_max), 6)
    return lat, lon

CANCELLATION_REASON_MAPPING = [
    {'cancellation_reason_id': 1, 'cancellation_reason': 'Driver cancelled'},
    {'cancellation_reason_id': 2, 'cancellation_reason': 'Passenger cancelled'},
    {'cancellation_reason_id': 3, 'cancellation_reason': 'No show'},
    {'cancellation_reason_id': 4, 'cancellation_reason': None}  # Completed rides
]

CANCELLATION_REASON_ID_MAP = {c['cancellation_reason']: c['cancellation_reason_id'] for c in CANCELLATION_REASON_MAPPING}



def generate_uber_ride_confirmation():
    
    # Generate timestamps
    pickup_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
    duration_minutes = random.randint(5, 120)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)
    booking_time = pickup_time - timedelta(minutes=random.randint(1, 10))
    
    # Distance in miles
    distance = round(random.uniform(0.5, 50), 2)
    
    # Pricing calculation
    base_fare = 2.50
    per_mile_rate = 1.75
    per_minute_rate = 0.35
    surge_multiplier = round(random.uniform(1.0, 2.5), 2)
    
    distance_fare = round(distance * per_mile_rate, 2)
    time_fare = round(duration_minutes * per_minute_rate, 2)
    subtotal = round((distance_fare + time_fare + base_fare) * surge_multiplier, 2)
    tip = round(random.choice([0, 0, 0, 1, 2, 3, 5, random.uniform(1, 20)]), 2)
    total_fare = round(subtotal + tip, 2)
    
    # Location details
    pickup_address = fake.address().replace('\n', ', ')
    dropoff_address = fake.address().replace('\n', ', ')
    
    # Get cities and their IDs
    pickup_city = random.choice(CITY_LIST)
    dropoff_city = random.choice(CITY_LIST)
    pickup_city_id = CITY_ID_MAP[pickup_city]
    dropoff_city_id = CITY_ID_MAP[dropoff_city]

    # Coordinates anchored to their respective cities
    pickup_latitude, pickup_longitude = _coords_for_city(pickup_city)
    dropoff_latitude, dropoff_longitude = _coords_for_city(dropoff_city)
    
    # Get vehicle make and its ID
    vehicle_make = random.choice(VEHICLE_MAKES_LIST)
    vehicle_make_id = VEHICLE_MAKE_ID_MAP[vehicle_make]
    
    # Determine cancellation status
    is_cancelled = random.random() < 0.1
    cancellation_reason = None
    cancellation_reason_id = 4  # Default: None (completed)
    if is_cancelled:
        cancellation_reason = random.choice(['Driver cancelled', 'Passenger cancelled', 'No show'])
        cancellation_reason_id = CANCELLATION_REASON_ID_MAP[cancellation_reason]

    # Get vehicle type and its ID
    vehicle_type = random.choice(VEHICLE_TYPES_LIST)
    vehicle_type_id = VEHICLE_TYPE_ID_MAP[vehicle_type]

    # Get payment method and its ID
    payment_method = random.choice(PAYMENT_METHODS_LIST)
    payment_method_id = PAYMENT_METHOD_ID_MAP[payment_method]

    # Get ride status and its ID
    ride_status = random.choice(['Completed', 'Completed', 'Cancelled'])
    ride_status_id = RIDE_STATUS_ID_MAP[ride_status]
    
    # Ride confirmation
    ride_confirmation = {
        # Keys/Identifiers
        'ride_id': str(uuid.uuid4()),
        'confirmation_number': fake.bothify('??#-####-??##'),
        'passenger_id': str(uuid.uuid4()),
        'driver_id': str(uuid.uuid4()),
        'vehicle_id': str(uuid.uuid4()),
        'pickup_location_id': str(uuid.uuid4()),
        'dropoff_location_id': str(uuid.uuid4()),
        
        # Foreign Keys to Mapping Tables
        'vehicle_type_id': vehicle_type_id,
        'vehicle_make_id': vehicle_make_id,
        'payment_method_id': payment_method_id,
        'ride_status_id': ride_status_id,
        'pickup_city_id': pickup_city_id,
        'dropoff_city_id': dropoff_city_id,
        'cancellation_reason_id': cancellation_reason_id,
        
        # Passenger Information
        'passenger_name': fake.name(),
        'passenger_email': fake.email(),
        'passenger_phone': fake.phone_number(),
        
        # Driver Information
        'driver_name': fake.name(),
        'driver_rating': round(random.uniform(4.0, 5.0), 2),
        'driver_phone': fake.phone_number(),
        'driver_license': fake.bothify('??-???-#######'),
        
        # Vehicle Information
        'vehicle_model': fake.word().capitalize(),
        'vehicle_color': random.choice(['Black', 'White', 'Gray', 'Silver', 'Blue', 'Red']),
        'license_plate': fake.bothify('???-####'),
        
        # Pickup & Dropoff Locations
        'pickup_address': pickup_address,
        'pickup_latitude': pickup_latitude,
        'pickup_longitude': pickup_longitude,
        'dropoff_address': dropoff_address,
        'dropoff_latitude': dropoff_latitude,
        'dropoff_longitude': dropoff_longitude,
        
        # Ride Details - Measures
        'distance_miles': distance,
        'duration_minutes': duration_minutes,
        'booking_timestamp': booking_time.isoformat(),
        'pickup_timestamp': pickup_time.isoformat(),
        'dropoff_timestamp': dropoff_time.isoformat(),
        
        # Pricing - Measures
        'base_fare': base_fare,
        'distance_fare': distance_fare,
        'time_fare': time_fare,
        'surge_multiplier': surge_multiplier,
        'subtotal': subtotal,
        'tip_amount': tip,
        'total_fare': total_fare,
        
        # Payment & Status
        'rating': random.choice([None, round(random.uniform(4.0, 5.0), 1)])
    }
    
    return ride_confirmation