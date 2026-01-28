import os
import math
import random
import logging
import json
from functools import wraps
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from flask import Flask, request, redirect, url_for, session, render_template, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration
AWS_REGION = "ap-south-2"
DYNAMODB_TABLE_NAME = "BusRoute"
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-2:251198450365:Route_42_alerts"
APP_SECRET = os.environ.get("APP_SECRET", "super_secret_key_change_this_in_production_2025")
USERS_FILE = "users_data.json"

# Get absolute path for templates
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')

app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = APP_SECRET
app.permanent_session_lifetime = timedelta(days=7)

# AWS Clients
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
sns_client = boto3.client("sns", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Bus routes (static data) - 3 BUSES ONLY
buses = {
    "10A": {
        "route": "Secunderabad → Mehdipatnam",
        "stops": [
            {"name": "Secunderabad", "lat": 17.4399, "lon": 78.4983},
            {"name": "Begumpet", "lat": 17.4444, "lon": 78.4611},
            {"name": "Ameerpet", "lat": 17.4375, "lon": 78.4483},
            {"name": "Punjagutta", "lat": 17.4294, "lon": 78.4506},
            {"name": "Mehdipatnam", "lat": 17.3956, "lon": 78.4403}
        ],
        "frequency": "Every 10 mins",
        "type": "AC"
    },
    "100M": {
        "route": "JBS → MGBS",
        "stops": [
            {"name": "JBS", "lat": 17.4565, "lon": 78.4998},
            {"name": "Paradise", "lat": 17.4432, "lon": 78.4841},
            {"name": "Charminar", "lat": 17.3616, "lon": 78.4747},
            {"name": "Moazzam Jahi Market", "lat": 17.3832, "lon": 78.4735},
            {"name": "MGBS", "lat": 17.3840, "lon": 78.4804}
        ],
        "frequency": "Every 8 mins",
        "type": "Metro Express"
    },
    "65G": {
        "route": "Miyapur → Shamshabad",
        "stops": [
            {"name": "Miyapur", "lat": 17.4947, "lon": 78.3569},
            {"name": "Lingampally", "lat": 17.4948, "lon": 78.3246},
            {"name": "Gachibowli", "lat": 17.4401, "lon": 78.3489},
            {"name": "Rajiv Gandhi Airport", "lat": 17.2402, "lon": 78.4294},
            {"name": "Shamshabad", "lat": 17.2289, "lon": 78.4297}
        ],
        "frequency": "Every 30 mins",
        "type": "Airport Express"
    }
}

# In-memory data
users = {}
user_travel_history = {}
user_subscriptions = {}


def load_users():
    """Load users and subscriptions from JSON file"""
    global users, user_subscriptions, user_travel_history
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r") as f:
                data = json.load(f)
                users = data.get("users", {})
                user_subscriptions = data.get("subscriptions", {})
                user_travel_history = data.get("history", {})
            logging.info(f"✅ Loaded {len(users)} users from {USERS_FILE}")
        except Exception as e:
            logging.error(f"❌ Failed to load user data: {e}")


def save_users():
    """Save users and subscriptions to JSON file"""
    try:
        data = {
            "users": users,
            "subscriptions": user_subscriptions,
            "history": user_travel_history
        }
        with open(USERS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"✅ Saved {len(users)} users to {USERS_FILE}")
    except Exception as e:
        logging.error(f"❌ Failed to save user data: {e}")


def now_ts():
    return int(datetime.now(timezone.utc).timestamp())


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please login first.", "warning")
            return redirect(url_for("login"))

        # Check if user still exists in the users dictionary
        username = session.get("user_id")
        if username not in users:
            logging.warning(f"⚠️ User '{username}' in session but not in users dict. Clearing session.")
            session.clear()
            flash("Your session has expired. Please login again.", "warning")
            return redirect(url_for("login"))

        return view_func(*args, **kwargs)

    return wrapper


def decimalize(x):
    if x is None or isinstance(x, Decimal):
        return x
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return Decimal(str(x))
    return x


def safe_put_item(item: dict, op="put_item"):
    """
    Safely insert item into DynamoDB with validation
    ✅ NOW VALIDATES: Only allows known bus IDs
    """
    if "BusID" not in item or not str(item["BusID"]).strip():
        raise ValueError("Missing BusID")

    # Convert BusID to uppercase first
    item["BusID"] = str(item["BusID"]).upper()

    # ✅ VALIDATE: Ensure bus exists in our system
    if item["BusID"] not in buses:
        raise ValueError(f"Unknown bus_id: {item['BusID']}. Valid buses: {', '.join(buses.keys())}")

    if "TimeStamp" not in item or item["TimeStamp"] in (None, ""):
        item["TimeStamp"] = now_ts()
    if isinstance(item["TimeStamp"], datetime):
        item["TimeStamp"] = int(item["TimeStamp"].timestamp())
    elif isinstance(item["TimeStamp"], str):
        item["TimeStamp"] = int(datetime.fromisoformat(item["TimeStamp"].replace("Z", "+00:00")).timestamp())
    else:
        item["TimeStamp"] = int(item["TimeStamp"])

    for k in ("Latitude", "Longitude", "SpeedKmph", "DistanceToStopKm"):
        if k in item and item[k] is not None:
            item[k] = decimalize(item[k])
    table.put_item(Item=item)
    logging.info(f"✅ DynamoDB {op}: BusID={item['BusID']} TimeStamp={item['TimeStamp']}")


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def sns_publish(subject, message):
    return sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)


def format_ts(ts):
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def add_to_travel_history(username, bus_id):
    if username not in user_travel_history:
        user_travel_history[username] = []
    if bus_id not in user_travel_history[username]:
        user_travel_history[username].append(bus_id)
        save_users()
        logging.info(f"Added {bus_id} to {username}'s history")


def get_travel_history(username):
    bus_ids = user_travel_history.get(username, [])
    history_data = []
    for bus_id in bus_ids:
        if bus_id in buses:
            try:
                resp = table.query(
                    KeyConditionExpression=Key("BusID").eq(bus_id),
                    ScanIndexForward=False,
                    Limit=5
                )
                trips = resp.get("Items", [])
                history_data.append({
                    "bus_id": bus_id,
                    "bus_info": buses[bus_id],
                    "trip_count": len(trips),
                    "last_trip": trips[0] if trips else None
                })
            except Exception as e:
                logging.error(f"Query failed for bus {bus_id}: {e}")
                history_data.append({
                    "bus_id": bus_id,
                    "bus_info": buses[bus_id],
                    "trip_count": 0,
                    "last_trip": None
                })
    return history_data


def is_user_subscribed(username):
    return user_subscriptions.get(username, False)


@app.post("/api/v1/track")
def api_track():
    try:
        data = request.get_json(force=True)
        bus_id = str(data.get("bus_id", "")).upper().strip()
        lat, lon = float(data["lat"]), float(data["lon"])
        speed = float(data.get("speed_kmph", random.uniform(10, 35)))
        status = str(data.get("status", "ACTIVE")).upper()
        ts = data.get("timestamp", now_ts())

        if bus_id not in buses:
            return jsonify({"ok": False, "error": f"Unknown bus_id {bus_id}"}), 400

        stops = buses[bus_id]["stops"]
        dists = [(s["name"], haversine_km(lat, lon, s["lat"], s["lon"])) for s in stops]
        next_stop, dist_km = min(dists, key=lambda x: x[1])

        item = {
            "BusID": bus_id,
            "TimeStamp": ts,
            "Latitude": lat,
            "Longitude": lon,
            "SpeedKmph": speed,
            "Status": status,
            "Route": buses[bus_id]["route"],
            "NextStop": next_stop,
            "DistanceToStopKm": dist_km
        }

        safe_put_item(item, op="api_track")

        # Send alerts if within radius
        alert_radius_km = float(data.get("alert_radius_km", 0.5))
        if dist_km <= alert_radius_km:
            subscribed_users = [user for user, subscribed in user_subscriptions.items() if subscribed]
            if subscribed_users:
                sns_publish(
                    subject=f"Bus {bus_id} nearing {next_stop}",
                    message=f"RouteWise Alert\nBus: {bus_id}\nNext Stop: {next_stop}\nDistance: {dist_km:.2f} km\nTime: {format_ts(item['TimeStamp'])}\n\nAlert sent to {len(subscribed_users)} subscribed users."
                )
                logging.info(f"🔔 Alert sent for bus {bus_id} to {len(subscribed_users)} users")

        return jsonify({"ok": True, "stored": item}), 200

    except Exception as e:
        logging.exception("api_track failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/")
def index():
    if "user_id" not in session:
        return redirect(url_for("login"))
    return redirect(url_for("home"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        email = request.form.get("email", "").strip()

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("register.html", logged_in=False)

        if username in users:
            flash("Username already exists.", "error")
            return render_template("register.html", logged_in=False)

        users[username] = {
            "pw": generate_password_hash(password),
            "email": email
        }
        user_subscriptions[username] = False
        user_travel_history[username] = []

        save_users()
        flash("Registration successful. Please login.", "success")
        return redirect(url_for("login"))

    return render_template("register.html", logged_in=False)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        u = users.get(username)
        if not u or not check_password_hash(u["pw"], password):
            flash("Invalid username or password.", "error")
            return render_template("login.html", logged_in=False)

        session["user_id"] = username
        session.permanent = True
        flash("Logged in successfully.", "success")
        return redirect(url_for("home"))

    return render_template("login.html", logged_in=False)


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    username = session["user_id"]
    return render_template(
        "home.html",
        logged_in=True,
        username=username,
        buses=buses,
        travel_history=get_travel_history(username),
        is_subscribed=is_user_subscribed(username)
    )


@app.route("/buses")
@login_required
def buses_page():
    return render_template("buses.html", logged_in=True, buses=buses)


@app.route("/bus/<bus_id>")
@login_required
def bus_details(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses:
        flash(f"Bus {bus_id} not found.", "error")
        return redirect(url_for("home"))

    add_to_travel_history(session["user_id"], bus_id)
    latest = None
    try:
        resp = table.query(
            KeyConditionExpression=Key("BusID").eq(bus_id),
            ScanIndexForward=False,
            Limit=1
        )
        items = resp.get("Items", [])
        latest = items[0] if items else None
        if latest and "TimeStamp" in latest:
            latest["FormattedTime"] = format_ts(latest["TimeStamp"])
            for key in ['Latitude', 'Longitude', 'SpeedKmph', 'DistanceToStopKm']:
                if key in latest and isinstance(latest[key], Decimal):
                    latest[key] = float(latest[key])
    except Exception as e:
        logging.error(f"Latest fetch failed: {e}")

    return render_template(
        "bus_details.html",
        logged_in=True,
        bus_id=bus_id,
        bus=buses[bus_id],
        latest=latest,
        is_subscribed=is_user_subscribed(session["user_id"])
    )


@app.route("/bus/<bus_id>/history")
@login_required
def bus_history(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses:
        flash(f"Bus {bus_id} not found.", "error")
        return redirect(url_for("home"))

    items = []
    try:
        resp = table.query(
            KeyConditionExpression=Key("BusID").eq(bus_id),
            ScanIndexForward=False,
            Limit=25
        )
        items = resp.get("Items", [])
        for r in items:
            if "TimeStamp" in r:
                r["FormattedTime"] = format_ts(r["TimeStamp"])
            for key in ['Latitude', 'Longitude', 'SpeedKmph', 'DistanceToStopKm']:
                if key in r and isinstance(r[key], Decimal):
                    r[key] = float(r[key])
    except Exception as e:
        logging.error(f"History fetch failed: {e}")

    return render_template("bus_history.html", logged_in=True, bus_id=bus_id, bus=buses[bus_id], items=items)


@app.route("/subscribe", methods=["GET", "POST"])
@login_required
def subscribe():
    username = session["user_id"]

    # Safe access to user data with fallback
    user_data = users.get(username, {})
    user_email = user_data.get("email", "")
    is_subscribed = is_user_subscribed(username)

    if request.method == "POST":
        action = request.form.get("action", "subscribe")
        email = request.form.get("email", "").strip()

        if action == "subscribe":
            if not email:
                flash("❌ Please provide a valid email address.", "error")
                return redirect(url_for("subscribe"))

            # Ensure user exists in dictionary before updating
            if username not in users:
                logging.error(f"User {username} not found during subscription")
                flash("❌ User account error. Please re-register.", "error")
                session.clear()
                return redirect(url_for("login"))

            users[username]["email"] = email
            user_subscriptions[username] = True
            save_users()
            flash(f"✅ Subscribed! Alerts will be sent to {email}. Confirm via AWS SNS email.", "success")

        elif action == "unsubscribe":
            user_subscriptions[username] = False
            save_users()
            flash("✅ You have been unsubscribed from alerts.", "info")

        return redirect(url_for("subscribe"))

    return render_template(
        "subscribe.html",
        logged_in=True,
        is_subscribed=is_subscribed,
        user_email=user_email
    )


@app.route("/admin/simulate/<bus_id>")
@login_required
def simulate(bus_id):
    bus_id = bus_id.upper().strip()
    if bus_id not in buses:
        flash(f"Unknown bus {bus_id}", "error")
        return redirect(url_for("home"))

    stop = random.choice(buses[bus_id]["stops"])
    payload = {
        "bus_id": bus_id,
        "lat": stop["lat"] + random.uniform(-0.005, 0.005),
        "lon": stop["lon"] + random.uniform(-0.005, 0.005),
        "speed_kmph": random.uniform(10, 35),
        "status": "ACTIVE",
        "alert_radius_km": 0.5
    }

    with app.test_request_context(json=payload):
        api_track()
    flash("✅ GPS detected - Bus tracking updated!", "success")
    return redirect(url_for("bus_details", bus_id=bus_id))


@app.route("/admin/generate-test-data")
@login_required
def generate_test_data():
    count = 0
    for bus_id in buses.keys():
        for i in range(5):
            stop = random.choice(buses[bus_id]["stops"])
            safe_put_item({
                "BusID": bus_id,
                "TimeStamp": now_ts() - (i * 300),
                "Latitude": stop["lat"] + random.uniform(-0.01, 0.01),
                "Longitude": stop["lon"] + random.uniform(-0.01, 0.01),
                "SpeedKmph": random.uniform(15, 45),
                "Status": "ACTIVE",
                "Route": buses[bus_id]["route"],
                "NextStop": stop["name"],
                "DistanceToStopKm": random.uniform(0.1, 2.0)
            })
            count += 1
    flash(f"✅ Generated {count} test records.", "success")
    return redirect(url_for("home"))


@app.route("/admin/clear-all-data")
@login_required
def clear_all_data():
    """Delete ALL items from DynamoDB table"""
    try:
        # Scan all items
        response = table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        # Delete each item
        count = 0
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(
                    Key={
                        'BusID': item['BusID'],
                        'TimeStamp': item['TimeStamp']
                    }
                )
                count += 1

        logging.info(f"🗑️ Deleted {count} items from DynamoDB")
        flash(f"✅ Deleted {count} old records from database.", "success")

        # Now regenerate data for the 3 buses
        return redirect(url_for("generate_test_data"))

    except Exception as e:
        logging.error(f"❌ Clear data failed: {e}")
        flash(f"❌ Error clearing data: {str(e)}", "error")
        return redirect(url_for("home"))


@app.route("/admin/delete-bus/<bus_id>")
@login_required
def delete_bus_data(bus_id):
    """Delete all data for a specific bus"""
    try:
        bus_id = bus_id.upper().strip()

        # Query all items for this bus
        response = table.query(
            KeyConditionExpression=Key("BusID").eq(bus_id)
        )
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key("BusID").eq(bus_id),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        # Delete each item
        count = 0
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(
                    Key={
                        'BusID': item['BusID'],
                        'TimeStamp': item['TimeStamp']
                    }
                )
                count += 1

        logging.info(f"🗑️ Deleted {count} items for bus {bus_id}")
        flash(f"✅ Deleted {count} records for bus {bus_id}.", "success")
        return redirect(url_for("home"))

    except Exception as e:
        logging.error(f"❌ Delete bus data failed: {e}")
        flash(f"❌ Error deleting bus data: {str(e)}", "error")
        return redirect(url_for("home"))


@app.route("/admin/cleanup-unknown-buses")
@login_required
def cleanup_unknown_buses():
    """Find and delete records for buses not in current system"""
    try:
        logging.info("🔍 Scanning DynamoDB for unknown buses...")

        # Scan all items in DynamoDB
        response = table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        # Find records with bus IDs not in current buses dict
        unknown_items = [item for item in items if item['BusID'] not in buses]

        if not unknown_items:
            flash("✅ No old/unknown bus records found. Database is clean!", "success")
            logging.info("✅ Database is clean - no unknown buses found")
            return redirect(url_for("home"))

        # Get list of unknown bus IDs
        unknown_bus_ids = sorted(set(item['BusID'] for item in unknown_items))

        logging.info(f"⚠️ Found {len(unknown_items)} records for unknown buses: {unknown_bus_ids}")

        # Delete all unknown records
        count = 0
        with table.batch_writer() as batch:
            for item in unknown_items:
                batch.delete_item(
                    Key={
                        'BusID': item['BusID'],
                        'TimeStamp': item['TimeStamp']
                    }
                )
                count += 1

        logging.info(f"🗑️ Deleted {count} records for old buses: {unknown_bus_ids}")
        flash(f"✅ Cleaned up {count} old records for buses: {', '.join(unknown_bus_ids)}", "success")
        return redirect(url_for("home"))

    except Exception as e:
        logging.error(f"❌ Cleanup failed: {e}")
        flash(f"❌ Error during cleanup: {str(e)}", "error")
        return redirect(url_for("home"))


if __name__ == "__main__":
    load_users()

    # Auto-generate test data for all buses if missing
    try:
        logging.info("🔍 Checking DynamoDB for existing bus data...")

        buses_with_data = []
        for bus_id in buses.keys():
            resp = table.query(
                KeyConditionExpression=Key("BusID").eq(bus_id),
                Limit=1
            )
            if resp.get('Items'):
                buses_with_data.append(bus_id)

        missing_buses = [b for b in buses.keys() if b not in buses_with_data]

        if missing_buses:
            logging.info(f"⚠️ Missing data for: {', '.join(missing_buses)}")
            logging.info("🔧 Auto-generating test data for ALL buses...")

            count = 0
            for bus_id in buses.keys():
                for i in range(5):
                    stop = random.choice(buses[bus_id]["stops"])
                    safe_put_item({
                        "BusID": bus_id,
                        "TimeStamp": now_ts() - (i * 300),
                        "Latitude": stop["lat"] + random.uniform(-0.01, 0.01),
                        "Longitude": stop["lon"] + random.uniform(-0.01, 0.01),
                        "SpeedKmph": random.uniform(15, 45),
                        "Status": "ACTIVE",
                        "Route": buses[bus_id]["route"],
                        "NextStop": stop["name"],
                        "DistanceToStopKm": random.uniform(0.1, 2.0)
                    })
                    count += 1

            logging.info(f"✅ Auto-generated {count} test records for ALL {len(buses)} buses!")
        else:
            logging.info(f"✅ Data exists for all {len(buses)} buses: {', '.join(buses_with_data)}")

    except Exception as e:
        logging.error(f"❌ Auto-generate check failed: {e}")

    logging.info("=" * 70)
    logging.info("🚌 RouteWise - City Bus Route Tracking System")
    logging.info("=" * 70)
    logging.info(f"📍 Total Buses Available: {len(buses)}")
    logging.info(f"🚍 Bus Routes: {', '.join(buses.keys())}")
    logging.info("=" * 70)
    logging.info("1. Register at http://localhost:5000/register")
    logging.info("2. Login and explore all bus routes!")
    logging.info("3. Click 🛰️ GPS buttons to simulate real-time tracking")
    logging.info("4. Subscribe to alerts at /subscribe")
    logging.info("5. Generate more data at /admin/generate-test-data")
    logging.info("6. Clear all data at /admin/clear-all-data")
    logging.info("7. Clean unknown buses at /admin/cleanup-unknown-buses")
    logging.info("=" * 70)
    app.run(host="0.0.0.0", port=5000, debug=True)
