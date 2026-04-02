import os
import psycopg2
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

# Read vars
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

print("CONF:", {
    "PGHOST": PGHOST,
    "PGPORT": PGPORT,
    "PGDATABASE": PGDATABASE,
    "PGUSER": PGUSER,
    "PGPASSWORD": "SET" if PGPASSWORD else "MISSING",
})

# Basic check
missing = [k for k, v in {
    "PGHOST": PGHOST, "PGPORT": PGPORT, "PGDATABASE": PGDATABASE,
    "PGUSER": PGUSER, "PGPASSWORD": PGPASSWORD
}.items() if not v]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}. "
                       "Ensure .env has PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.")

# Connect
conn = psycopg2.connect(
    host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD
)
with conn, conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_schema(), version();")
    print("DB OK:", cur.fetchone())
print("CONNECTED")
