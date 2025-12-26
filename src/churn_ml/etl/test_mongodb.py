import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables from .env
load_dotenv()

# Correct environment variable name
MONGODB_URI = os.getenv("MONGODB_URL")

if not MONGODB_URI:
    raise ValueError("MONGODB_URL is not set in the environment variables")

# Create MongoDB client
client = MongoClient(
    MONGODB_URI,
    server_api=ServerApi("1")
)

# Validate connection with a ping
try:
    client.admin.command("ping")
    print("MongoDB connection established successfully.")
except Exception as e:
    raise ConnectionError("Failed to connect to MongoDB") from e
