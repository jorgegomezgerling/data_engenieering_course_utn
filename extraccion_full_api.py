from funciones import get_data
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")

