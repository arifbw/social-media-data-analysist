import streamlit as st
import streamlit_authenticator as stauth
from PIL import Image
import page_service as ps
from pymongo import MongoClient, ASCENDING, DESCENDING

import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import pandas as pd


st.set_page_config(
    page_title="Box and Hoax Detector",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

# Cek Auth
with open('config.yaml') as file:
    config_yaml = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config_yaml['credentials'],
    config_yaml['cookie']['name'],
    config_yaml['cookie']['key'],
    config_yaml['cookie']['expiry_days'],
    auto_hash=False
)

authenticator.login(location="unrendered")

ps.setup_style_awal(st)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")

if 'auth_obj' in st.session_state:
    authenticator = st.session_state["auth_obj"]

ps.setup_st_sidebar(st, authenticator)

#init DB
@st.cache_resource
def init_connection():
    return MongoClient(**st.secrets["mongo"])

client = init_connection()
db = client.medmon

@st.cache_data(ttl=300)
def fetch_data_bot():
    collection = db["bot"]
    query = {"Persentase Bot": {"$gt": 35}}
    projection = {
        "Nama Akun": 1,
        "Persentase Bot": 1,
        "Bot Risk": 1,
        "Total Postingan Serupa dengan Akun Berbeda": 1,
        "Akun dengan Postingan Serupa": 1,
        "_id": 0  # Exclude the MongoDB "_id" field
    }
    data = list(collection.find(query, projection))
    df = pd.DataFrame(data)
    return df

@st.cache_data(ttl=300)
def fetch_data_scam():
    collection = db["hasil_proses_v1"]
    query = {
        "Scam Detector": {"$all": ["Terindikasi Scam"]},
        "Persentase Lowongan": {"$gt": 20},
        "Keterampilan Teknis": {"$all": ["tdk_ada_informasi"]},
        "Keterampilan Non Teknis": {"$all": ["tdk_ada_informasi"]},
        "Keterampilan Bahasa": {"$all": ["tdk_ada_informasi"]},
        "Konten": {
            "$not": {
                "$regex": "(hindari|waspada|hati-hati|awas)", 
                "$options": "i"
            }
        }
    }
    projection = {
        "Akun/Judul": 1,
        "Klasifikasi Akun": 1,
        "Konten": 1,
        "Scam Detector": 1,
        "Persentase Lowongan": 1,
        "Sumber": 1,
        "_id": 0  # Exclude the MongoDB "_id" field
    }
    data = list(collection.find(query, projection))
    df = pd.DataFrame(data)
    return df

st.write("## Bot And Hoax Detector 🕵️‍♂️🚀")
st.write("###### Sistem deteksi otomatis untuk identifikasi aktivitas bot dan validasi informasi terhadap indikasi hoaks")

tabs = st.tabs(["Terindikasi Bot", "Terindikasi Hoax"])

with tabs[0]:
    data = fetch_data_bot()

    # Display the data as a Streamlit table
    if not data.empty:
        st.dataframe(data, use_container_width=True)
    else:
        st.write("No data found in the MongoDB collection.")

with tabs[1]:
    data = fetch_data_scam()

    # Display the data as a Streamlit table
    if not data.empty:
        st.dataframe(data, use_container_width=True)
    else:
        st.write("No data found in the MongoDB collection.")