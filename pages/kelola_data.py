import streamlit as st
import streamlit_authenticator as stauth
from PIL import Image
import page_service as ps
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
import pandas as pd

import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

st.set_page_config(
    page_title="Data Scraping",
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

if not st.session_state.authentication_status:
    st.switch_page("streamlit_app.py")

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
collection = db["hasil_proses_v1"]

total_docs = collection.count_documents({})

filter_option = {
    "list_kolom": None,
    "tanggal": None,
    "jenis_akun": None,
    "sumber": None
}

top_section = st.columns([1,2.2], gap="small")
list_kolom = collection.find_one().keys()

def fetch_data(search_query, sort_by, sort_dir, filters, page, page_size):
    # Initialize query
    query = {}
    page = page - 1

    # Apply text search if provided
    if search_query:
        query["$text"] = {"$search": search_query}
    
    if "tanggal" in filters:
        start_date, end_date = [
            date if isinstance(date, str) else date.strftime("%Y-%m-%d")
            for date in filters["tanggal"]
        ]
        query["Tanggal Publikasi"] = {"$gte": start_date, "$lte": end_date}

    # Apply filter for "Klasifikasi Akun"
    if "jenis_akun" in filters:
        query["Klasifikasi Akun"] = {"$in": filters["jenis_akun"]}
    
    # Apply filter for "Sumber"
    if "sumber" in filters:
        query["Sumber"] = {"$in": filters["sumber"]}

    # Pagination settings
    # page = st.session_state.get("page", 0)
    # page_size = st.session_state.get("page_size", 10)
    
    # Sorting direction
    sort_direction = ASCENDING if sort_dir == "🔼" else DESCENDING

    # Fetch filtered and sorted data from MongoDB
    if(page_size!="all"):
        cursor = (
            collection.find(query)
            .sort(sort_by, sort_direction)
            .skip(page * page_size)
            .limit(page_size)
        )
    else:
        cursor = (
            collection.find(query)
            .sort(sort_by, sort_direction)
        )
    data = list(cursor)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Remove MongoDB ID column
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    # Apply column selection filter
    if "list_kolom" in filters:
        available_columns = [col for col in filters["list_kolom"] if col in df.columns]
        df = df[available_columns]

    # Update DataFrame index to match pagination
    if(page_size!="all"):
        start_index = page * page_size
        df.index = range(start_index, start_index + len(df))

    return df

st.write("## Semua Data Hasil Scraping 🕵️‍♂️🚀")
st.write("###### Manajemen dan pengelolaan proses ekstraksi data berbasis web secara terstruktur dan otomatis.")

with st.container(border=True):
    cols = st.columns([3,1], gap="medium", vertical_alignment="bottom")

    with cols[0]:
        search_query = st.text_input("Search Data Posting :", placeholder="Cari data postingan disini ...")
    with cols[1]:
        popover = st.popover("Filter Data :", icon=":material/filter_alt:", use_container_width=True)

    filter_option["list_kolom"] = popover.multiselect("Kolom di tampilkan", list_kolom, ["Akun/Judul", "Konten", "Sumber"], key="display_kolom")

    excluded_columns = ["_id", "UUID", "Topik", "Konten"]
    columns = [key for key in list_kolom if key not in excluded_columns]

    with popover.container():
        cols = st.columns([1,1])

        with cols[0]:
            sort_by = st.selectbox("Sort By :", options=columns, key="sort_by")
        with cols[1]:
            sort_dir = st.radio("Direction : ", options=["🔼", "🔽"], horizontal=True, key="sort_dir")

    filter_option["tanggal"] = popover.date_input('Rentang Waktu :', [datetime(2024, 7, 1), datetime.now()], key="filter_tanggal")

    cols = popover.columns(2, gap="large", vertical_alignment="top")

    selected_jenis_akun = []
    options = ["Akun Loker", "Akun Netizen", "Akun Pers"]

    with cols[0]:
        st.write("Jenis Akun :")

        for idx, option in enumerate(options):
            if st.checkbox(option, value=True, key=f"jenis_akun_{idx}"):
                selected_jenis_akun.append(option)

        filter_option["jenis_akun"] = selected_jenis_akun

    selected_sumber = []
    options = ["Twitter", "Instagram", "Facebook", "Tiktok", "Linkedin", "Youtube", "Telegram", "Media Online"]

    with cols[1]:
        st.write("Sumber Sosmed :")

        for idx, option in enumerate(options):
            if st.checkbox(option,value=True, key=f"sumber_{idx}"):
                selected_sumber.append(option)

        filter_option["sumber"] = selected_sumber

with st.container(border=True):
    with st.spinner('Preparing data.'):
        try:
            down_menu = st.columns([1,4,1.5])

            with down_menu[0]:
                page_size = st.selectbox("Rows per page", options=[50, 100, 150, 200, 250])
                total_pages = round(total_docs / page_size)

            with down_menu[1]:
                st.text("")

            with down_menu[2]:
                page = st.number_input("Page", min_value=1, max_value=total_pages, step=1, key="a")

            # Fetch and display the data
            st.session_state["filter_option"] = filter_option
            st.session_state["max_value"] = total_docs
            df = fetch_data(search_query, sort_by, sort_dir, filter_option, page, page_size)
            st.dataframe(df, use_container_width=True, height=500)

            down_menu = st.columns([2,4,1])
            with down_menu[0]:
                st.write(f"**Total Data**: {total_docs}")
            with down_menu[1]:
                st.text("")
            with down_menu[2]:
                st.write(f"**Page**: {page} / {total_pages}")
        except Exception as e:
            st.info("")
            st.error("Gagal Menyiapkan Data." + str(e))