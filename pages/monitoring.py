import streamlit as st
from PIL import Image
import page_service as ps
from pymongo import MongoClient, ASCENDING, DESCENDING
# from datetime import datetime, timedelta
import datetime
import pandas as pd
import plotly.express as px
from io import BytesIO

import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

# Inisiasi page
st.set_page_config(
    page_title="Analisa Data Pasker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

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

st.markdown("""
    <style>
        .stMainBlockContainer{
            padding-left: 40px;
            padding-right: 40px;
            padding-top: 20px;
            padding-bottom: 40px;
        }
    </style>
""", unsafe_allow_html=True)
ps.setup_style_awal(st)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")


if(authenticator):
    ps.setup_st_sidebar(st, authenticator)

#init DB
@st.cache_resource
def init_connection():
    return MongoClient(**st.secrets["mongo"])

client = init_connection()
db = client.medmon
collection = db["hasil_proses_v1"]

# Main Page Function
st.write("# Analisa Data 🕵️‍♂️🚀")
st.subheader("Semua Data Hasil Scraping & Klasifikasi")


def fetch_data(search_query, sort_by, sort_dir):
    query = {}

    if search_query:
        query = {"$text": {"$search": search_query}}

    page = st.session_state["page"] if "page" in st.session_state else 0
    page_size = st.session_state["page_size"] if "page_size" in st.session_state else 10
    
    sort_direction = ASCENDING if sort_dir == "🔼" else DESCENDING
    
    cursor = collection.find(query).sort(sort_by, sort_direction).skip(page * page_size).limit(page_size)
    data = list(cursor)
    
    df = pd.DataFrame(data)

    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    

    start_index = page * page_size
    df.index = range(start_index, start_index + len(df))

    return df

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
    cursor = (
        collection.find(query)
        .sort(sort_by, sort_direction)
        .skip(page * page_size)
        .limit(page_size)
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
    start_index = page * page_size
    df.index = range(start_index, start_index + len(df))

    return df

kd = ["Tipe Pekerjaan", "Tingkat Pekerjaan", "Tingkat Pendidikan", "Pengalaman Kerja", 
      "Tunjangan", "Jenis Kelamin", "Cara Kerja", "Lokasi", 
      "Keterampilan Bahasa", "Keterampilan Teknis", "Keterampilan Non Teknis"]

@st.cache_data()
def get_all_column():
    # result = collection.find_one().keys()
    return None

@st.cache_data(ttl=180)
def get_top_accounts(limit=10):
    pipeline = [
        {"$match": {"Klasifikasi Akun": "Akun Loker"}},  # Filter by category
        {
            "$group": {
                "_id": "$Akun/Judul",
                "Followers": {"$last": "$Followers"},
                "Sumber": {"$last": "$Sumber"}  # Include Sumber in the output
            }
        },
        {"$sort": {"Followers": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 1,
                "Sumber": 1,
                "Followers": 1,
            }
        }
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

@st.cache_data(ttl=180)
def get_total_posts_by_classification():
    pipeline = [
        {"$group": {"_id": "$Klasifikasi Akun", "Count": {"$sum": 1}}}
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

@st.cache_data(ttl=180)
def get_category_counts(category_field):
    # If the category is "Jenis Kelamin", handle it specifically
    if category_field == "Jenis Kelamin":
        pipeline = [
            {
                "$project": {
                    "Jenis Kelamin": {
                        "$cond": [
                            {
                                "$eq": [
                                    {"$size": {"$setIntersection": [{"$ifNull": ["$Jenis Kelamin", []]}, ["Laki-Laki", "Perempuan"]]}},
                                    2
                                ]
                            },
                            "Laki-laki & Perempuan",  # Both genders are present
                            {"$arrayElemAt": ["$Jenis Kelamin", 0]}  # Just one gender
                        ]
                    }
                }
            },
            {"$group": {"_id": "$Jenis Kelamin", "Count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": "tdk_ada_informasi"}}}
        ]
    else:
        # Default pipeline for other categories
        pipeline = [
            {"$unwind": f"${category_field}"},
            {"$group": {"_id": f"${category_field}", "Count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": "tdk_ada_informasi"}}}
        ]
    
    # Execute the aggregation pipeline and return as DataFrame
    return pd.DataFrame(list(collection.aggregate(pipeline)))

filter_option = {
    "list_kolom": None,
    "tanggal": None,
    "jenis_akun": None,
    "sumber": None
}

top_section = st.columns([1,2.2], gap="small")
list_kolom = collection.find_one().keys()

with top_section[0]:
    # Date filter options
    with st.container(border=True, height=550):
        st.write("##### Filter Data :")

        search_query = st.text_input("Search Data Posting :", placeholder="Cari data postingan disini ...")

        filter_option["list_kolom"] = st.multiselect("Kolom di tampilkan", list_kolom, ["Akun/Judul", "Konten", "Sumber"])

        excluded_columns = ["_id", "UUID", "Topik", "Konten"]
        columns = [key for key in list_kolom if key not in excluded_columns]
        sort_by = st.selectbox("Sort By :", options=columns)
        
        sort_dir = st.radio("Direction", options=["🔼", "🔽"], horizontal=True)

        filter_option["tanggal"] = st.date_input('Rentang Waktu :', [datetime.datetime(2024, 7, 1), datetime.datetime.now()])

        # st.write("Jenis Akun :")
        
        with st.expander("Jenis Akun:", expanded=True):
            selected_jenis_akun = []
            options = ["Akun Loker", "Akun Netizen", "Akun Pers"]

            for option in options:
                if st.checkbox(option, value=True):
                    selected_jenis_akun.append(option)

        filter_option["jenis_akun"] = selected_jenis_akun

        # st.write("Sumber Sosmed :")
        with st.expander("Sumber Sosmed:", expanded=False):
            selected_sumber = []
            options = ["Twitter", "Instagram", "Facebook", "Tiktok", "Linkedin", "Youtube"]

            for option in options:
                if st.checkbox(option,value=True):
                    selected_sumber.append(option)

        filter_option["sumber"] = selected_sumber

with top_section[1]:
    with st.container(border=True):
        st.write("##### Scrapping Data :")

        with st.spinner('Preparing data.'):
            try:
                down_menu = st.columns([4.5,1.5,1], vertical_alignment="center")
                
                with down_menu[1]:
                    page = st.number_input("Page", min_value=1, step=1, key="a")

                with down_menu[2]:
                    page_size = st.selectbox("Rows per page", options=[50, 100, 150, 200, 250])

                # Display total documents and calculate total pages
                with down_menu[0]:
                    total_docs = collection.count_documents({})
                    total_pages = round(total_docs / page_size)
                    st.write(f"**Total Data**: {total_docs}, **Page**: {page} / {total_pages}")

                # Fetch and display the data
                df = fetch_data(search_query, sort_by, sort_dir, filter_option, page, page_size)
                st.dataframe(df, use_container_width=True)

                
            except Exception as e:
                st.info("")
                st.error("Gagal Menyiapkan Data." + str(e))

top_chart = st.columns(3, gap="small")
# Top 10 Accounts with Most Followers
with top_chart[0]:
    with st.container(border=True):
        st.write("### Top 10 :red[Akun Loker Follower] Terbanyak")
        follower_by_account = get_top_accounts()
        st.dataframe(follower_by_account, hide_index=True, use_container_width=True)

with top_chart[1]:
    with st.container(border=True):
        st.write("### Distribusi Data Berdasarkan :red[Sumber Sosial Media]")
        
        # Query to count each source
        source_count = collection.aggregate([
            {"$group": {"_id": "$Sumber", "Count": {"$sum": 1}}}
        ])
        source_count_df = pd.DataFrame(list(source_count))
        source_count_df.columns = ['Sumber', 'Count']

        # Pie chart for source distribution
        fig_source_pie = px.pie(source_count_df, names='Sumber', values='Count', title='Distribusi Sumber Sosial Media')
        st.plotly_chart(fig_source_pie)

# Total Posts by Account Classification
with top_chart[2]:
    with st.container(border=True):
        st.write("### Total Postingan Berdasarkan :red[Klasifikasi Akun]")
        sentimen_count = get_total_posts_by_classification()
        sentimen_count.columns = ['Klasifikasi Akun', 'Count']
        fig_sentimen = px.bar(sentimen_count, x='Klasifikasi Akun', y='Count', color='Klasifikasi Akun')
        st.plotly_chart(fig_sentimen)

# Loop through categories and generate charts
for idx, item in enumerate(kd):
    with st.container(border=True):
        st.subheader(f'Distribusi Data :red[{item}] di Setiap Postingan')

        # Get category count data
        category_count = get_category_counts(item)
        category_count.columns = [item, 'Count']

        col1, col2 = st.columns(2, gap="small")

        if idx % 3 == 0:
            with col1:
                st.dataframe(category_count, hide_index=True)
            with col2:
                fig_pie_category = px.pie(category_count, names=item, values='Count')
                st.plotly_chart(fig_pie_category)
        elif idx % 3 == 1:
            with col1:
                fig_bar_category = px.bar(category_count, x='Count', y=item, orientation='h')
                st.plotly_chart(fig_bar_category)
            with col2:
                st.dataframe(category_count, hide_index=True)
        else:
            with col2:
                fig_bar_category = px.bar(category_count, y='Count', x=item, orientation='v')
                st.plotly_chart(fig_bar_category)
            with col1:
                st.dataframe(category_count, hide_index=True)