import streamlit as st
from PIL import Image
import page_service as ps
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
from io import BytesIO

# Inisiasi page
st.set_page_config(
    page_title="Analisa Data Pasker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

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

# Main Page Function
st.write("# Analisa Data 🕵️‍♂️🚀")
st.subheader("Semua Data Hasil Scraping & Klasifikasi")


def fetch_data(search_query, sort_by, sort_dir):
    query = {}

    if search_query:
        query = {"$text": {"$search": search_query}}  # Assuming full-text search index is enabled

    page = st.session_state["page"] if "page" in st.session_state else 0
    page_size = st.session_state["page_size"] if "page_size" in st.session_state else 10
    
    # Convert sort direction to pymongo format
    sort_direction = ASCENDING if sort_dir == "🔼" else DESCENDING
    
    # Fetch data with sorting and pagination
    cursor = collection.find(query).sort(sort_by, sort_direction).skip(page * page_size).limit(page_size)
    data = list(cursor)
    
    # Convert to DataFrame for display in Streamlit
    df = pd.DataFrame(data)

    if '_id' in df.columns:
        df = df.drop(columns=['_id'])  # Drop the MongoDB _id column if not needed
    

    start_index = page * page_size
    df.index = range(start_index + 1, start_index + 1 + len(df))

    return df

def filter_data(data, filter_option, start_date=None, end_date=None):
    today = datetime.today().date()
    jenis_akun_filter = filter_option["jenis_akun"]

    if filter_option["tanggal"] == "Today":
        filtered_data = data[(data["Tanggal Publikasi"].dt.date == today)]
    elif filter_option["tanggal"] == "Tomorrow":
        filtered_data = data[(data["Tanggal Publikasi"].dt.date == today + timedelta(days=1))]
    elif filter_option["tanggal"] == "Last 3 Days":
        filtered_data = data[(data["Tanggal Publikasi"].dt.date >= today - timedelta(days=2))]
    elif filter_option["tanggal"] == "This Month":
        filtered_data = data[(data["Tanggal Publikasi"].dt.month == today.month) & (data["Tanggal Publikasi"].dt.year == today.year)]
    elif filter_option["tanggal"] == "Last 3 Months":
        three_months_ago = today - timedelta(days=90)
        filtered_data = data[(data["Tanggal Publikasi"].dt.date >= three_months_ago)]
    elif filter_option["tanggal"] == "Date Range" and start_date and end_date:
        filtered_data = data[(data["Tanggal Publikasi"].dt.date >= start_date) & (data["Tanggal Publikasi"].dt.date <= end_date)]
    else:
        filtered_data = data

    filtered_data =  filtered_data[filtered_data['Klasifikasi Akun'].isin(jenis_akun_filter)]
    
    return filtered_data

kd = ["Tipe Pekerjaan", "Tingkat Pekerjaan", "Tingkat Pendidikan", "Pengalaman Kerja", 
      "Tunjangan", "Jenis Kelamin", "Cara Kerja", "Lokasi", 
      "Keterampilan Bahasa", "Keterampilan Teknis", "Keterampilan Non Teknis"]

def get_top_accounts(limit=10):
    pipeline = [
        {"$group": {"_id": "$Akun/Judul", "Followers": {"$last": "$Followers"}}},
        {"$sort": {"Followers": -1}},
        {"$limit": limit}
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

def get_total_posts_by_classification():
    pipeline = [
        {"$group": {"_id": "$Klasifikasi Akun", "Count": {"$sum": 1}}}
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

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
    "tanggal": None,
    "jenis_akun": None
}

top_section = st.columns([1,3], gap="small")

with top_section[0]:
    # Date filter options
    with st.container(border=True):
        st.info("Fitur ini masih tahap pengembangan", icon=":material/info:")
        filter_option["tanggal"] = st.selectbox(
            "Filter by Date",
            ["All", "Today", "Tomorrow", "Last 3 Days", "This Month", "Last 3 Months", "Date Range"]
        )

        filter_option["jenis_akun"] = st.multiselect("Klasifikasi Akun", ["Akun Loker", "Akun Netizen", "Akun Pers"], ["Akun Loker", "Akun Netizen", "Akun Pers"])
        
        start_date, end_date = None, None
        
        if filter_option["tanggal"] == "Date Range":
            start_date = st.date_input("Start Date")
            end_date = st.date_input("End Date")

with top_section[1]:
    with st.spinner('Preparing data.'):
        try:
            top_menu = st.columns([5, 2, 3])

            with top_menu[0]:
                search_query = st.text_input("Search Data Posting :", placeholder="Cari data postingan disini ...")
            with top_menu[1]:
                columns = collection.find_one().keys()
                sort_by = st.selectbox("Sort By :", options=columns)
            with top_menu[2]:
                sort_dir = st.radio("Direction", options=["🔼", "🔽"], horizontal=True)
                
            # Fetch and display the data
            df = fetch_data(search_query, sort_by, sort_dir)
            st.dataframe(df, use_container_width=True)

            down_menu = st.columns([4.5,1,1], vertical_alignment="center")
            
            with down_menu[2]:
                st.session_state["page_size"] = st.selectbox("Rows per page", options=[10, 20, 50, 70, 100])
            with down_menu[1]:
                st.session_state["page"] = st.number_input("Page", min_value=1, step=1)

            # Display total documents and calculate total pages
            with down_menu[0]:
                total_docs = collection.count_documents({})
                total_pages = (total_docs + st.session_state["page_size"] - 1)  # Calculate total pages
                st.write(f"Total Data: {total_docs}, Pages: {st.session_state["page"]} / {total_pages}")
                
        except Exception as e:
            st.error("Gagal Menyiapkan Data." + str(e))

top_chart = st.columns(3, gap="small")
# Top 10 Accounts with Most Followers
with top_chart[0]:
    with st.container(border=True):
        st.write("### Top 10 Akun :red[Follower] Terbanyak")
        follower_by_account = get_top_accounts()
        st.write(follower_by_account)

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
                st.write(category_count)
            with col2:
                fig_pie_category = px.pie(category_count, names=item, values='Count')
                st.plotly_chart(fig_pie_category)
        elif idx % 3 == 1:
            with col1:
                fig_bar_category = px.bar(category_count, x='Count', y=item, orientation='h')
                st.plotly_chart(fig_bar_category)
            with col2:
                st.write(category_count)
        else:
            with col1:
                fig_bar_category = px.bar(category_count, y='Count', x=item, orientation='v')
                st.plotly_chart(fig_bar_category)
            with col2:
                st.write(category_count)