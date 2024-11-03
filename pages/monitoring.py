import streamlit as st
from PIL import Image
import page_service as ps
import pymongo
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
from io import BytesIO

st.set_page_config(
    page_title="Analisa Data Pasker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")

if 'auth_obj' in st.session_state:
    authenticator = st.session_state["auth_obj"]

ps.setup_st_sidebar(st, authenticator)

st.write("# Analisa Data 🕵️‍♂️🚀")
st.subheader("Semua Data Hasil Scraping & Klasifikasi")

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

@st.cache_resource
def get_all_data():
    excel_file_path = "data_sample_besar.xlsx"
    df = pd.read_excel(excel_file_path)
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

client = init_connection()

db = client.medmon
filter_option = {
    "tanggal": None,
    "jenis_akun": None
}
collection = db["hasil_proses_v1"]

with st.expander("Filter Data", icon=":material/filter_alt:"):
        # Date filter options
        filter_option["tanggal"] = st.selectbox(
            "Filter by Date",
            ["All", "Today", "Tomorrow", "Last 3 Days", "This Month", "Last 3 Months", "Date Range"]
        )

        filter_option["jenis_akun"] = st.multiselect("Klasifikasi Akun", ["Akun Loker", "Akun Netizen", "Akun Pers"], ["Akun Loker", "Akun Netizen", "Akun Pers"])
        
        start_date, end_date = None, None
        
        if filter_option["tanggal"] == "Date Range":
            start_date = st.date_input("Start Date")
            end_date = st.date_input("End Date")

with st.spinner('Preparing data.'):
    # Query MongoDB collection with pagination
    raw_data = get_all_data()
    # Convert raw data to DataFrame
    data = pd.DataFrame(raw_data)

    if "Tanggal Publikasi" in data.columns:
        data["Tanggal Publikasi"] = pd.to_datetime(data["Tanggal Publikasi"])

    # Apply date filter
    filtered_data = filter_data(data, filter_option, start_date, end_date)

    # Display filtered data
    st.dataframe(filtered_data)

    st.text("")
    st.text("")
    st.text("")
    st.text("")
    st.text("")
    st.text("")

    kd = ["Tipe Pekerjaan","Tingkat Pekerjaan","Tingkat Pendidikan","Pengalaman Kerja","Tunjangan","Jenis Kelamin","Cara Kerja","Lokasi","Keterampilan Bahasa","Keterampilan Teknis","Keterampilan Non Teknis"]

    col1, col2 = st.columns(2, gap="large")
    with col1:
        st.write("### Top 10 Akun :red[Follower] Terbanyak")
        follower_by_account = filtered_data.groupby('Akun/Judul').last().sort_values(by='Followers', ascending=False).head(10)
        total_postingan = filtered_data.groupby('Akun/Judul').size().reset_index(name='Total Postingan')

        follower_by_account = follower_by_account.merge(total_postingan, on='Akun/Judul')
        
        st.write(follower_by_account[['Akun/Judul', 'Followers', 'Total Postingan', 'Sumber']])
    with col2:
        st.write("### Total Postingan Berdasarkan :red[Klasifikasi Akun]")
        sentimen_count = filtered_data['Klasifikasi Akun'].value_counts().reset_index()
        sentimen_count.columns = ['Klasifikasi Akun', 'Count']
        fig_sentimen = px.bar(sentimen_count, x='Klasifikasi Akun', y='Count', color='Klasifikasi Akun')
        st.plotly_chart(fig_sentimen)

    for idx, item in enumerate(kd):
        category_count = filtered_data[item].explode().value_counts().reset_index()
        category_count.columns = [item, 'Count']
        category_count_filtered = category_count[category_count[item] != 'tdk_ada_informasi']


        st.subheader(f'Distribusi Data :red[{item}] di Setiap Postingan')

        col1, col2 = st.columns(2, gap="small")
        # Pie chart

        if idx % 3 == 0:
            with col1:
                st.write(category_count_filtered)
            with col2:
                fig_pie_category = px.pie(category_count_filtered, names=item, values='Count')
                st.plotly_chart(fig_pie_category)
        elif idx % 3 == 1:
            with col1:
                fig_bar_category = px.bar(category_count_filtered, x='Count', y=item, orientation='h')
                st.plotly_chart(fig_bar_category)
            with col2:
                st.write(category_count_filtered)
        else:
            with col1:
                fig_bar_category = px.bar(category_count_filtered, y='Count', x=item, orientation='v')
                st.plotly_chart(fig_bar_category)
            with col2:
                st.write(category_count_filtered)


