def setup_style_awal(st):
    st.markdown("""
        <style>
            .stToolbarActions, .stMainMenu, .stAppDeployButton{
                display: none !important;
            }
        </style>
    """, unsafe_allow_html=True)

def setup_st_sidebar(st, authenticator):
    st.markdown("""
        <style>
            .stAppHeader{
                background: #ffe0dc;
                box-shadow: 0px 5px 10px 1px rgba(0, 0, 0, 0.2);
            }
            .stSidebar{
                border-top-right-radius: 40px;
                border-bottom-right-radius: 40px;
                box-shadow: 5px 0px 5px 1px rgba(0, 0, 0, 0.1);
                width: 270px !important;
                background: white;
            }
        </style>
    """, unsafe_allow_html=True)

    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/4668/4668808.png", width=80)

    # Sidebar navigation
    st.sidebar.subheader(f'Hi, *{st.session_state["name"]}* ! 👋')
    
    st.sidebar.text("")
    st.sidebar.text("")
    
    if st.session_state["name"] == "Admin Lowongan":
        st.sidebar.page_link('streamlit_app.py', label='Klasifikasi Text', icon=":material/precision_manufacturing:")
        # st.sidebar.page_link('pages/klasifikasi_old.py', label='Klasifikasi Text', icon=":material/precision_manufacturing:")
        st.sidebar.page_link('pages/monitoring.py', label='Analisa Data', icon=":material/monitoring:")
        st.sidebar.page_link('pages/kelola_data.py', label='Kelola Data', icon=":material/database:")
        st.sidebar.page_link('pages/bnh_detector.py', label='Bot & Hoax Detector', icon=":material/smart_toy:")
    else:
        st.sidebar.page_link('streamlit_app.py', label='Klasifikasi Text', icon=":material/precision_manufacturing:")
        st.sidebar.page_link('pages/monitoring.py', label='Analisa Data', icon=":material/monitoring:")
    
    st.sidebar.text("")
    st.sidebar.text("")

    authenticator.logout("👈 Keluar Akun", location='sidebar')