def setup_st_sidebar(st, authenticator):
    st.markdown("""
        <style>
            .stAppHeader{
                background: #ffe0dc;
                box-shadow: 0px 5px 10px 1px rgba(0, 0, 0, 0.2);
            }
            .stSidebar{
                border-top-right-radius: 70px;
                border-bottom-right-radius: 70px;
                box-shadow: 5px 0px 5px 1px rgba(0, 0, 0, 0.1);
                width: 270px !important;
                background: white;
            }
            .stMainMenu, .stAppDeployButton{
                display: none !important;
            } 
        </style>
    """, unsafe_allow_html=True)

    st.sidebar.image("https://cliply.co/wp-content/uploads/2019/12/371903520_SOCIAL_ICONS_TRANSPARENT_400px.gif", width=100)

    # Sidebar navigation
    st.sidebar.subheader(f'Hi, *{st.session_state["name"]}* ! 👋')
    
    st.sidebar.text("")
    st.sidebar.text("")
    
    st.sidebar.page_link('streamlit_app.py', label='Mesin Klasifikasi', icon=":material/precision_manufacturing:")
    st.sidebar.page_link('pages/monitoring.py', label='Analisa Data', icon=":material/monitoring:")
    st.sidebar.page_link('pages/kelola_data.py', label='Kelola Data', icon=":material/database:")
    st.sidebar.page_link('pages/bnh_detector.py', label='Bot & Hoax Detector', icon=":material/smart_toy:")
    
    st.sidebar.text("")
    st.sidebar.text("")

    authenticator.logout("👈 Keluar Akun", location='sidebar')