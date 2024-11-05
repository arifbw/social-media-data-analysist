st.header('Klasifikasi Akun :')

    left, right = st.columns(2)
    with left:
        st.toggle("Hapus Post Duplikat")
        st.toggle("Hapus karakter spesial")
        st.toggle("Hapus post bukan lowongan")

    with right:
        st.toggle("Aktifkan Bot Detector")
        st.toggle("Aktifkan Hoax Detector")
        st.toggle("Hapus Akun Bot / Hoax")
    
    st.text("")
    left, right = st.columns(2)
    with right:
        if st.button("Test Data Cleansing", icon=":material/play_circle:", use_container_width=True):
            st.write("pages/jupyter.py")


st.markdown("""
        <style>
            div[data-testid='stNumberInputContainer']{
                border: 5px solid #f0f2f6;
            }
                
            div[data-testid='stNumberInputContainer'] input{
                background: white;
            }
                
            div.stTabs div[data-baseweb='tab-border']{
                display: none;
            }  
                
            div.stTabs div[role='tabpanel']{
                border: 2px solid rgba(49, 51, 63, 0.1);
                padding: 10px 15px;
                border-top-right-radius: 20px;
                border-bottom-left-radius: 20px;
                border-bottom-right-radius: 20px;
            }
        </style>
    """, unsafe_allow_html=True)