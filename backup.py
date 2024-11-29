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



def draw_chart_old(idx,item,aw):
    with st.container(border=True, key=f"chart_card_{idx}"):
        with st.spinner('Preparing data.'):
            st.write(f'##### Distribusi Data :red[{item}] di Setiap Postingan')
            
            category_count = get_category_counts(item,aw)
            category_count.columns = [item, 'Count']

            tab_chart = st.tabs(["Chart","Data"])

            if idx % 3 == 0:
                with tab_chart[1]:
                    st.dataframe(category_count, hide_index=True, use_container_width=True)
                with tab_chart[0]:
                    fig_pie_category = px.pie(category_count, names=item, values='Count', color_discrete_sequence=color_sequence)
                    st.plotly_chart(fig_pie_category)
                    
                    data_tbl_ppt = category_count.sort_values(by='Count', ascending=False).head(10)
                    save_chart_to_slide(presentation, fig_pie_category, f'Distribusi Data {item}', data_tbl_ppt)
            elif idx % 3 == 1:
                with tab_chart[0]:
                    fig_bar_category = px.bar(category_count, x='Count', y=item, orientation='h', color_discrete_sequence=color_sequence)
                    st.plotly_chart(fig_bar_category)
                    
                    data_tbl_ppt = category_count.sort_values(by='Count', ascending=False).head(10)
                    save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}', data_tbl_ppt)
                with tab_chart[1]:
                    st.dataframe(category_count, hide_index=True, use_container_width=True)
            else:
                with tab_chart[0]:
                    fig_bar_category = px.bar(category_count, y='Count', x=item, orientation='v', color_discrete_sequence=color_sequence)
                    st.plotly_chart(fig_bar_category)
                    
                    data_tbl_ppt = category_count.sort_values(by='Count', ascending=False).head(10)
                    save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}', data_tbl_ppt)
                with tab_chart[1]:
                    st.dataframe(category_count, hide_index=True, use_container_width=True)

    st.text("")
    st.text("")

def draw_chart_old2(idx, item, aw):
    # Create a container for the chart
    with st.container(border=True, key=f"chart_card_{idx}"):
        with st.spinner('Preparing data...'):
            st.write(f'##### Distribusi Data :red[{item}] di Setiap Postingan')
            
            # Tabs for chart and data
            tab_chart = st.tabs(["Chart", "Data", "Settings"])

            with tab_chart[2]:
                # Fetch and prepare data
                category_count = get_category_counts(item, aw)
                category_count.columns = [item, 'Count']
                
                # Add dropdown for chart type selection
                chart_type = st.selectbox(
                    "Pilih jenis chart:",
                    ["Pie Chart", "Horizontal Bar Chart", "Vertical Bar Chart"],
                    key=f"chart_type_{idx}"
                )
                
                # Input field for limiting data points
                data_limit = st.number_input(
                    "Tampilkan jumlah data maksimal:",
                    min_value=1,
                    max_value=len(category_count),
                    value=len(category_count),
                    step=1,
                    key=f"data_limit_{idx}"
                )
                
                # Filter data based on the selected limit
                data_tbl_ppt = category_count.sort_values(by='Count', ascending=False).head(data_limit)
            
            with tab_chart[0]:  # Chart tab
                if chart_type == "Pie Chart":
                    fig_pie_category = px.pie(
                        data_tbl_ppt,
                        names=item,
                        values='Count',
                        color_discrete_sequence=color_sequence
                    )
                    st.plotly_chart(fig_pie_category)
                    save_chart_to_slide(presentation, fig_pie_category, f'Distribusi Data {item}', data_tbl_ppt)
                
                elif chart_type == "Horizontal Bar Chart":
                    fig_bar_category = px.bar(
                        data_tbl_ppt,
                        x='Count',
                        y=item,
                        orientation='h',
                        color_discrete_sequence=color_sequence
                    )
                    st.plotly_chart(fig_bar_category)
                    save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}', data_tbl_ppt)
                
                elif chart_type == "Vertical Bar Chart":
                    fig_bar_category = px.bar(
                        data_tbl_ppt,
                        y='Count',
                        x=item,
                        orientation='v',
                        color_discrete_sequence=color_sequence
                    )
                    st.plotly_chart(fig_bar_category)
                    save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}', data_tbl_ppt)
            
            with tab_chart[1]:  # Data tab
                st.dataframe(data_tbl_ppt, hide_index=True, use_container_width=True)

    # Add spacing after the chart
    st.text("")
    st.text("")
