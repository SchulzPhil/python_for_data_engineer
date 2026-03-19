import requests

import streamlit as st
import pandas as pd
import plotly.express as px

API_URL = 'http://localhost:8000'

st.title('CSV Manager')

filename = st.text_input('Filename', 'energy_consumption.csv')


if 'page' not in st.session_state:
    st.session_state.page = 1
if 'limit' not in st.session_state:
    st.session_state.limit = 50

if 'df_page' not in st.session_state:
    st.session_state.df_page = pd.DataFrame()
if 'df_all' not in st.session_state:
    st.session_state.df_all = pd.DataFrame()


def load_data(page_only=False):
    try:
        res_page = requests.get(
            f'{API_URL}/records/{filename}',
            params={'page': st.session_state.page, 'limit': st.session_state.limit}
        )
        res_page.raise_for_status()
        data_page = res_page.json()
        df_page = pd.DataFrame(data_page['data'])
        if not df_page.empty:
            df_page['delete'] = False
        st.session_state.df_page = df_page

        if not page_only:
            res_all = requests.get(f'{API_URL}/records/{filename}', params={'page': 1, 'limit': 1000})
            res_all.raise_for_status()
            data_all = res_all.json()
            df_all = pd.DataFrame(data_all['data'])
            st.session_state.df_all = df_all

    except Exception as e:
        st.error(f'Failed to load data: {e}')
        st.session_state.df_page = pd.DataFrame()
        st.session_state.df_all = pd.DataFrame()


if st.button('Load / Refresh'):
    load_data()

df_page = st.session_state.df_page
df_all = st.session_state.df_all


col1, col2, col3 = st.columns([1, 2, 1])
meta_pages = (len(df_all) // st.session_state.limit) + 1 if not df_all.empty else 1

with col1:
    if st.button('⬅ Prev'):
        if st.session_state.page > 1:
            st.session_state.page -= 1
            load_data(page_only=True)
            st.rerun()
with col3:
    if st.button('Next ➡'):
        if st.session_state.page < meta_pages:
            st.session_state.page += 1
            load_data(page_only=True)
            st.rerun()
with col2:
    st.write(f'Page: {st.session_state.page} / {meta_pages}')


st.subheader('Table (existing rows cannot be edited)')

if not df_page.empty:
    df_display = df_page.copy()
    df_display['delete'] = False

    edited_df = st.data_editor(
        df_display,
        column_config={
            'timestep': st.column_config.TextColumn('timestep', disabled=True),
            'consumption_eur': st.column_config.NumberColumn('consumption_eur', disabled=True),
            'consumption_sib': st.column_config.NumberColumn('consumption_sib', disabled=True),
            'price_eur': st.column_config.NumberColumn('price_eur', disabled=True),
            'price_sib': st.column_config.NumberColumn('price_sib', disabled=True),
            'delete': st.column_config.CheckboxColumn('Delete')
        },
        hide_index=True,
        use_container_width=True
    )

    rows_to_delete = edited_df[edited_df['delete'] == True]
    if not rows_to_delete.empty:
        for _, row in rows_to_delete.iterrows():
            row_id = int(row['id'])
            try:
                requests.delete(f'{API_URL}/records/{filename}/{row_id}')
            except:
                st.warning(f'Failed to delete row id={row_id}')
        st.success(f'Deleted {len(rows_to_delete)} rows')
        load_data(page_only=False)
        st.rerun()

st.subheader('Add new row')

with st.form('add_form'):
    timestep = st.text_input('timestep (YYYY-MM-DD HH:MM:SS)')
    consumption_eur = st.number_input('consumption_eur', value=0)
    consumption_sib = st.number_input('consumption_sib', value=0)
    price_eur = st.number_input('price_eur', value=0.0)
    price_sib = st.number_input('price_sib', value=0.0)

    submitted = st.form_submit_button('Add this row')

    if submitted:
        try:
            payload = {
                'timestep': timestep,
                'consumption_eur': int(consumption_eur),
                'consumption_sib': int(consumption_sib),
                'price_eur': float(price_eur),
                'price_sib': float(price_sib)
            }
            res = requests.post(f'{API_URL}/records/{filename}', json=payload)
            res.raise_for_status()
            st.success('Row added!')
            load_data(page_only=False)
            st.rerun()
        except Exception as e:
            st.error(f'Failed to add row: {e}')

if not df_all.empty:
    df_sorted = df_all.sort_values('timestep')

    st.subheader('Consumption over time (full file)')
    fig1 = px.line(df_sorted, x='timestep', y=['consumption_eur', 'consumption_sib'])
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader('Price over time (full file)')
    fig2 = px.line(df_sorted, x='timestep', y=['price_eur', 'price_sib'])
    st.plotly_chart(fig2, use_container_width=True)
