import streamlit as st
import pydeck as pdk
import numpy as np
import requests
import json
from utils.utils import load_location_info

def show():
    # Obtener información de las ubicaciones
    location_info = load_location_info()

    st.title("Modelo de Predicción de Clima")

    # Crear una columna para centrar el contenido
    col1, col2 = st.columns([1, 3])  # Ajustar las proporciones para centrar el contenido

    with col2:
        # Selección de ubicación y fecha
        st.subheader("Seleccione Fecha y Ubicación")
        selected_location = st.selectbox('Seleccione Ubicación', list(location_info.keys()), key="location_select")
        selected_date = st.date_input('Seleccione Fecha', key="date_select")

        # Deslizadores para las variables del modelo
        st.subheader('Ajuste de Parámetros del Modelo')
        
        # Variables obligatorias
        rainfall = st.slider('Rainfall', min_value=0.0, max_value=100.0, value=0.0)
        sunshine = st.slider('Sunshine', min_value=0.0, max_value=24.0, value=0.0)
        wind_gust_speed = st.slider('WindGustSpeed', min_value=0.0, max_value=100.0, value=0.0)
        
        # Variables opcionales
        pressure9am = st.slider('Pressure9am', min_value=950.0, max_value=1050.0, value=None, format="%.2f", help="Opcional")
        temp9am = st.slider('Temp9am', min_value=-10.0, max_value=50.0, value=None, format="%.2f", help="Opcional")
        temp3pm = st.slider('Temp3pm', min_value=-10.0, max_value=50.0, value=None, format="%.2f", help="Opcional")
        cloud3pm = st.slider('Cloud3pm', min_value=0.0, max_value=10.0, value=None, format="%.2f", help="Opcional")
        humidity3pm = st.slider('Humidity3pm', min_value=0.0, max_value=100.0, value=50.0)

        # Enviar los datos con el botón de predicción
        if st.button("Predecir"):
            with st.spinner('Realizando la predicción...'):
                # Obtener las coordenadas de la ubicación seleccionada
                latitude, longitude = location_info[selected_location]

                # Preparar el payload para la solicitud
                payload = {
                    "Rainfall": rainfall,
                    "Sunshine": sunshine,
                    "WindGustSpeed": wind_gust_speed,
                    "Humidity3pm": humidity3pm,
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Day": selected_date.day,
                    "Month": selected_date.month,
                    "Year": selected_date.year
                }

                # Agregar las variables opcionales si están definidas
                if pressure9am is not None:
                    payload["Pressure9am"] = pressure9am
                if temp9am is not None:
                    payload["Temp9am"] = temp9am
                if temp3pm is not None:
                    payload["Temp3pm"] = temp3pm
                if cloud3pm is not None:
                    payload["Cloud3pm"] = cloud3pm

                # Enviar la solicitud POST usando el paquete `requests`
                try:
                    response = requests.get("http://fastapi:8800/", json=payload) #TODO CHANGE HERE!!!
                    response.raise_for_status()  # Levantar un error si la respuesta no es exitosa
                    prediction = response.json()  # Decodificar la respuesta JSON
                except requests.exceptions.RequestException as e:
                    st.error(f"Error al realizar la solicitud: {e}")
                    return

                # Mostrar los resultados
                st.subheader(f"Predicción para el {selected_date} en la ubicación {selected_location}:")
                st.write(f"¿Lloverá mañana? {prediction.get('result', 'Desconocido')}")

                # Visualización de los resultados en el mapa
                view_state = pdk.ViewState(
                    latitude=latitude,
                    longitude=longitude,
                    zoom=8,
                    bearing=0,
                    pitch=0,
                )

                # Capa del mapa
                scatter_layer_selected = pdk.Layer(
                    'ScatterplotLayer',
                    data=[{"latitude": latitude, "longitude": longitude}],
                    get_position='[longitude, latitude]',
                    get_radius=8000,
                    get_fill_color=[255, 0, 0] if prediction.get('result') == "Yes" else [0, 255, 0],
                    opacity=0.8,
                )

                r = pdk.Deck(layers=[scatter_layer_selected], initial_view_state=view_state,
                             map_provider="mapbox", map_style=pdk.map_styles.SATELLITE)

                # Mostrar el mapa
                st.pydeck_chart(r)
